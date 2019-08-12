package orc

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/PatrickHuang888/goorc/pb/pb"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	MAGIC                = "ORC"
	MAGIC_LENGTH         = uint64(3)
	DIRECTORY_SIZE_GUESS = 16 * 1024
)

type Reader interface {
	GetSchema() *TypeDescription
	NumberOfRows() uint64
	Stripes() ([]StripeReader, error)
	Close() error
}

type ReaderOptions struct {
	CompressionKind pb.CompressionKind
	ChunkSize       uint64
	RowSize         int
}

func DefaultReaderOptions() *ReaderOptions {
	return &ReaderOptions{RowSize: DEFAULT_ROW_SIZE, ChunkSize: DEFAULT_CHUNK_SIZE}
}

type reader struct {
	f       *os.File
	schemas []*TypeDescription
	opts    *ReaderOptions

	tail    *pb.FileTail
	stripes []StripeReader
}

func NewReader(path string, opts *ReaderOptions) (r Reader, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrapf(err, "open file %s error", path)
	}

	tail, err := extractFileTail(f)
	if err != nil {
		return nil, errors.Wrap(err, "read file tail error")
	}

	schemas := unmarshallSchema(tail.Footer.Types)
	opts.ChunkSize = tail.Postscript.GetCompressionBlockSize()
	opts.CompressionKind = tail.Postscript.GetCompression()
	r = &reader{f: f, tail: tail, opts: opts, schemas: schemas}
	return
}

func (r *reader) GetSchema() *TypeDescription {
	return r.schemas[0]
}

func (r *reader) Stripes() (ss []StripeReader, err error) {
	for _, stripeInfo := range r.tail.Footer.Stripes {
		offset := stripeInfo.GetOffset()
		indexLength := stripeInfo.GetIndexLength()
		dataLength := stripeInfo.GetDataLength()
		chunkSize := r.tail.Postscript.GetCompressionBlockSize()

		// row index
		indexOffset := offset
		if _, err = r.f.Seek(int64(indexOffset), 0); err != nil {
			return nil, errors.WithStack(err)
		}
		indexBuf := make([]byte, indexLength)
		if _, err = io.ReadFull(r.f, indexBuf); err != nil {
			return nil, errors.WithStack(err)
		}
		decompressed, err := ReadChunks(indexBuf, r.tail.GetPostscript().GetCompression(), int(chunkSize))
		if err != nil {
			return nil, errors.WithStack(err)
		}
		index := &pb.RowIndex{}
		if err = proto.Unmarshal(decompressed, index); err != nil {
			return nil, errors.Wrapf(err, "unmarshal strip index error")
		}

		// footer
		footerOffset := int64(offset + indexLength + dataLength)
		if _, err := r.f.Seek(footerOffset, 0); err != nil {
			return nil, errors.WithStack(err)
		}
		buf := make([]byte, stripeInfo.GetFooterLength())
		if _, err = io.ReadFull(r.f, buf); err != nil {
			return nil, errors.WithStack(err)
		}
		dbuf, err := ReadChunks(buf, r.tail.GetPostscript().GetCompression(), int(chunkSize))
		if err != nil {
			return nil, errors.WithStack(err)
		}
		footer := &pb.StripeFooter{}
		if err = proto.Unmarshal(dbuf, footer); err != nil {
			return nil, errors.Wrapf(err, "unmarshal currentStripe footer error")
		}

		sr := &stripeReader{f: r.f, opts: r.opts, footer: footer, schemas: r.schemas, info: stripeInfo, idx: -1}
		if err := sr.prepare(); err != nil {
			return ss, errors.WithStack(err)
		}
		ss = append(ss, sr)
	}

	return
}

func (r *reader) Close() error {
	if err := r.f.Close(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

type StripeReader interface {
	NextBatch(batch ColumnVector) (bool, error)
}

type stripeReader struct {
	f       *os.File
	schemas []*TypeDescription
	opts    *ReaderOptions

	info   *pb.StripeInformation
	footer *pb.StripeFooter

	columnReaders []*columnReader
	idx           int
}

type columnReader struct {
	id       int
	schema   *TypeDescription
	encoding *pb.ColumnEncoding

	streams map[pb.Stream_Kind]*streamReader

	f            *os.File
	numberOfRows uint64
	opts         *ReaderOptions

	indexStart  uint64 // index area start
	indexLength uint64

	//present       bool //if has present stream
	//presentStart  uint64
	//presentLength uint64
	//presentRead   bool   // if present already read
	presents      []bool // present data
	presentDecoder Decoder
	dataDecoder      Decoder // data decoder
	secondaryDecoder Decoder
	lengthes         []uint64 // length data
	secondaries      []uint64

	readCursor uint64
}

type streamReader struct {
	start      uint64
	length     uint64
	readLength uint64
	kind       pb.Stream_Kind
	buf        *bytes.Buffer
}

func (cr *columnReader) Print() {
	fmt.Printf("   Column Reader: %d\n", cr.id)
	fmt.Printf("td: %s\n", cr.schema.Kind.String())
	//cr.td.Print()
	fmt.Printf("encoding: %s\n", cr.encoding.String())
	fmt.Printf("streams: \n")
	for _, s := range cr.streams {
		fmt.Printf("%s ", s.String())
	}
	fmt.Println()
	fmt.Println()
}

// stripe {index{},column{[present],data,[length]},footer}
func (sr *stripeReader) prepare() error {
	n := len(sr.schemas)
	crs := make([]*columnReader, n)
	for i := 0; i < len(sr.schemas); i++ {
		cr := &columnReader{}
		cr.id = i
		cr.schema = sr.schemas[i]
		cr.encoding = sr.footer.GetColumns()[i]
		cr.streams = make(map[pb.Stream_Kind]*streamReader)
		cr.f = sr.f
		cr.numberOfRows = sr.info.GetNumberOfRows()
		cr.opts = sr.opts
		crs[i] = cr
	}

	// get all streams of a column
	// and gather streams all kind
	infos:= make(map[pb.Stream_Kind][]*pb.Stream)
	for _, info := range sr.footer.GetStreams() {
		id := info.GetColumn()
		k:= info.GetKind()
		crs[id].streams[k] = &streamReader{length: info.GetLength(), kind: k}
		infos[k]= append(infos[k], info)
	}

	//calculate index stream fields
	var totalLengthIndex uint64
	for i:=0; i<n; i++ {
		index:= crs[i].streams[pb.Stream_ROW_INDEX]
		if index!=nil {
			if i == 0 {
				index.start = sr.info.GetOffset()
			} else {
				indexi := crs[i].streams[pb.Stream_ROW_INDEX]
				if indexi != nil {
					indexPre := crs[i-1].streams[pb.Stream_ROW_INDEX]
					indexi.start = indexPre.start + indexPre.length
				}
			}
		}
		totalLengthIndex+=index.length
	}

	for i := 0; i < n; i++ {
		/*if crs[i].streams[pb.Stream_PRESENT] != nil {
			crs[i].present = true
		}*/
		/*var streamKinds []int
		for k:= range crs[i].streams {
			streamKinds= append(streamKinds, int(k))
		}
		sort.Ints(streamKinds)*/

		if i == 0 {
			index0 := crs[i].streams[pb.Stream_ROW_INDEX]
			if index0!=nil {
				index0.start = sr.info.GetOffset()
			}
			present0:= crs[i].streams[pb.Stream_PRESENT]
			if present0!=nil {
				present0.start = index0.start + index0.length
				crs[0].presentDecoder= &boolRunLength{}
			}
		} else {
			indexi:= crs[i].streams[pb.Stream_ROW_INDEX]
			if indexi!=nil {
				indexPre:= crs[i-1].streams[pb.Stream_ROW_INDEX]
				indexi.start= indexPre.start+ indexPre.length
			}




			previous = 0
			for _, s := range crs[i-1].streams {
				if s.info.GetKind() != pb.Stream_ROW_INDEX {
					previous += s.GetLength()
				}
			}
			if crs[i].present {
				crs[i].presentLength = crs[i-1].streams[pb.Stream_PRESENT].GetLength()
			}
			crs[i].presentStart = crs[i-1].presentStart + previous*/
		}

		switch crs[i].schema.Kind {
		case pb.Type_SHORT:
			fallthrough
		case pb.Type_INT:
			fallthrough
		case pb.Type_LONG:
			crs[i].dataStart = crs[i].presentStart + crs[i].presentLength
			crs[i].dataLength = crs[i].streams[pb.Stream_DATA].GetLength()

			if crs[i].encoding.GetKind() == pb.ColumnEncoding_DIRECT_V2 {
				dec := &intRleV2{}
				dec.signed = true
				crs[i].dataDecoder = dec
			} else {
				return errors.New("column long encoding unknown")
			}

		case pb.Type_FLOAT:
			assertx(crs[i].encoding.GetKind() == pb.ColumnEncoding_DIRECT)
		// todo:
		case pb.Type_DOUBLE:
			assertx(crs[i].encoding.GetKind() == pb.ColumnEncoding_DIRECT)
		// todo:

		case pb.Type_CHAR:
			fallthrough
		case pb.Type_VARCHAR:
			fallthrough
		case pb.Type_STRING:
			if crs[i].encoding.GetKind() == pb.ColumnEncoding_DIRECT_V2 {
				crs[i].dataDecoder = &bytesDirectV2{}
				crs[i].dataStart = crs[i].presentStart + crs[i].presentLength
				crs[i].dataLength = crs[i].streams[pb.Stream_DATA].GetLength()
				crs[i].lengthStart = crs[i].dataStart + crs[i].dataLength
				crs[i].lengthLength = crs[i].streams[pb.Stream_LENGTH].GetLength()
			} else {
				return errors.New("column string encoding unknown")
			}

		case pb.Type_BOOLEAN:
			assertx(crs[i].encoding.GetKind() == pb.ColumnEncoding_DIRECT)
			crs[i].dataDecoder = &boolRunLength{}
			crs[i].dataStart = crs[i].presentStart + crs[i].presentLength
			crs[i].dataLength = crs[i].streams[pb.Stream_DATA].GetLength()

		case pb.Type_BINARY:
			if crs[i].encoding.GetKind() == pb.ColumnEncoding_DIRECT_V2 {
				crs[i].dataDecoder = &bytesDirectV2{}
				crs[i].dataStart = crs[i].presentStart + crs[i].presentLength
				crs[i].dataLength = crs[i].streams[pb.Stream_DATA].GetLength()
				crs[i].lengthStart = crs[i].dataStart + crs[i].dataLength
				crs[i].lengthLength = crs[i].streams[pb.Stream_LENGTH].GetLength()
			} else {
				return errors.New("column binary encoding unknown")
			}

		case pb.Type_DECIMAL:
			if crs[i].encoding.GetKind() == pb.ColumnEncoding_DIRECT_V2 {
				crs[i].dataDecoder = &base128VarInt{}
				crs[i].dataStart = crs[i].presentStart + crs[i].presentLength
				crs[i].dataLength = crs[i].streams[pb.Stream_DATA].GetLength()
				crs[i].secondaryStart = crs[i].dataStart + crs[i].dataLength
				crs[i].secondaryLength = crs[i].streams[pb.Stream_SECONDARY].GetLength()
			} else {
				return errors.New("column decimal encoding unknown")
			}

		case pb.Type_DATE:
		// todo:

		case pb.Type_TIMESTAMP:
		// todo:

		case pb.Type_STRUCT:
			assertx(crs[i].encoding.GetKind() == pb.ColumnEncoding_DIRECT)

		case pb.Type_LIST:
			if crs[i].encoding.GetKind() == pb.ColumnEncoding_DIRECT_V2 {
				crs[i].lengthStart = crs[i].presentStart + crs[i].presentLength
				crs[i].lengthLength = crs[i].streams[pb.Stream_LENGTH].GetLength()
			} else {
				return errors.New("column list encoding unknown")
			}

		case pb.Type_MAP:
			if crs[i].encoding.GetKind() == pb.ColumnEncoding_DIRECT_V2 {
				crs[i].lengthStart = crs[i].presentStart + crs[i].presentLength
				crs[i].lengthLength = crs[i].streams[pb.Stream_LENGTH].GetLength()
			} else {
				return errors.New("column list encoding unknown")
			}

		case pb.Type_UNION:
			assertx(crs[i].encoding.GetKind() == pb.ColumnEncoding_DIRECT)
			// todo: if need direct decoder init here or loaded all in memory
			crs[i].directStart = crs[i].presentStart + crs[i].presentLength
			// fixme:
			//crs[i].directLength= crs[i].streams[pb.Stream_DIRECT]
			// todo: if need direct buf

		default:
			return errors.New("column schema type unknown")
		}

		crs[i].dataBuf = bytes.NewBuffer(make([]byte, sr.opts.ChunkSize))
	}

	sr.columnReaders = crs

	for _, v := range crs {
		v.Print()
	}
	return nil
}

// a stripe is typically  ~200MB
func (sr *stripeReader) NextBatch(batch ColumnVector) (bool, error) {
	batch.reset()

	columnReader := sr.columnReaders[batch.ColumnId()]
	log.Debugf("reading cursor at %d\n", columnReader.readCursor)

	/*indexStream := cr.streams[pb.Stream_ROW_INDEX]
	fmt.Println("==========")
	fmt.Println(indexStream.String())

	if _, err := cr.f.Seek(int64(cr.indexStart), 0); err != nil {
		fmt.Printf("%+v", err)
		return false
	}

	head := make([]byte, 3)
	if _, err := io.ReadFull(cr.f, head); err != nil {
		fmt.Printf("%+v", errors.WithStack(err))
		return false
	}

	original := (head[0] & 0x01) == 1
	chunkLength := int(head[2])<<15 | int(head[1])<<7 | int(head[0])>>1

	indexBuf := make([]byte, chunkLength)
	if _, err := io.ReadFull(cr.f, indexBuf); err != nil {
		fmt.Printf("%+v", err)
	}

	if original {
		fmt.Println("orgin+++++")
		ri := &pb.RowIndex{}
		if err := proto.Unmarshal(indexBuf, ri); err != nil {
			fmt.Printf("%+v", errors.WithStack(err))
			return false
		}
		fmt.Println(ri.String())
	} else {

		decompressed := make([]byte, cr.chunkBufSize)
		r := flate.NewReader(bytes.NewReader(indexBuf))
		n, err := r.Read(decompressed)
		r.Close()
		if err != nil && err != io.EOF {
			fmt.Printf("%+v", errors.WithStack(err))
			return false
		}
		ri := &pb.RowIndex{}
		if err := proto.Unmarshal(decompressed[:n], ri); err != nil {
			fmt.Printf("%+v", errors.WithStack(err))
			return false
		}
		fmt.Println(ri.String())
	}*/

	if err := columnReader.readPresent(); err != nil {
		return false, errors.WithStack(err)
	}

	encoding := columnReader.encoding.GetKind()

	switch columnReader.schema.Kind {
	case pb.Type_SHORT:
		fallthrough
	case pb.Type_INT:
		fallthrough
	case pb.Type_LONG:
		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			return columnReader.readLongsV2(batch.(*LongColumn))
		}

	case pb.Type_FLOAT:
	// todo:

	case pb.Type_DOUBLE:
	// todo:

	case pb.Type_STRING:
		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			return columnReader.readStringsV2(batch.(*StringColumn))
		}

	case pb.Type_BOOLEAN:
		return columnReader.readBools(batch.(*BoolColumn))

	case pb.Type_BYTE:
		return columnReader.readBytes(batch.(*TinyIntColumn))

	case pb.Type_BINARY:
		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			return columnReader.readBinaryV2(batch.(*BinaryColumn))
		}

	case pb.Type_DECIMAL:
		column, ok := batch.(*Decimal64Column)
		if !ok {
			return false, errors.New("decimal column should be decimal64")
		}
		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			return columnReader.readDecimal64sV2(column)
		}

	case pb.Type_DATE:
		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			return columnReader.readDatesV2(batch.(*DateColumn))
		}

	case pb.Type_TIMESTAMP:
		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			return columnReader.readTimestampsV2(batch.(*TimestampColumn))
		}

	case pb.Type_STRUCT:
		column := batch.(*StructColumn)
		// fixme: next value calculation
		var next bool
		for _, f := range column.Fields {
			n, err := sr.NextBatch(f)
			if err != nil {
				return false, errors.WithStack(err)
			}
			next = n
		}
		return next, nil

	case pb.Type_UNION:
		// todo:

	case pb.Type_LIST:
		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			// Why length ???
			if err := columnReader.readLength(); err != nil {
				return false, errors.WithStack(err)
			}
			column := batch.(*ListColumn)
			next, err := sr.NextBatch(column.Child)
			if err != nil {
				return false, errors.WithStack(err)
			}
			return next, nil
		}

	default:
		return false, errors.Errorf("type %s not impl", columnReader.schema.Kind.String())
	}

	return false, nil
}

func (cr *columnReader) readDatesV2(column *DateColumn) (next bool, err error) {
	dd := cr.dataDecoder.(*intRleV2)
	for cr.dataRead < cr.dataLength {
		for i := dd.consumedIndex; i < dd.len(); {
			l := len(column.Vector)
			if l < cap(column.Vector) {
				if column.nullable {
					if !cr.present || cr.presents[l-1] {
						column.Nulls = append(column.Nulls, false)
						column.Vector = append(column.Vector, fromDays(dd.literals[i]))
						i++
					} else {
						column.Nulls = append(column.Nulls, true)
						column.Vector = append(column.Vector, fromDays(dd.literals[i]))
					}
				} else { // no nulls
					column.Vector = append(column.Vector, fromDays(dd.literals[i]))
					i++
				}
				cr.readCursor++
			} else {
				// still not finished
				dd.consumedIndex = i
				return true, nil
			}
		}

		if cr.dataRead >= cr.dataLength {
			break
		}

		dd.reset()
		if err := cr.readData(); err != nil {
			return false, errors.WithStack(err)
		}
	}

	return false, nil
}

func (cr *columnReader) readTimestampsV2(column *TimestampColumn) (next bool, err error) {
	data := cr.streams[pb.Stream_DATA]
	secondary := cr.streams[pb.Stream_SECONDARY]
	dd := cr.dataDecoder.(*intRleV2)
	sd := cr.secondaryDecoder.(*intRleV2)
	for {
		i := dd.consumedIndex
		j := sd.consumedIndex
		for ; i < dd.len() && j < sd.len(); {
			l := len(column.Vector)
			if l < cap(column.Vector) {
				if column.nullable {
					if !cr.present || cr.presents[l-1] {
						column.Nulls = append(column.Nulls, false)
						column.Vector = append(column.Vector, getTimestamp(dd.literals[i], sd.uliterals[i]))
						i++
						j++
					} else {
						column.Nulls = append(column.Nulls, true)
						column.Vector = append(column.Vector, Timestamp{})
					}
				} else { // no nulls
					column.Vector = append(column.Vector, getTimestamp(dd.literals[i], sd.uliterals[i]))
					i++
					j++
				}
				cr.readCursor++
			} else {
				// still not finished
				dd.consumedIndex = i
				return true, nil
			}
		}

		if data.readAll() && secondary.readAll() {
			break
		}

		if dd.consumedIndex == dd.len() {
			dd.reset()
			data := cr.streams[pb.Stream_DATA]
			if err := data.read(cr.opts, cr.f, dd); err != nil {
				return false, errors.WithStack(err)
			}
		}

		if sd.consumedIndex == sd.len() {
			sd.reset()
			if err := secondary.read(cr.opts, cr.f, sd); err != nil {
				return false, errors.WithStack(err)
			}
		}
	}

	return column.Rows() != 0, nil
}

func (cr *columnReader) readBools(column *BoolColumn) (next bool, err error) {
	if cr.present {
		assertx(column.nullable)
		assertx(cr.presents != nil)
	}

	dd := cr.dataDecoder.(*boolRunLength)

	// because bools extend to byte, may not know the real rows from read,
	// so using number of rows
	for (cr.readCursor < cr.numberOfRows) && (cr.dataRead < cr.dataLength) {
		for i := dd.consumedIndex; i < dd.len(); {
			l := len(column.Vector)
			if l < cap(column.Vector) {
				if column.nullable {
					if !cr.present || cr.presents[l-1] {
						column.Nulls = append(column.Nulls, false)
						column.Vector = append(column.Vector, dd.bools[i])
						i++
					} else {
						column.Nulls = append(column.Nulls, true)
						column.Vector = append(column.Vector, false)
					}
				} else { // no nulls
					column.Vector = append(column.Vector, dd.bools[i])
					i++
				}
				cr.readCursor++
			} else {
				// still not finished
				dd.consumedIndex = i
				return true, nil
			}
		}

		if cr.dataRead >= cr.dataLength {
			break
		}

		dd.reset()
		if err := cr.readData(); err != nil {
			return false, errors.WithStack(err)
		}
	}

	return false, nil
}

func (cr *columnReader) readBinaryV2(column *BinaryColumn) (next bool, err error) {
	dd := cr.dataDecoder.(*bytesDirectV2)
	for cr.dataRead < cr.dataLength {
		for i := dd.consumedIndex; i < dd.len(); {
			l := len(column.Vector)
			if l < cap(column.Vector) {

				if column.nullable {
					if !cr.present || cr.presents[l-1] {
						column.Nulls = append(column.Nulls, false)
						column.Vector = append(column.Vector, dd.content[i])
						i++
					} else {
						column.Nulls = append(column.Nulls, true)
						column.Vector = append(column.Vector, nil)
					}
				} else { // no nulls
					column.Vector = append(column.Vector, dd.content[i])
					i++
				}
				cr.readCursor++
			} else {
				// still not finished
				dd.consumedIndex = i
				return true, nil
			}
		}

		if cr.dataRead >= cr.dataLength {
			break
		}

		if err := cr.readLength(); err != nil {
			return false, errors.WithStack(err)
		}

		dd.reset()
		dd.length = cr.lengthes
		if err := cr.readData(); err != nil {
			return false, errors.WithStack(err)
		}
	}

	return false, nil
}

func (cr *columnReader) readStringsV2(column *StringColumn) (next bool, err error) {
	dd := cr.dataDecoder.(*bytesDirectV2)
	for cr.dataRead <= cr.dataLength {
		for i := dd.consumedIndex; i < dd.len(); {
			l := len(column.Vector)
			if l < cap(column.Vector) {
				if column.nullable {
					if cr.streams[pb.Stream_PRESENT] == nil || cr.presents[l-1] {
						column.Nulls = append(column.Nulls, false)
						column.Vector = append(column.Vector, string(dd.content[i]))
						i++
					} else {
						column.Nulls = append(column.Nulls, true)
						column.Vector = append(column.Vector, "")
					}
				} else { // no nulls
					column.Vector = append(column.Vector, string(dd.content[i]))
					i++
				}
				cr.readCursor++
			} else {
				// still not finished
				dd.consumedIndex = i
				return true, nil
			}
		}

		if cr.dataRead >= cr.dataLength {
			break
		}

		if err := cr.readLength(); err != nil {
			return false, errors.WithStack(err)
		}

		dd.reset()
		dd.length = cr.lengthes
		if err := cr.readData(); err != nil {
			return false, errors.WithStack(err)
		}
	}

	return false, nil
}

func (cr *columnReader) readBytes(column *TinyIntColumn) (next bool, err error) {
	presentStream := cr.streams[pb.Stream_PRESENT]
	dd := cr.dataDecoder.(*byteRunLength)

	for cr.dataRead < cr.dataLength {
		for i := dd.consumedIndex; i < dd.len(); {
			l := len(column.Vector)
			if l < cap(column.Vector) {

				if column.nullable {
					if presentStream == nil || cr.presents[l-1] {
						column.Nulls = append(column.Nulls, false)
						column.Vector = append(column.Vector, dd.literals[i])
						i++
					} else {
						column.Nulls = append(column.Nulls, true)
						column.Vector = append(column.Vector, 0)
					}
				} else { // no nulls
					column.Vector = append(column.Vector, dd.literals[i])
					i++
				}

			} else {
				// still not finished
				dd.consumedIndex = i
				return true, nil
			}
		}

		if cr.dataRead >= cr.dataLength {
			break
		}

		dd.reset()
		if err := cr.readData(); err != nil {
			return false, errors.WithStack(err)
		}
	}

	return false, nil
}

func (cr *columnReader) readLongsV2(column *LongColumn) (next bool, err error) {
	data := cr.streams[pb.Stream_DATA]
	dd := cr.dataDecoder.(*intRleV2)
	for {
		for i := dd.consumedIndex; i < dd.len(); {
			l := len(column.Vector)
			if l < cap(column.Vector) {
				if column.nullable {
					if !cr.present || cr.presents[l-1] {
						column.Nulls = append(column.Nulls, false)
						column.Vector = append(column.Vector, dd.literals[i])
						i++
					} else {
						column.Nulls = append(column.Nulls, true)
						column.Vector = append(column.Vector, 0)
					}
				} else { // no nulls
					column.Vector = append(column.Vector, dd.literals[i])
					i++
				}
				cr.readCursor++
			} else {
				// still not finished
				dd.consumedIndex = i
				return true, nil
			}
		}

		if data.readAll() {
			break
		}

		dd.reset()
		if err := data.read(cr.opts, cr.f, dd); err != nil {
			return false, errors.WithStack(err)
		}
	}

	return false, nil
}

func (cr *columnReader) readDecimal64sV2(column *Decimal64Column) (next bool, err error) {
	data := cr.streams[pb.Stream_DATA]
	secondary := cr.streams[pb.Stream_SECONDARY]
	dd := cr.dataDecoder.(*base128VarInt)
	sd := cr.secondaryDecoder.(*intRleV2)
	for {
		for i := dd.consumedIndex; i < dd.len(); {
			l := len(column.Vector)
			if l < cap(column.Vector) {
				if column.nullable {
					if !cr.present || cr.presents[l-1] {
						column.Nulls = append(column.Nulls, false)
						column.Vector = append(column.Vector, dd.values[i])
						i++
					} else {
						column.Nulls = append(column.Nulls, true)
						column.Vector = append(column.Vector, 0)
					}
				} else { // no nulls
					column.Vector = append(column.Vector, dd.values[i])
					i++
				}
				cr.readCursor++
			} else {
				// still not finished
				dd.consumedIndex = i
				return true, nil
			}
		}

		if data.readAll() {
			break
		}

		if err := secondary.read(cr.opts, cr.f, sd); err != nil {
			return false, errors.WithStack(err)
		}
		// toAssure:
		column.Scale = uint16(cr.secondaries[0])

		dd.reset()
		if err := data.read(cr.opts, cr.f, dd); err != nil {
			return false, errors.WithStack(err)
		}
	}

	return false, nil
}

// toAssure: read present stream all in memory
func (cr *columnReader) readPresent() error {
	if !cr.present || cr.presentRead {
		return nil
	}

	if _, err := cr.f.Seek(int64(cr.presentStart), 0); err != nil {
		return errors.WithStack(err)
	}

	var n uint64
	presentBuf := bytes.NewBuffer(make([]byte, cr.opts.ChunkSize))
	presentBuf.Reset()
	for n < cr.presentLength {
		l, err := readAChunk(cr.opts, cr.f, presentBuf)
		if err != nil {
			return errors.WithStack(err)
		}
		n += uint64(l)
	}

	pd := &boolRunLength{}
	if err := pd.readValues(presentBuf); err != nil {
		return errors.WithStack(err)
	}
	cr.presents = pd.bools[0:cr.numberOfRows]
	cr.presentRead = true

	return nil
}

func (stream *streamReader) read(opts *ReaderOptions, f *os.File, dec Decoder) error {
	if stream.readAll() {
		return nil
	}

	if _, err := f.Seek(int64(stream.start+stream.readLength), 0); err != nil {
		return errors.WithStack(err)
	}

	stream.buf.Reset()
	l, err := readAChunk(opts, f, stream.buf)
	if err != nil {
		return errors.WithStack(err)
	}
	stream.readLength += l

	if err := dec.readValues(stream.buf); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (stream *streamReader) readAll() bool {
	return stream.readLength >= stream.length
}

func decompress(kind pb.CompressionKind, original bool, dst *bytes.Buffer, src *bytes.Buffer) (n int64, err error) {
	if original {
		if n, err = io.Copy(dst, src); err != nil {
			return 0, errors.WithStack(err)
		}
	} else {
		switch kind {
		case pb.CompressionKind_ZLIB:
			r := flate.NewReader(src)
			n, err = dst.ReadFrom(r)
			r.Close()
			if err != nil && err != io.EOF {
				return 0, errors.Wrapf(err, "decompress chunk data error")
			}
			return n, nil
		default:
			return 0, errors.New("compression kind other than zlib not impl")
		}
	}
	return
}

func (r *reader) NumberOfRows() uint64 {
	return r.tail.Footer.GetNumberOfRows()
}

func unmarshallSchema(types []*pb.Type) (schemas []*TypeDescription) {
	schemas = make([]*TypeDescription, len(types))
	for i, t := range types {
		node := &TypeDescription{Kind: t.GetKind(), Id: uint32(i)}
		schemas[i] = node
	}
	for i, t := range types {
		schemas[i].Children = make([]*TypeDescription, len(t.Subtypes))
		schemas[i].ChildrenNames = make([]string, len(t.Subtypes))
		for j, v := range t.Subtypes {
			schemas[i].ChildrenNames[j] = t.FieldNames[j]
			schemas[i].Children[j] = schemas[v]
		}
	}
	return
}

/*func unmarshallSchema(types []*pb.Type) (schema *TypeDescription) {
	tds := make([]*TypeDescription, len(types))
	for i, t := range types {
		node := &TypeDescription{Kind: t.GetKind(), Id: uint32(i)}
		tds[i] = node
	}
	if len(tds) > 0 {
		schema = tds[0]
	}
	for i, t := range types {
		tds[i].Children = make([]*TypeDescription, len(t.Subtypes))
		tds[i].ChildrenNames = make([]string, len(t.Subtypes))
		for j, v := range t.Subtypes {
			tds[i].ChildrenNames[j] = t.FieldNames[j]
			tds[i].Children[j] = tds[v]
		}
	}
	return
}
*/

func marshallSchema(schema *TypeDescription) (types []*pb.Type) {
	types = preOrderWalkSchema(schema)
	return
}

func preOrderWalkSchema(node *TypeDescription) (types []*pb.Type) {
	t := &pb.Type{}
	t.Kind = &node.Kind
	for i, name := range node.ChildrenNames {
		t.FieldNames = append(t.FieldNames, name)
		t.Subtypes = append(t.Subtypes, node.Children[i].Id)
	}
	types = append(types, t)
	for _, n := range node.Children {
		ts := preOrderWalkSchema(n)
		types = append(types, ts...)
	}
	return
}

func extractFileTail(f *os.File) (tail *pb.FileTail, err error) {

	fi, err := f.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "get file status error")
	}
	size := fi.Size()
	if size == 0 {
		// Hive often creates empty files (including ORC) and has an
		// optimization to create a 0 byte file as an empty ORC file.
		// todo: empty tail, log
		fmt.Printf("file size 0")
		return
	}
	if size <= int64(len(MAGIC)) {
		return nil, errors.New("not a valid orc file")
	}

	// read last bytes into buffer to get PostScript
	// refactor: buffer 16k length or capacity
	readSize := Min(size, DIRECTORY_SIZE_GUESS)
	buf := make([]byte, readSize)
	if _, err := f.Seek(size-readSize, 0); err != nil {
		return nil, errors.WithStack(err)
	}
	if _, err := io.ReadFull(f, buf); err != nil {
		return nil, errors.WithStack(err)
	}

	// read postScript
	psLen := int64(buf[readSize-1])
	psOffset := readSize - 1 - psLen
	ps, err := extractPostScript(buf[psOffset : psOffset+psLen])
	if err != nil {
		return nil, errors.Wrapf(err, "extract postscript error %s", f.Name())
	}
	footerSize := int64(ps.GetFooterLength()) // compressed footer length
	metaSize := int64(ps.GetMetadataLength())

	// check if extra bytes need to be read
	extra := Max(0, psLen+1+footerSize+metaSize-readSize)
	if extra > 0 {
		// more bytes need to be read, read extra bytes
		ebuf := make([]byte, extra)
		if _, err := f.Seek(size-readSize-extra, 0); err != nil {
			return nil, errors.WithStack(err)
		}
		if _, err = io.ReadFull(f, ebuf); err != nil {
			return nil, errors.WithStack(err)
		}
		// refactor: array allocated
		buf = append(buf, ebuf...)
	}

	// read file footer
	footerStart := psOffset - footerSize
	cmpBufSize := ps.GetCompressionBlockSize()
	footerBuf := bytes.NewBuffer(make([]byte, cmpBufSize))
	footerBuf.Reset()
	cmpFooterBuf := bytes.NewBuffer(buf[footerStart : footerStart+footerSize])

	if err := decompressedTo(footerBuf, cmpFooterBuf, ps.GetCompression()); err != nil {
		return nil, errors.WithStack(err)
	}

	footer := &pb.Footer{}
	if err = proto.Unmarshal(footerBuf.Bytes(), footer); err != nil {
		return nil, errors.Wrapf(err, "unmarshal footer error")
	}
	fmt.Printf("Footer: %s\n", footer.String())

	fl := uint64(size)
	psl := uint64(psLen)
	ft := &pb.FileTail{Postscript: ps, Footer: footer, FileLength: &fl, PostscriptLength: &psl}
	return ft, nil
}

func extractPostScript(buf []byte) (ps *pb.PostScript, err error) {
	ps = &pb.PostScript{}
	if err = proto.Unmarshal(buf, ps); err != nil {
		return nil, errors.Wrapf(err, "unmarshall postscript err")
	}
	if err = checkOrcVersion(ps); err != nil {
		return nil, errors.Wrapf(err, "check orc version error")
	}

	// Check compression codec.
	/*switch ps.GetCompression() {
	  default:
	  	return nil, errors.New("unknown compression")
	  }*/
	fmt.Printf("Postscript: %s\n", ps.String())
	return ps, err
}

func checkOrcVersion(ps *pb.PostScript) error {
	// todo：
	return nil
}

func ensureOrcFooter(f *os.File, psLen int, buf []byte) error {
	magicLength := len(MAGIC)
	fullLength := magicLength + 1;
	if psLen < fullLength || len(buf) < fullLength {
		return errors.Errorf("malformed ORC file %s, invalid postscript length %d", f.Name(), psLen)
	}
	// now look for the magic string at the end of the postscript.
	//if (!Text.decode(array, offset, magicLength).equals(OrcFile.MAGIC)) {
	offset := len(buf) - fullLength
	// fixme: encoding
	if string(buf[offset:]) != MAGIC {
		// If it isn't there, this may be the 0.11.0 version of ORC.
		// Read the first 3 bytes of the file to check for the header
		// todo:

		return errors.Errorf("malformed ORC file %s, invalid postscript", f.Name())
	}
	return nil
}

func MinUint64(x, y uint64) uint64 {
	if x < y {
		return x
	}
	return y
}

func Min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

func Max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}

// decompress buffer src into dst
func decompressedTo(dst *bytes.Buffer, src *bytes.Buffer, kind pb.CompressionKind) (err error) {
	switch kind {
	case pb.CompressionKind_ZLIB:
		for src.Len() > 0 {
			header := make([]byte, 3)
			if _, err = src.Read(header); err != nil {
				return errors.WithStack(err)
			}
			original := header[0]&0x01 == 1
			chunkLength := int64(header[2])<<15 | int64(header[1])<<7 | int64(header[0])>>1
			if original {
				if _, err = io.CopyN(dst, src, chunkLength); err != nil {
					return errors.WithStack(err)
				}
			} else {
				buf := bytes.NewBuffer(make([]byte, chunkLength))
				buf.Reset()
				if _, err = io.CopyN(buf, src, chunkLength); err != nil {
					return errors.WithStack(err)
				}
				r := flate.NewReader(buf)
				if _, err = io.Copy(dst, r); err != nil {
					return errors.WithStack(err)
				}
				if err = r.Close(); err != nil {
					return errors.WithStack(err)
				}
			}
		}
	default:
		return errors.New("decompression other than zlib not impl")
	}
	return
}

func assertx(condition bool) {
	if !condition {
		panic("assert error")
	}
}

// read one chunk and decompressed to out, n is read count of f
func readAChunk(opts *ReaderOptions, f *os.File, out *bytes.Buffer) (n uint64, err error) {
	head := make([]byte, 3)
	if _, err = io.ReadFull(f, head); err != nil {
		return 0, errors.WithStack(err)
	}
	n += 3
	original := (head[0] & 0x01) == 1
	chunkLength := uint64(head[2])<<15 | uint64(head[1])<<7 | uint64(head[0])>>1
	if uint64(chunkLength) > opts.ChunkSize {
		return 0, errors.Errorf("chunk length %d larger than chunk size %d", chunkLength, opts.ChunkSize)
	}

	buf := bytes.NewBuffer(make([]byte, opts.ChunkSize))
	buf.Reset()
	if w, err := io.CopyN(buf, f, int64(chunkLength)); err != nil {
		return uint64(3 + w), errors.WithStack(err)
	}
	n += chunkLength

	if _, err := decompress(opts.CompressionKind, original, out, buf); err != nil {
		return n, errors.WithStack(err)
	}

	return
}

func ReadChunks(chunksBuf []byte, compressKind pb.CompressionKind, chunkBufferSize int) (decompressed []byte, err error) {
	for offset := 0; offset < len(chunksBuf); {
		// header
		original := (chunksBuf[offset] & 0x01) == 1
		chunkLength := int(chunksBuf[offset+2])<<15 | int(chunksBuf[offset+1])<<7 |
			int(chunksBuf[offset])>>1
		buf := make([]byte, chunkBufferSize)
		//fixme:
		if chunkLength > chunkBufferSize {
			return nil, errors.New("chunk length larger than compression block size")
		}
		offset += 3

		if original {
			//fixme:
			decompressed = append(decompressed, chunksBuf[offset:offset+chunkLength]...)
		} else {
			switch compressKind {
			case pb.CompressionKind_ZLIB:
				r := flate.NewReader(bytes.NewReader(chunksBuf[offset : offset+chunkLength]))
				n, err := r.Read(buf)
				r.Close()
				if err != nil && err != io.EOF {
					return nil, errors.Wrapf(err, "decompress chunk data error when read footer")
				}
				if n == 0 {
					return nil, errors.New("decompress 0 footer")
				}
				//fixme:
				decompressed = append(decompressed, buf[:n]...)
			default:
				//todo:
				return nil, errors.New("compress other than zlib not implemented")
			}
		}
		offset += chunkLength
	}
	return
}
