package orc

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/PatrickHuang888/goorc/pb/pb"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
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

func CreateReaderOptions() *ReaderOptions {
	return &ReaderOptions{RowSize: DEFAULT_ROW_SIZE}
}

type reader struct {
	f *os.File

	tail *pb.FileTail

	schemas []*TypeDescription
	opts    *ReaderOptions

	stripes []StripeReader
}

func CreateReader(path string) (r Reader, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrapf(err, "open file %s error", path)
	}

	tail, err := extractFileTail(f)
	if err != nil {
		return nil, errors.Wrap(err, "read file tail error")
	}

	schemas := unmarshallSchema(tail.Footer.Types)
	opts := CreateReaderOptions()
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
		//fmt.Printf("Stripe index: %s\n", index.String())
		/*for _, entry := range index.Entry {
			fmt.Printf("strip index entry: %s\n", entry.String())
		}*/

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

		sr := &stripeReader{f: r.f, offset: offset, footer: footer, schemas: r.schemas, tail: r.tail, idx: -1}
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

type StripeReader interface {
	NextBatch(batch ColumnVector) (bool, error)
}

type stripeReader struct {
	f *os.File

	prepared bool

	offset      uint64
	indexLength uint64
	dataLength  uint64
	//numberOfRows uint64

	footer *pb.StripeFooter

	schemas []*TypeDescription
	opts    *ReaderOptions

	tail *pb.FileTail

	idx int

	columnReaders []*columnReader

	err error
}

type columnReader struct {
	id       int
	schema   *TypeDescription
	encoding *pb.ColumnEncoding
	streams  map[pb.Stream_Kind]*pb.Stream
	f        *os.File
	si       *pb.StripeInformation

	opts *ReaderOptions

	indexStart uint64 // index area start
	dataStart  uint64 // data area start

	presentStart  uint64
	presentLength uint64
	presentRead   bool   // if present already read
	presents      []bool // present data

	lengthRead   bool
	lengthStart  uint64
	lengthLength uint64
	lengthes     []uint64 // length data

	dataRead uint64 // data stream already read
	dataBuf  *bytes.Buffer

	dataDecoder Decoder // data decoder

	batchCount int

	//decmpBuf *bytes.Buffer // decompressed buffer for a chunk
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

func (sr *stripeReader) prepare() {

	n := len(sr.schemas)
	columnReaders := make([]*columnReader, n)
	for i := 0; i < n; i++ {
		dataBuf := bytes.NewBuffer(make([]byte, sr.opts.ChunkSize))
		dataBuf.Reset()
		columnReaders[i] = &columnReader{id: i, schema: sr.schemas[i], encoding: sr.footer.GetColumns()[i],
			opts:    sr.opts,
			streams: make(map[pb.Stream_Kind]*pb.Stream), f: sr.f, batchCount: -1, dataBuf: dataBuf}

		//cr.dataDecoder = &intRleV2{signed: true}
	}

	indexOffsets := make(map[uint32]uint64)
	dataOffsets := make(map[uint32]uint64)
	for _, stream := range sr.footer.GetStreams() {
		id := stream.GetColumn()
		columnReaders[id].streams[stream.GetKind()] = stream
		// fixme: offset calculation
		if stream.GetKind() == pb.Stream_ROW_INDEX {
			indexOffsets[id] += stream.GetLength()
		} else {
			dataOffsets[id] += stream.GetLength()
		}
	}
	//si := sr.tail.Footer.Stripes[sr.currentStripe]
	columnReaders[0].indexStart = sr.offset
	columnReaders[0].dataStart = sr.offset + sr.indexLength
	for i := 1; i < n; i++ {
		columnReaders[i].indexStart = columnReaders[i-1].indexStart + indexOffsets[uint32(i-1)]
		columnReaders[i].dataStart = columnReaders[i-1].dataStart + dataOffsets[uint32(i-1)]
	}

	sr.columnReaders = columnReaders

	for _, v := range columnReaders {
		v.Print()
	}

}

// a stripe is typically  ~200MB
func (sr *stripeReader) NextBatch(batch ColumnVector) (bool, error) {
	batch.reset()

	columnReader := sr.columnReaders[batch.ColumnId()]
	// todo:page
	columnReader.batchCount++
	fmt.Printf("read batch %d\n", columnReader.batchCount)

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

	encoding := columnReader.encoding.GetKind()
	switch columnReader.schema.Kind {
	case pb.Type_BOOLEAN:
		assert(encoding == pb.ColumnEncoding_DIRECT)
		return columnReader.readBools(batch.(*BoolColumn))

	case pb.Type_BYTE:
		assert(encoding == pb.ColumnEncoding_DIRECT)
		return columnReader.readBytes(batch.(*TinyIntColumn))

	case pb.Type_BINARY:
		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			return columnReader.readBinaryV2(batch.(*BinaryColumn))
		}
		return false, errors.New("column binary encoding unknown")

	case pb.Type_SHORT:
		fallthrough
	case pb.Type_INT:
		fallthrough
	case pb.Type_LONG:
		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			return columnReader.readLongsV2(batch.(*LongColumn))
		}
		return false, errors.New("column bigint encoding unknown")

	case pb.Type_DATE:
		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			return columnReader.readDatesV2(batch.(*DateColumn))
		}
		return false, errors.Errorf("column date encoding %s unknown", encoding)

	case pb.Type_TIMESTAMP:
		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			return columnReader.readTimestampsV2(batch.(*TimestampColumn))
		}
		return false, errors.Errorf("column timestamp encoding %s unknown", encoding)

	case pb.Type_STRING:
		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			return columnReader.readStringsV2(batch.(*StringColumn))
		}
		return false, errors.Errorf("column string encoding %s unknown", encoding)

	case pb.Type_STRUCT:
		assert(encoding == pb.ColumnEncoding_DIRECT)
		if err := columnReader.readPresent(); err != nil {
			return false, errors.WithStack(err)
		}
		for _, f := range batch.(*StructColumn).Fields {
			sr.NextBatch(f)
		}

	default:
		return false, errors.Errorf("type %s not impl", columnReader.schema.Kind.String())
	}

	return false, nil
}

func (cr *columnReader) readStructs(column *StructColumn) (next bool, err error) {
	if err := cr.readPresent(); err != nil {
		return false, errors.WithStack(err)
	}

	for _, c := range column.Fields {
		cr.read
	}
	return false, nil
}

func (cr *columnReader) readDatesV2(column *DateColumn) (next bool, err error) {
	presentStream := cr.streams[pb.Stream_PRESENT]
	dd := cr.dataDecoder.(*intRleV2)

	for dd.consumedIndex != 0 {
		for i := dd.consumedIndex; i < dd.len(); {
			l := len(column.Vector)
			if l < cap(column.Vector) {

				if column.nullable {
					if presentStream == nil || cr.presents[l-1] {
						column.Nulls = append(column.Nulls, false)
						t := time.Time(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC))
						t.Add(time.Duration(dd.literals[i]))
						column.Vector = append(column.Vector, Date())
						i++
					} else {
						column.Nulls = append(column.Nulls, true)
						column.Vector = append(column.Vector, false)
					}
				} else { // no nulls
					column.Vector = append(column.Vector, dd.bools[i])
					i++
				}

			} else {
				// still not finished
				dd.consumedIndex = i
				return true, nil
			}
		}
		// decoder consuming finished
		dd.reset()

		if err := cr.readPresent(); err != nil {
			return false, errors.WithStack(err)
		}

		if err := cr.readData(); err != nil {
			return false, errors.WithStack(err)
		}
	}

	return column.Rows() != 0, nil
}

func (cr *columnReader) readTimestampsV2(column *TimestampColumn) (next bool, err error) {

}

// toAssure: how to decide bools length, because unit is byte??
func (cr *columnReader) readBools(column *BoolColumn) (next bool, err error) {
	presentStream := cr.streams[pb.Stream_PRESENT]
	dd := cr.dataDecoder.(*boolRunLength)

	for dd.consumedIndex != 0 {
		for i := dd.consumedIndex; i < dd.len(); {
			l := len(column.Vector)
			if l < cap(column.Vector) {

				if column.nullable {
					if presentStream == nil || cr.presents[l-1] {
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

			} else {
				// still not finished
				dd.consumedIndex = i
				return true, nil
			}
		}
		// decoder consuming finished
		dd.reset()

		if err := cr.readPresent(); err != nil {
			return false, errors.WithStack(err)
		}

		if err := cr.readData(); err != nil {
			return false, errors.WithStack(err)
		}
	}

	return column.Rows() != 0, nil
}

func (cr *columnReader) readBinaryV2(column *BinaryColumn) (next bool, err error) {
	presentStream := cr.streams[pb.Stream_PRESENT]
	dd := cr.dataDecoder.(*bytesDirectV2)

	for dd.consumedIndex != 0 {
		for i := dd.consumedIndex; i < dd.len(); {
			l := len(column.Vector)
			if l < cap(column.Vector) {

				if column.nullable {
					if presentStream == nil || cr.presents[l-1] {
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

			} else {
				// still not finished
				dd.consumedIndex = i
				return true, nil
			}
		}
		// decoder consuming finished
		dd.reset()

		if err := cr.readPresent(); err != nil {
			return false, errors.WithStack(err)
		}

		if err := cr.readLength(); err != nil {
			return false, errors.WithStack(err)
		}
		dd.length = cr.lengthes

		if err := cr.readData(); err != nil {
			return false, errors.WithStack(err)
		}
	}

	return column.Rows() != 0, nil
}

func (cr *columnReader) readStringsV2(column *StringColumn) (next bool, err error) {
	presentStream := cr.streams[pb.Stream_PRESENT]
	dd := cr.dataDecoder.(*bytesDirectV2)

	for dd.consumedIndex != 0 {
		for i := dd.consumedIndex; i < dd.len(); {
			l := len(column.Vector)
			if l < cap(column.Vector) {

				if column.nullable {
					if presentStream == nil || cr.presents[l-1] {
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

			} else {
				// still not finished
				dd.consumedIndex = i
				return true, nil
			}
		}
		// decoder consuming finished
		dd.reset()

		if err := cr.readPresent(); err != nil {
			return false, errors.WithStack(err)
		}

		if err := cr.readLength(); err != nil {
			return false, errors.WithStack(err)
		}
		dd.length = cr.lengthes

		if err := cr.readData(); err != nil {
			return false, errors.WithStack(err)
		}
	}

	return column.Rows() != 0, nil
}

// toAssure: read present stream all in memory
func (cr *columnReader) readPresent() error {
	presentStream := cr.streams[pb.Stream_PRESENT]
	if presentStream == nil || cr.presentRead {
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
	cr.presents = pd.bools
	cr.presentRead = true

	return nil
}

func (cr *columnReader) readLength() error {
	if cr.lengthRead {
		return nil
	}

	if _, err := cr.f.Seek(int64(cr.lengthStart), 0); err != nil {
		return errors.WithStack(err)
	}

	var n uint64
	lengthBuf := bytes.NewBuffer(make([]byte, cr.opts.ChunkSize))
	lengthBuf.Reset()
	for n < cr.lengthLength {
		l, err := readAChunk(cr.opts, cr.f, lengthBuf)
		if err != nil {
			return errors.WithStack(err)
		}
		n += uint64(l)
	}

	ld := &intRleV2{}
	ld.signed = false
	if err := ld.readValues(lengthBuf); err != nil {
		return errors.WithStack(err)
	}
	cr.lengthes = ld.uliterals
	cr.lengthRead = true
	return nil
}

func (cr *columnReader) readData() error {
	presentStream := cr.streams[pb.Stream_PRESENT]
	dataStream := cr.streams[pb.Stream_DATA]
	assert(dataStream != nil)
	dataStart := cr.dataStart + presentStream.GetLength()
	dataLength := dataStream.GetLength()

	if cr.dataRead < dataLength {
		if _, err := cr.f.Seek(int64(dataStart+cr.dataRead), 0); err != nil {
			return errors.WithStack(err)
		}

		cr.dataBuf.Reset()
		l, err := readAChunk(cr.opts, cr.f, cr.dataBuf)
		if err != nil {
			return errors.WithStack(err)
		}
		cr.dataRead += l

		// decode
		if err := cr.dataDecoder.readValues(cr.dataBuf); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (cr *columnReader) readBytes(column *TinyIntColumn) (next bool, err error) {
	presentStream := cr.streams[pb.Stream_PRESENT]
	dd := cr.dataDecoder.(*byteRunLength)

	for dd.consumedIndex != 0 {
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
		// decoder consuming finished
		dd.reset()

		if err := cr.readPresent(); err != nil {
			return false, errors.WithStack(err)
		}

		if err := cr.readData(); err != nil {
			return false, errors.WithStack(err)
		}
	}

	return column.Rows() != 0, nil
}

func (cr *columnReader) readLongsV2(column *LongColumn) (next bool, err error) {
	presentStream := cr.streams[pb.Stream_PRESENT]
	dd := cr.dataDecoder.(*intRleV2)

	for dd.consumedIndex != 0 {
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
		// decoder consuming finished
		dd.reset()

		if err := cr.readPresent(); err != nil {
			return false, errors.WithStack(err)
		}

		if err := cr.readData(); err != nil {
			return false, errors.WithStack(err)
		}
	}

	return column.Rows() != 0, nil
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

func assert(condition bool) error {
	if !condition {
		return errors.New("assert error!")
	}
	return nil
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
		return 0, errors.New("chunk length larger than chunk size")
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
