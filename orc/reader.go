package orc

import (
	"bytes"
	"compress/flate"
	"fmt"
	"github.com/PatrickHuang888/goorc/pb/pb"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"strings"
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
	for i, stripeInfo := range r.tail.Footer.Stripes {
		offset := stripeInfo.GetOffset()
		indexLength := stripeInfo.GetIndexLength()
		dataLength := stripeInfo.GetDataLength()
		ps := r.tail.GetPostscript()

		// row index
		indexOffset := offset
		if _, err = r.f.Seek(int64(indexOffset), 0); err != nil {
			return nil, errors.WithStack(err)
		}
		indexBuf := make([]byte, indexLength)
		if _, err = io.ReadFull(r.f, indexBuf); err != nil {
			return nil, errors.WithStack(err)
		}
		if ps.GetCompression() != pb.CompressionKind_NONE {
			ib := &bytes.Buffer{}
			if err = decompressBuffer(ps.GetCompression(), ib, bytes.NewBuffer(indexBuf));
				err != nil {
				return nil, err
			}
			indexBuf = ib.Bytes()
		}
		index := &pb.RowIndex{}
		if err = proto.Unmarshal(indexBuf, index); err != nil {
			return nil, errors.Wrapf(err, "unmarshal strip index error")
		}

		// footer
		footerOffset := int64(offset + indexLength + dataLength)
		if _, err := r.f.Seek(footerOffset, 0); err != nil {
			return nil, errors.WithStack(err)
		}
		footerBuf := make([]byte, stripeInfo.GetFooterLength())
		if _, err = io.ReadFull(r.f, footerBuf); err != nil {
			return nil, errors.WithStack(err)
		}
		if ps.GetCompression() != pb.CompressionKind_NONE {
			fb := &bytes.Buffer{}
			if err = decompressBuffer(r.tail.GetPostscript().GetCompression(), fb, bytes.NewBuffer(footerBuf));
				err != nil {
				return nil, err
			}
			footerBuf = fb.Bytes()
		}
		footer := &pb.StripeFooter{}
		if err = proto.Unmarshal(footerBuf, footer); err != nil {
			return nil, errors.Wrapf(err, "unmarshal currentStripe footer error")
		}

		sr := &stripeReader{f: r.f, opts: r.opts, footer: footer, schemas: r.schemas, info: stripeInfo, idx: i}
		if err := sr.prepare(); err != nil {
			return ss, errors.WithStack(err)
		}
		log.Debugf("get stripe %d : %s", sr.idx, sr.info.String())
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
	id           int
	schema       *TypeDescription
	encoding     *pb.ColumnEncoding
	f            *os.File
	numberOfRows uint64
	opts         *ReaderOptions

	streams map[pb.Stream_Kind]*streamReader

	//indexStart  uint64 // index area start
	//indexLength uint64

	presents       []bool // present data, all in memory
	presentDecoder Decoder
	presentRead    bool // if present already read or no present

	read             bool    // if streams read
	dataDecoder      Decoder // data decoder
	secondaryDecoder Decoder
	lengthDecoder    Decoder
	dictDecoder      Decoder
	//lengthes         []uint64 // length data
	//secondaries      []uint64

	readCursor uint64
}

func (cr *columnReader) String() string {
	sb := strings.Builder{}
	fmt.Fprintf(&sb, "id %d, ", cr.id)
	fmt.Fprintf(&sb, "kind %s, ", cr.schema.Kind.String())
	fmt.Fprintf(&sb, "encoding %s, ", cr.encoding.String())
	fmt.Fprintf(&sb, "read cursor %d", cr.readCursor)
	return sb.String()
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
		cr.presentDecoder = &boolRunLength{}
		crs[i] = cr
	}

	indexStart := sr.info.GetOffset()
	dataStart := indexStart + sr.info.GetIndexLength()
	for _, si := range sr.footer.GetStreams() {
		id := si.GetColumn()
		k := si.GetKind()
		length := si.GetLength()
		buf := bytes.NewBuffer(make([]byte, sr.opts.ChunkSize))
		buf.Reset()
		var s *streamReader
		if k == pb.Stream_ROW_INDEX {
			s = &streamReader{start: indexStart, length: length, kind: k, buf: buf, f: sr.f, opts: sr.opts}
			indexStart += length
		} else {
			s = &streamReader{start: dataStart, length: length, kind: k, buf: buf, f: sr.f, opts: sr.opts}
			dataStart += length
		}
		crs[id].streams[k] = s
	}

	sr.columnReaders = crs

	/*for _, v := range crs {
		v.Print()
	}*/
	return nil
}

// a stripe is typically  ~200MB
func (sr *stripeReader) NextBatch(batch ColumnVector) (next bool, err error) {
	batch.reset()

	cr := sr.columnReaders[batch.ColumnId()]
	log.Debugf("column: %s reading", cr.String())

	if err = cr.readPresent(); err != nil {
		return false, errors.WithStack(err)
	}

	encoding := cr.encoding.GetKind()
	switch cr.schema.Kind {
	case pb.Type_SHORT:
		fallthrough
	case pb.Type_INT:
		fallthrough
	case pb.Type_LONG:
		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			if cr.dataDecoder == nil {
				cr.dataDecoder = &intRleV2{signed: true, literals: make([]int64, cr.numberOfRows)}
			}
			return cr.readLongsV2(batch.(*LongColumn))
		}

		return false, errors.Errorf("encoding %s for int/long not impl", encoding)

	case pb.Type_FLOAT:
	// todo:

	case pb.Type_DOUBLE:
		if encoding != pb.ColumnEncoding_DIRECT {
			return false, errors.Errorf("column %d double should encoding DIRECT", batch.ColumnId())
		}
		if cr.dataDecoder == nil {
			cr.dataDecoder = &ieee754Double{values:make([]float64, cr.numberOfRows)}
		}
		return cr.readDoubles(batch.(*DoubleColumn))

	case pb.Type_STRING:
		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			if cr.dataDecoder == nil {
				cr.dataDecoder = &bytesContent{}
			}
			if cr.lengthDecoder == nil {
				cr.lengthDecoder = &intRleV2{signed: false}
			}
			return cr.readStringsV2(batch.(*StringColumn))
		}

		if encoding == pb.ColumnEncoding_DICTIONARY_V2 {
			if cr.dataDecoder == nil {
				cr.dataDecoder = &intRleV2{signed: false}
			}
			if cr.dictDecoder == nil {
				cr.dictDecoder = &bytesContent{}
			}
			if cr.lengthDecoder == nil {
				cr.lengthDecoder = &intRleV2{signed: false}
			}
			return cr.readStringsDictV2(batch.(*StringColumn))
		}

		return false, errors.Errorf("column %d type %s encoding %s not impl", cr.id, cr.schema.Kind.String(), encoding)

	case pb.Type_BOOLEAN:
		if cr.dataDecoder == nil {
			cr.dataDecoder = &boolRunLength{}
		}
		return cr.readBools(batch.(*BoolColumn))

	case pb.Type_BYTE: // TinyInt
		if cr.dataDecoder == nil {
			cr.dataDecoder = &byteRunLength{}
		}
		return cr.readBytes(batch.(*TinyIntColumn))

	case pb.Type_BINARY:
		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			if cr.dataDecoder == nil {
				cr.dataDecoder = &bytesContent{}
			}
			if cr.lengthDecoder == nil {
				cr.lengthDecoder = &intRleV2{signed: false}
			}
			return cr.readBinaryV2(batch.(*BinaryColumn))
		}

		return false, errors.Errorf("encoding %s for binary not impl", encoding)

	case pb.Type_DECIMAL:
		column, ok := batch.(*Decimal64Column)
		if !ok {
			return false, errors.New("decimal column should be decimal64")
		}
		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			if cr.dataDecoder == nil {
				cr.dataDecoder = &base128VarInt{values:make([]int64, cr.numberOfRows)}
			}
			if cr.secondaryDecoder == nil {
				cr.secondaryDecoder = &intRleV2{signed: false, uliterals:make([]uint64, cr.numberOfRows)}
			}
			return cr.readDecimal64sV2(column)
		}

		return false, errors.Errorf("encoding %s for decimal not impl", encoding)

	case pb.Type_DATE:
		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			if cr.dataDecoder == nil {
				cr.dataDecoder = &intRleV2{signed: true}
			}
			return cr.readDatesV2(batch.(*DateColumn))
		}

		return false, errors.Errorf("encoding %s  for date not impl", encoding)

	case pb.Type_TIMESTAMP:
		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			if cr.dataDecoder == nil {
				cr.dataDecoder = &intRleV2{signed: true, literals:make([]int64, cr.numberOfRows)}
			}
			if cr.secondaryDecoder == nil {
				cr.secondaryDecoder = &intRleV2{signed: false, uliterals:make([]uint64, cr.numberOfRows)}
			}
			return cr.readTimestampsV2(batch.(*TimestampColumn))
		}

		return false, errors.Errorf("encoding %s for timestamp not impl", encoding)

	case pb.Type_STRUCT:
		column := batch.(*StructColumn)
		// reAssure: next value calculation
		for _, f := range column.Fields {
			n, err := sr.NextBatch(f)
			if err != nil {
				return false, err
			}
			if n {
				next = true
			}
		}

	case pb.Type_UNION:
		// todo:

	case pb.Type_LIST:
		// todo:
		/*if encoding == pb.ColumnEncoding_DIRECT_V2 {
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
		}*/

	default:
		return false, errors.Errorf("type %s not impl", cr.schema.Kind.String())
	}
	return
}

func (cr *columnReader) readDatesV2(column *DateColumn) (next bool, err error) {
	data := cr.streams[pb.Stream_DATA]
	assertx(data != nil)
	dd := cr.dataDecoder.(*intRleV2)

	if !cr.read {
		if err := data.readWhole(cr.opts, cr.f); err != nil {
			return false, err
		}

		dd.reset()
		if err := dd.readValues(data.buf); err != nil {
			return false, err
		}

		cr.read = true
	}

	for i := dd.consumedIndex; i < dd.len(); {
		l := len(column.Vector)
		if l < cap(column.Vector) {
			if column.nullable {
				if cr.presents == nil || (cr.presents != nil && cr.presents[l-1]) {
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
			dd.consumedIndex = i
			return true, nil
		}
	}

	return false, nil
}

func (cr *columnReader) readTimestampsV2(column *TimestampColumn) (next bool, err error) {
	data := cr.streams[pb.Stream_DATA]
	assertx(data != nil)
	secondary := cr.streams[pb.Stream_SECONDARY]
	assertx(secondary != nil)
	dd := cr.dataDecoder.(*intRleV2)
	sd := cr.secondaryDecoder.(*intRleV2)

	if !cr.read {

		if err := data.readWhole(cr.opts, cr.f); err != nil {
			return false, err
		}
		if err := secondary.readWhole(cr.opts, cr.f); err != nil {
			return false, err
		}

		dd.reset()
		if err := dd.readValues(data.buf); err != nil {
			return false, err
		}

		sd.reset()
		if err := sd.readValues(secondary.buf); err != nil {
			return false, errors.WithStack(err)
		}

		assertx(dd.len() == sd.len())

		cr.read = true
	}

	i := dd.consumedIndex
	j := sd.consumedIndex
	for ; i < dd.len(); {
		l := len(column.Vector)
		if l < cap(column.Vector) {
			if column.nullable {
				if cr.presents == nil || (cr.presents != nil && cr.presents[l-1]) {
					column.Nulls = append(column.Nulls, false)
					column.Vector = append(column.Vector, Timestamp{dd.literals[i], uint32(sd.uliterals[j])})
					i++
					j++
				} else {
					column.Nulls = append(column.Nulls, true)
					column.Vector = append(column.Vector, Timestamp{})
				}
			} else { // no nulls
				column.Vector = append(column.Vector, Timestamp{dd.literals[i], uint32(sd.uliterals[j])})
				i++
				j++
			}
			cr.readCursor++
		} else {
			dd.consumedIndex = i
			return true, nil
		}
	}
	dd.consumedIndex = i
	sd.consumedIndex = j

	return false, nil
}

func (cr *columnReader) readBools(column *BoolColumn) (next bool, err error) {
	dd := cr.dataDecoder.(*boolRunLength)
	data := cr.streams[pb.Stream_DATA]
	assertx(data != nil)

	if !cr.read {
		if err := data.readWhole(cr.opts, cr.f); err != nil {
			return false, err
		}

		dd.reset()
		if err := dd.readValues(data.buf); err != nil {
			return false, err
		}

		cr.read = true
	}

	// because bools extend to byte, may not know the real rows from read,
	// so using number of rows
	for cr.readCursor < cr.numberOfRows {
		for i := dd.consumedIndex; i < dd.len(); {
			l := len(column.Vector)
			if l < cap(column.Vector) {
				if column.nullable {
					if cr.presents == nil || (cr.presents != nil && cr.presents[l-1]) {
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
				dd.consumedIndex = i
				return true, nil
			}
		}
	}

	return false, nil
}

func (cr *columnReader) readBinaryV2(column *BinaryColumn) (next bool, err error) {
	data := cr.streams[pb.Stream_DATA]
	assertx(data != nil)
	length := cr.streams[pb.Stream_LENGTH]
	assertx(length != nil)
	dd := cr.dataDecoder.(*bytesContent)
	ld := cr.lengthDecoder.(*intRleV2)

	if !cr.read {
		if err := length.readWhole(cr.opts, cr.f); err != nil {
			return false, errors.WithStack(err)
		}

		ld.reset()
		if err := ld.readValues(length.buf); err != nil {
			return false, err
		}
		dd.length = ld.uliterals

		if err := data.readWhole(cr.opts, cr.f); err != nil {
			return false, errors.WithStack(err)
		}

		dd.reset()
		if err := dd.readValues(data.buf); err != nil {
			return false, err
		}

		cr.read = true
	}

	for i := dd.consumedIndex; i < dd.len(); {
		l := len(column.Vector)
		if l < cap(column.Vector) {
			if column.nullable {
				if cr.presents == nil || (cr.presents != nil && cr.presents[l-1]) {
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

	return false, nil
}

func (cr *columnReader) readStringsV2(column *StringColumn) (next bool, err error) {
	dd := cr.dataDecoder.(*bytesContent)
	data := cr.streams[pb.Stream_DATA]
	assertx(data != nil)
	ld := cr.lengthDecoder.(*intRleV2)
	length := cr.streams[pb.Stream_LENGTH]
	assertx(length != nil)

	if !cr.read {
		// length read first
		if err := length.readWhole(cr.opts, cr.f); err != nil {
			return false, errors.WithStack(err)
		}

		ld.reset()
		if err := ld.readValues(length.buf); err != nil {
			return false, err
		}
		dd.length = ld.uliterals

		if err := data.readWhole(cr.opts, cr.f); err != nil {
			return false, errors.WithStack(err)
		}

		dd.reset()
		if err := dd.readValues(data.buf); err != nil {
			return false, err
		}

		cr.read = true
	}

	for i := dd.consumedIndex; i < dd.len(); {
		l := len(column.Vector)
		if l < cap(column.Vector) {
			if column.nullable {
				if cr.presents == nil || cr.presents[l-1] {
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
			dd.consumedIndex = i
			return true, nil
		}
	}

	return false, nil
}

func (cr *columnReader) readStringsDictV2(column *StringColumn) (next bool, err error) {
	dd := cr.dataDecoder.(*intRleV2)
	data := cr.streams[pb.Stream_DATA]
	assertx(data != nil)
	dictDecoder := cr.dictDecoder.(*bytesContent)
	dictData := cr.streams[pb.Stream_DICTIONARY_DATA]
	assertx(dictData != nil)
	ld := cr.lengthDecoder.(*intRleV2)
	length := cr.streams[pb.Stream_LENGTH]
	assertx(length != nil)

	if !cr.read {

		if err := data.readWhole(cr.opts, cr.f); err != nil {
			return false, err
		}

		dd.reset()
		if err := dd.readValues(data.buf); err != nil {
			return false, err
		}

		// length stream read first
		if err := length.readWhole(cr.opts, cr.f); err != nil {
			return false, err
		}

		ld.reset()
		if err := ld.readValues(length.buf); err != nil {
			return false, err
		}
		dictDecoder.length = ld.uliterals

		if err := dictData.readWhole(cr.opts, cr.f); err != nil {
			return false, errors.WithStack(err)
		}

		dictDecoder.reset()
		if err := dictDecoder.readValues(dictData.buf); err != nil {
			return false, err
		}

		cr.read = true
	}

	for i := dd.consumedIndex; i < dd.len(); {
		l := len(column.Vector)
		if l < cap(column.Vector) {
			if column.nullable {
				if cr.presents == nil || cr.presents[l-1] {
					column.Nulls = append(column.Nulls, false)
					v := dictDecoder.content[dd.uliterals[i]]
					column.Vector = append(column.Vector, string(v))
					i++
				} else {
					column.Nulls = append(column.Nulls, true)
					column.Vector = append(column.Vector, "")
				}
			} else { // no nulls
				v := dictDecoder.content[dd.uliterals[i]]
				column.Vector = append(column.Vector, string(v))
				i++
			}
			cr.readCursor++
		} else {
			dd.consumedIndex = i
			return true, nil
		}
	}

	return false, nil
}

func (cr *columnReader) readBytes(column *TinyIntColumn) (next bool, err error) {
	data := cr.streams[pb.Stream_DATA]
	assertx(data != nil)
	dd := cr.dataDecoder.(*byteRunLength)

	if !cr.read {

		if err := data.readWhole(cr.opts, cr.f); err != nil {
			return false, errors.WithStack(err)
		}

		dd.reset()
		if err := dd.readValues(data.buf); err != nil {
			return false, err
		}

		cr.read = true
	}

	for i := dd.consumedIndex; i < dd.len(); {
		l := len(column.Vector)
		if l < cap(column.Vector) {
			if column.nullable {
				if cr.presents == nil || (cr.presents != nil && cr.presents[l-1]) {
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
			dd.consumedIndex = i
			return true, nil
		}
	}

	return false, nil
}

func (cr *columnReader) readLongsV2(column *LongColumn) (next bool, err error) {
	dataDecoder := cr.dataDecoder.(*intRleV2)
	data := cr.streams[pb.Stream_DATA]
	assertx(data != nil)

	if !cr.read {
		if err := data.readWhole(cr.opts, cr.f); err != nil {
			return false, err
		}

		dataDecoder.reset()
		if err := dataDecoder.readValues(data.buf); err != nil {
			return false, err
		}

		cr.read = true
	}

	for i := dataDecoder.consumedIndex; i < dataDecoder.len(); {
		l := len(column.Vector)
		if l < cap(column.Vector) {
			if column.nullable {
				if cr.presents == nil || (cr.presents != nil && cr.presents[cr.readCursor]) {
					column.Nulls = append(column.Nulls, false)
					column.Vector = append(column.Vector, dataDecoder.literals[i])
					i++
				} else {
					column.Nulls = append(column.Nulls, true)
					column.Vector = append(column.Vector, 0)
				}
			} else { // no nulls
				column.Vector = append(column.Vector, dataDecoder.literals[i])
				i++
			}
			cr.readCursor++
		} else {
			dataDecoder.consumedIndex = i
			return true, nil
		}
	}

	return false, nil
}

func (cr *columnReader) readDecimal64sV2(column *Decimal64Column) (next bool, err error) {
	data := cr.streams[pb.Stream_DATA]
	assertx(data != nil)
	secondary := cr.streams[pb.Stream_SECONDARY]
	assertx(secondary != nil)
	dd := cr.dataDecoder.(*base128VarInt)
	sd := cr.secondaryDecoder.(*intRleV2)

	if !cr.read {

		if err := secondary.readWhole(cr.opts, cr.f); err != nil {
			return false, errors.WithStack(err)
		}
		sd.reset()
		if err := sd.readValues(secondary.buf); err != nil {
			return false, err
		}

		if err := data.readWhole(cr.opts, cr.f); err != nil {
			return false, err
		}
		dd.reset()
		if err := dd.readValues(data.buf); err != nil {
			return false, err
		}

		cr.read = true
	}

	for i := dd.consumedIndex; i < dd.len(); {
		l := len(column.Vector)
		if l < cap(column.Vector) {
			if column.nullable {
				if cr.presents == nil || (cr.presents != nil && cr.presents[l-1]) {
					column.Nulls = append(column.Nulls, false)
					column.Vector = append(column.Vector, Decimal64{dd.values[i], uint16(sd.uliterals[i])})
					i++
				} else {
					column.Nulls = append(column.Nulls, true)
					column.Vector = append(column.Vector, Decimal64{})
				}
			} else { // no nulls
				column.Vector = append(column.Vector, Decimal64{dd.values[i], uint16(sd.uliterals[i])})
				i++
			}
			cr.readCursor++
		} else {
			dd.consumedIndex = i
			return true, nil
		}
	}

	return false, nil
}

func (cr *columnReader) readDoubles(column *DoubleColumn) (next bool, err error) {
	data := cr.streams[pb.Stream_DATA]
	assertx(data != nil)
	dd := cr.dataDecoder.(*ieee754Double)

	if !cr.read {

		if err := data.readWhole(cr.opts, cr.f); err != nil {
			return false, errors.WithStack(err)
		}

		dd.reset()
		if err := dd.readValues(data.buf); err != nil {
			return false, err
		}

		cr.read = true
	}

	for i := dd.consumedIndex; i < dd.len(); {
		l := len(column.Vector)
		if l < cap(column.Vector) {
			if column.nullable {
				if cr.presents == nil || (cr.presents != nil && cr.presents[l-1]) {
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
			dd.consumedIndex = i
			return true, nil
		}
	}

	return false, nil
}

// toAssure: read present stream all in memory
func (cr *columnReader) readPresent() error {
	if cr.presentRead {
		return nil
	}
	present := cr.streams[pb.Stream_PRESENT]
	if present == nil {
		cr.presentRead = true
		return nil
	}
	cr.presentDecoder.reset()
	if err := present.readWhole(cr.opts, cr.f); err != nil {
		return errors.WithStack(err)
	}
	if err := cr.presentDecoder.readValues(present.buf); err != nil {
		return err
	}
	cr.presents = cr.presentDecoder.(*boolRunLength).bools[0:cr.numberOfRows]
	cr.presentRead = true
	return nil
}

type streamReader struct {
	start      uint64
	length     uint64
	readLength uint64
	kind       pb.Stream_Kind
	buf        *bytes.Buffer

	opts *ReaderOptions
	f    *os.File
}

func (stream *streamReader) String() string {
	return fmt.Sprintf("start %d, length %d, kind %s, already read %d", stream.start, stream.length,
		stream.kind.String(), stream.readLength)
}

// stream read one or more chunks a time when needed
/*func (stream *streamReader) read(dec Decoder) error {
	log.Debugf("stream %s reading", stream.String())

	if _, err := f.Seek(int64(stream.start+stream.readLength), 0); err != nil {
		return errors.WithStack(err)
	}

	//stream.buf.Reset()

	//stream.buf.Truncate(stream.buf.Len())

	fmt.Printf("read chunk %d \n", stream.readLength)
	l, err := readAChunk(opts, f, stream.buf)
	if err != nil {
		return err
	}
	stream.readLength += l

	err := dec.readValues(stream)
	if err != nil {
		return errors.WithMessagef(err, "already decoding length %d", dec.len())
	}

	//}

	return nil
}*/

/*func (stream *streamReader) ReadByte() (byte, error) {
	if stream.buf.Len() == 0 && !stream.finish() {
		stream.buf.Reset()

		// Rethink: using 1 f in whole reader or 1 f per streaming ?
		if _, err := stream.f.Seek(int64(stream.start+stream.readLength), 0); err != nil {
			return 0, errors.WithStack(err)
		}

		l, err := readAChunk(stream.opts, stream.f, stream.buf)
		log.Debugf("stream %s readAChunk %d", stream.kind.String(), l)
		if err != nil {
			return 0, err
		}
		stream.readLength += l
	}
	b, err := stream.buf.ReadByte()
	if err != nil {
		return b, errors.WithStack(err)
	}
	return b, nil
}

func (stream *streamReader) Read(p []byte) (n int, err error) {
	if stream.buf.Len() == 0 && !stream.finish() {
		stream.buf.Reset()

		if _, err := stream.f.Seek(int64(stream.start+stream.readLength), 0); err != nil {
			return 0, errors.WithStack(err)
		}

		l, err := readAChunk(stream.opts, stream.f, stream.buf)
		log.Debugf("read %d", l)
		if err != nil {
			return 0, err
		}
		stream.readLength += l
	}
	n, err = stream.buf.Read(p)
	if err != nil {
		return n, errors.WithStack(err)
	}
	return n, nil
}

func (stream *streamReader) Len() int {
	return stream.buf.Len()
}*/

// read whole stream into memory
func (stream *streamReader) readWhole(opts *ReaderOptions, f *os.File) (err error) {
	if _, err = f.Seek(int64(stream.start), 0); err != nil {
		return errors.WithStack(err)
	}
	stream.buf.Reset()

	if opts.CompressionKind == pb.CompressionKind_NONE {
		if _, err = io.CopyN(stream.buf, f, int64(stream.length)); err != nil {
			return errors.WithStack(err)
		}
		stream.readLength += stream.length
		return nil
	}

	for ; !stream.finish(); {
		l, err := readAChunk(opts, f, stream.buf)
		if err != nil {
			return err
		}
		stream.readLength += l
	}
	return nil
}

func (stream *streamReader) finish() bool {
	return stream.readLength >= stream.length
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
	footerBuf := buf[footerStart : footerStart+footerSize]
	if ps.GetCompression() != pb.CompressionKind_NONE {
		fb := bytes.NewBuffer(make([]byte, ps.GetCompressionBlockSize()))
		fb.Reset()
		if err := decompressBuffer(ps.GetCompression(), fb, bytes.NewBuffer(footerBuf)); err != nil {
			return nil, errors.WithStack(err)
		}
		footerBuf = fb.Bytes()
	}
	footer := &pb.Footer{}
	if err = proto.Unmarshal(footerBuf, footer); err != nil {
		return nil, errors.Wrapf(err, "unmarshal footer error")
	}

	log.Debugf("Footer: %s\n", footer.String())

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

	if _, err := decompressChunkData(opts.CompressionKind, original, out, buf); err != nil {
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

// data should be compressed
func decompressChunkData(kind pb.CompressionKind, original bool, dst *bytes.Buffer, src *bytes.Buffer) (n int64, err error) {
	assertx(kind != pb.CompressionKind_NONE)
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
			if err != nil {
				return 0, errors.Wrapf(err, "decompress chunk data error")
			}
			return n, nil
		default:
			return 0, errors.New("compression kind other than zlib not impl")
		}
	}
	return
}

// buffer should be compressed, maybe contains several chunks
func decompressBuffer(kind pb.CompressionKind, dst *bytes.Buffer, src *bytes.Buffer) (err error) {
	assertx(kind != pb.CompressionKind_NONE)
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
