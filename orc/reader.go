package orc

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/PatrickHuang888/goorc/pb/pb"
	"github.com/golang/protobuf/proto"
	"github.com/patrickhuang888/goorc/orc/encoding"
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
	Stripes() ([]Stripe, error)
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
	stripes []Stripe
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

func (r *reader) Stripes() (ss []Stripe, err error) {
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
			if err = decompressBuffer(ps.GetCompression(), ib, bytes.NewBuffer(indexBuf)); err != nil {
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
			if err = decompressBuffer(r.tail.GetPostscript().GetCompression(), fb, bytes.NewBuffer(footerBuf)); err != nil {
				return nil, err
			}
			footerBuf = fb.Bytes()
		}
		footer := &pb.StripeFooter{}
		if err = proto.Unmarshal(footerBuf, footer); err != nil {
			return nil, errors.Wrapf(err, "unmarshal currentStripe footer error")
		}

		sr := &stripe{f: r.f, opts: r.opts, footer: footer, schemas: r.schemas, info: stripeInfo, idx: i}
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

type Stripe interface {
	NextBatch(batch ColumnVector) (bool, error)
}

type stripe struct {
	f       *os.File
	schemas []*TypeDescription
	opts    *ReaderOptions

	info   *pb.StripeInformation
	footer *pb.StripeFooter

	columns []*column
	idx           int
}

type column struct {
	id           int
	schema       *TypeDescription
	encoding     *pb.ColumnEncoding
	f            *os.File
	numberOfRows uint64
	opts         *ReaderOptions

	streams map[pb.Stream_Kind]streamReader

	cursor uint64
}

func (c *column) String() string {
	sb := strings.Builder{}
	fmt.Fprintf(&sb, "id %d, ", c.id)
	fmt.Fprintf(&sb, "kind %s, ", c.schema.Kind.String())
	fmt.Fprintf(&sb, "encoding %s, ", c.encoding.String())
	fmt.Fprintf(&sb, "read cursor %d", c.cursor)
	return sb.String()
}

// stripe {index{},column{[present],data,[length]},footer}
func (s *stripe) prepare() error {
	columnSize := len(s.schemas)
	columns := make([]*column, columnSize)
	for i := 0; i < len(s.schemas); i++ {
		c := &column{}
		c.id = i
		c.schema = s.schemas[i]
		c.encoding = s.footer.GetColumns()[i]
		c.streams = make(map[pb.Stream_Kind]stream)
		c.f = s.f
		c.numberOfRows = s.info.GetNumberOfRows()
		c.opts = s.opts
		columns[i] = c
	}

	indexStart := s.info.GetOffset()
	dataStart := indexStart + s.info.GetIndexLength()
	
	for _, ss := range s.footer.GetStreams() {
		id := ss.GetColumn()
		kind := ss.GetKind()
		length := ss.GetLength()
		buf := bytes.NewBuffer(make([]byte, s.opts.ChunkSize))
		buf.Reset()
		var stream stream
		if kind == pb.Stream_ROW_INDEX {
			// todo: init index sr
			indexStart += length
		} else {
			sr := &sr{start: dataStart, length: length, kind: kind, buf: buf, f: s.f, opts: s.opts}

			if kind== pb.Stream_PRESENT {
				decoder := &encoding.BoolRunLength{}
				stream = &boolSR{r: sr, decoder: decoder}
			}

			columnEncoding := columns[id].encoding.GetKind()
			switch columns[id].schema.Kind {
			case pb.Type_SHORT:
				fallthrough
			case pb.Type_INT:
				fallthrough
			case pb.Type_LONG:
				if columnEncoding == pb.ColumnEncoding_DIRECT_V2 {
					if kind== pb.Stream_DATA {
						decoder:= &encoding.IntRleV2{}
						stream= &longSR{r:sr, signed:true, decoder:decoder}
					}
					break
				}
				return errors.New("not impl")

			case pb.Type_FLOAT:
			// todo:

			case pb.Type_DOUBLE:
				if columnEncoding != pb.ColumnEncoding_DIRECT {
					return errors.New("column encoding error")
				}
				if kind==pb.Stream_DATA {
					decoder:= &encoding.Ieee754Double{}
					stream= &ieeeFloatSR{r:sr, decoder:decoder}
				}

			case pb.Type_STRING:
				if columnEncoding == pb.ColumnEncoding_DIRECT_V2 {
					if kind== pb.Stream_DATA {
						stream= &bytesContentSR{r:sr}
						break
					}
					if kind==pb.Stream_LENGTH {
						decoder:= &encoding.IntRleV2{}
						stream= &longSR{r:sr, signed:false, decoder:decoder}
						break
					}
				}
				if columnEncoding == pb.ColumnEncoding_DICTIONARY_V2 {
					if kind== pb.Stream_DATA {
						stream= &bytesContentSR{r:sr}
						break
					}
					if kind==pb.Stream_LENGTH {
						decoder:= &encoding.IntRleV2{}
						stream= &longSR{r:sr, signed:false, decoder:decoder}
						break
					}
					if kind==pb.Stream_DICTIONARY_DATA {
						stream= &bytesContentSR{r:sr}
						break
					}
				}
				return errors.New("column encoding error")
			}




			dataStart += length
		}
		columns[id].streams[kind] = stream
	}

	s.columns = columns

	/*for _, v := range crs {
		v.Print()
	}*/
	return nil
}

// entry of stripe reader
// a stripe is typically  ~200MB
func (sr *stripe) NextBatch(batch ColumnVector) (next bool, err error) {

	c := sr.columns[batch.ColumnId()]
	log.Debugf("column: %s reading", c.String())

	batch.reset()

	encoding := c.encoding.GetKind()
	switch c.schema.Kind {
	case pb.Type_SHORT:
		fallthrough
	case pb.Type_INT:
		fallthrough
	case pb.Type_LONG:
		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			return c.nextLongsV2(batch.(*LongColumn))
		}

	case pb.Type_FLOAT:
	// todo:

	case pb.Type_DOUBLE:
		return c.nextDoubles(batch.(*DoubleColumn))

	case pb.Type_STRING:
		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			return c.nextStringsV2(batch.(*StringColumn))
		}

		if encoding == pb.ColumnEncoding_DICTIONARY_V2 {
			return c.nextStringsDictV2(batch.(*StringColumn))
		}

		return false, errors.Errorf("column %d type %s encoding %s not impl", c.id, c.schema.Kind.String(), encoding)

	case pb.Type_BOOLEAN:
		return c.nextBools(batch.(*BoolColumn))

	case pb.Type_BYTE: // TinyInt
		return c.nextBytes(batch.(*TinyIntColumn))

	case pb.Type_BINARY:
		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			return c.nextBinaryV2(batch.(*BinaryColumn))
		}

		return false, errors.Errorf("encoding %s for binary not impl", encoding)

	case pb.Type_DECIMAL:
		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			return c.nextDecimal64sV2(batch.(*Decimal64Column))
		}

		return false, errors.Errorf("encoding %s for decimal not impl", encoding)

	case pb.Type_DATE:
		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			return c.nextDatesV2(batch.(*DateColumn))
		}

		return false, errors.Errorf("encoding %s  for date not impl", encoding)

	case pb.Type_TIMESTAMP:
		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			return c.nextTimestampsV2(batch.(*TimestampColumn))
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
		return false, errors.Errorf("type %s not impl", c.schema.Kind.String())
	}
	return
}

func (c *column) nextDatesV2(batch *DateColumn) (next bool, err error) {
	if err = c.nextPresents(batch.presents); err != nil {
		return false, err
	}

	data := c.streams[pb.Stream_DATA].(*longSR)

	i := 0
	for ; i < cap(batch.Vector) && !data.finished(); i++ {
		if len(batch.presents) == 0 || (len(batch.presents) != 0 && batch.presents[i]) {
			v, err := data.nextInt()
			if err != nil {
				return false, err
			}
			// opti:
			batch.Vector = append(batch.Vector, fromDays(v))
		} else {
			batch.Vector = append(batch.Vector, Date{})
		}
	}

	c.cursor += uint64(i)

	return !data.finished(), nil
}

func (c *column) nextTimestampsV2(batch *TimestampColumn) (next bool, err error) {
	if err = c.nextPresents(batch.presents); err != nil {
		return false, err
	}

	data := c.streams[pb.Stream_DATA].(*longSR)
	secondary := c.streams[pb.Stream_SECONDARY].(*longSR)

	i := 0
	for ; i < cap(batch.Vector) && !data.finished() && !secondary.finished(); i++ {
		if len(batch.presents) == 0 || (len(batch.presents) != 0 && batch.presents[i]) {
			seconds, err := data.nextInt()
			if err != nil {
				return false, err
			}
			nanos, err := data.nextUInt()
			if err != nil {
				return false, err
			}
			// opti:
			batch.Vector = append(batch.Vector, Timestamp{seconds, uint32(nanos)})
		} else {
			batch.Vector = append(batch.Vector, Timestamp{})
		}
	}

	if (data.finished() && !secondary.finished()) || (secondary.finished() && !data.finished()) {
		return false, errors.New("read error")
	}

	c.cursor += uint64(i)

	return !data.finished(), nil
}

func (c *column) nextBools(batch *BoolColumn) (next bool, err error) {
	if err = c.nextPresents(batch.presents); err != nil {
		return false, err
	}

	data := c.streams[pb.Stream_DATA].(*boolSR)

	// ??
	// because bools extend to byte, may not know the real rows from read,
	// so using number of rows
	for i := 0; c.cursor < c.numberOfRows && i < cap(batch.Vector); i++ {
		if len(batch.presents) != 0 {
			assertx(i <= len(batch.presents))
		}

		if len(batch.presents) == 0 || (len(batch.presents) != 0 && batch.presents[i]) {
			v, err := data.next()
			if err != nil {
				return false, err
			}
			batch.Vector = append(batch.Vector, v)
		} else {
			batch.Vector = append(batch.Vector, false)
		}
		c.cursor++
	}

	return c.cursor == c.numberOfRows, nil
}

func (c *column) nextBinaryV2(batch *BinaryColumn) (next bool, err error) {
	if err = c.nextPresents(batch.presents); err != nil {
		return false, err
	}

	length := c.streams[pb.Stream_LENGTH]
	data := c.streams[pb.Stream_DATA]

	i := 0
	for ; i < cap(batch.Vector) && !data.finished(); i++ {
		if len(batch.presents) != 0 {
			assertx(i < len(batch.presents))
		}

		if len(batch.presents) == 0 || (len(batch.presents) != 0 && batch.presents[i]) {
			l, err := length.(*longSR).nextUInt()
			if err != nil {
				return false, err
			}
			v, err := data.(*bytesContentSR).next(l)
			if err != nil {
				return false, err
			}
			batch.Vector = append(batch.Vector, v)
		} else {
			batch.Vector = append(batch.Vector, []byte{})
		}
	}

	c.cursor = c.cursor + uint64(i)
	return !data.finished(), nil
}

func (c *column) nextStringsV2(batch *StringColumn) (next bool, err error) {
	if err = c.nextPresents(batch.presents); err != nil {
		return false, err
	}

	length := c.streams[pb.Stream_LENGTH]
	data := c.streams[pb.Stream_DATA]

	i := 0
	for ; i < cap(batch.Vector) && !data.finished() && !length.finished(); i++ {
		if len(batch.presents) == 0 || (len(batch.presents) != 0 && batch.presents[i]) {
			l, err := length.(*longSR).nextUInt()
			if err != nil {
				return false, err
			}
			v, err := data.(*bytesContentSR).next(l)
			if err != nil {
				return false, err
			}
			// default utf-8
			batch.Vector = append(batch.Vector, string(v))
		} else {
			batch.Vector = append(batch.Vector, "")
		}
	}

	if (length.finished() && !data.finished()) || (data.finished() && !length.finished()) {
		return false, errors.New("read error")
	}

	c.cursor += uint64(i)

	return !data.finished(), nil
}

func (c *column) nextStringsDictV2(batch *StringColumn) (next bool, err error) {
	if err = c.nextPresents(batch.presents); err != nil {
		return false, err
	}

	data := c.streams[pb.Stream_DATA].(*longSR)
	dictData := c.streams[pb.Stream_DICTIONARY_DATA].(*bytesContentSR)
	dictLength := c.streams[pb.Stream_LENGTH].(*longSR)

	ls, err := dictLength.getAllUInts()
	if err != nil {
		return false, err
	}

	ds, err := dictData.getAll(ls)
	if err != nil {
		return false, err
	}

	i := 0
	for ; i < cap(batch.Vector) && !data.finished(); i++ {
		if len(batch.presents) == 0 || (len(batch.presents) != 0 && batch.presents[i]) {
			v, err := data.nextUInt()
			if err != nil {
				return false, err
			}
			if v >= uint64(len(ds)) {
				return false, errors.New("dict index error")
			}
			batch.Vector = append(batch.Vector, string(ds[v]))
		} else {
			batch.Vector = append(batch.Vector, "")
		}
	}

	c.cursor += uint64(i)

	return !data.finished(), nil
}

func (c *column) nextBytes(batch *TinyIntColumn) (next bool, err error) {
	if err = c.nextPresents(batch.presents); err != nil {
		return false, err
	}

	data := c.streams[pb.Stream_DATA].(*byteSR)

	i := 0
	for ; i < cap(batch.Vector) && !data.finished(); i++ {
		if len(batch.presents) == 0 || (len(batch.presents) != 0 && batch.presents[i]) {
			v, err := data.next()
			if err != nil {
				return false, err
			}
			batch.Vector = append(batch.Vector, v)
		} else {
			batch.Vector = append(batch.Vector, 0)
		}
	}

	c.cursor += uint64(i)

	return !data.finished(), nil
}

func (c *column) nextLongsV2(batch *LongColumn) (next bool, err error) {
	if err = c.nextPresents(batch.presents); err != nil {
		return false, err
	}

	// rethink: assert data!=nil
	data := c.streams[pb.Stream_DATA].(*longSR)

	i := 0
	for ; !data.finished() && i < cap(batch.Vector); i++ {
		// rethink: i and present index check

		if len(batch.presents) == 0 || (len(batch.presents) != 0 && batch.presents[i]) {
			v, err := data.nextInt()
			if err != nil {
				return false, err
			}
			batch.Vector = append(batch.Vector, v)
		} else {
			batch.Vector = append(batch.Vector, 0)
		}
	}

	c.cursor += uint64(i)

	return !data.finished(), nil
}

func (c *column) nextDecimal64sV2(batch *Decimal64Column) (next bool, err error) {
	if err = c.nextPresents(batch.presents); err != nil {
		return false, err
	}

	data := c.streams[pb.Stream_DATA].(*int64VarIntSR)
	secondary := c.streams[pb.Stream_SECONDARY].(*longSR)

	i := 0
	for ; i < cap(batch.Vector) && !data.finished() && !secondary.finished(); i++ {
		if len(batch.presents) == 0 || (len(batch.presents) != 0 && batch.presents[i]) {
			precision, err := data.next()
			if err != nil {
				return false, err
			}
			scala, err := secondary.nextUInt()
			if err != nil {
				return false, err
			}
			batch.Vector = append(batch.Vector, Decimal64{precision, uint16(scala)})
		} else {
			batch.Vector = append(batch.Vector, Decimal64{})
		}
	}

	if (data.finished() && !secondary.finished()) || (secondary.finished() && !data.finished()) {
		return false, errors.New("read error")
	}

	c.cursor += uint64(i)

	return !data.finished(), nil
}

func (c *column) nextDoubles(batch *DoubleColumn) (next bool, err error) {
	if err = c.nextPresents(batch.presents); err != nil {
		return false, err
	}

	data := c.streams[pb.Stream_DATA].(*ieeeFloatSR)

	i := 0
	for ; i < cap(batch.Vector) && !data.finished(); i++ {
		if len(batch.presents) == 0 || (len(batch.presents) != 0 && batch.presents[i]) {
			v, err := data.next()
			if err != nil {
				return false, err
			}
			batch.Vector = append(batch.Vector, v)
		} else {
			batch.Vector = append(batch.Vector, 0)
		}
	}

	c.cursor += uint64(i)

	return !data.finished(), nil
}

func (c *column) nextPresents(presents []bool) (err error) {
	ps := c.streams[pb.Stream_PRESENT]
	if ps != nil {
		for !ps.finished() && len(presents) < cap(presents) {
			v, err := ps.(*boolSR).next()
			if err != nil {
				return err
			}
			presents = append(presents, v)
		}
	}
	return nil
}

type stream interface {
	//io.ByteReader
	//io.Reader
	//Len() int

	// read finished and decoded consumed
	finished() bool
}

type sr struct {
	start      uint64
	length     uint64
	readLength uint64
	kind       pb.Stream_Kind
	buf        *bytes.Buffer

	opts *ReaderOptions

	f *os.File
}

func (stream *sr) String() string {
	return fmt.Sprintf("start %d, length %d, kind %s, already read %d", stream.start, stream.length,
		stream.kind.String(), stream.readLength)
}

type byteSR struct {
	r *sr

	values   []byte
	consumed int

	decoder *encoding.ByteRunLength
}

func (s *byteSR) next() (v byte, err error) {
	if s.consumed == len(s.values) {
		s.values = s.values[:0]
		s.consumed = 0

		if err = s.decoder.ReadValues(s.r, s.values); err != nil {
			return 0, err
		}
	}

	v = s.values[s.consumed]
	s.consumed++
	return
}

func (s *byteSR) finished() bool {
	return s.r.readFinished() && (s.consumed == len(s.values))
}

type bytesContentSR struct {
	r *sr

	values []byte
	pos    int
}

func (s *bytesContentSR) next(length uint64) (v []byte, err error) {
	if s.pos+int(length) > len(s.values) {
		s.values = s.values[s.pos:]
		s.pos = 0
		buf := bytes.NewBuffer(s.values)

		if _, err = s.r.ReadAChunk(buf); err != nil {
			return nil, err
		}
	}

	v = s.values[s.pos : s.pos+int(length)]
	s.pos += int(length)
	return
}

func (s *bytesContentSR) finished() bool {
	return s.r.readFinished() && (s.pos == len(s.values))
}

// for stream like dict
func (s *bytesContentSR) getAll(lengthAll []uint64) (vs [][]byte, err error) {
	buf := bytes.NewBuffer(s.values)
	for !s.r.readFinished() {
		if _, err = s.r.ReadAChunk(buf); err != nil {
			return nil, err
		}
	}

	for _, l := range lengthAll {
		if s.pos+int(l) > len(s.values) {
			return nil, errors.New("read data error")
		}
		vs = append(vs, s.values[s.pos:s.pos+int(l)])
		s.pos += int(l)
	}

	return
}

type ieeeFloatSR struct {
	r *sr

	values []float64
	pos    int

	decoder *encoding.Ieee754Double
}

func (s *ieeeFloatSR) next() (v float64, err error) {
	if s.pos >= len(s.values) {
		s.pos = 0
		s.values = s.values[:0]

		if err = s.decoder.ReadValues(s.r, s.values); err != nil {
			return 0, err
		}
	}

	v = s.values[s.pos]
	s.pos++
	return
}

func (s *ieeeFloatSR) finished() bool {
	return s.r.readFinished() && (s.pos == len(s.values))
}

type int64VarIntSR struct {
	r *sr

	values []int64
	pos    int

	decoder *encoding.Base128VarInt
}

func (s *int64VarIntSR) next() (v int64, err error) {
	if s.pos >= len(s.values) {
		s.pos = 0
		s.values = s.values[:0]

		if err = s.decoder.ReadValues(s.r, s.values); err != nil {
			return 0, err
		}
	}

	v = s.values[s.pos]
	s.pos++
	return
}

func (s *int64VarIntSR) finished() bool {
	return s.r.readFinished() && (s.pos == len(s.values))
}

type longSR struct {
	r *sr

	signed bool
	values []uint64
	pos    int

	decoder *encoding.IntRleV2
}

func (s *longSR) nextInt() (v int64, err error) {
	if !s.signed {
		return 0, errors.New("signed error")
	}
	x, err := s.nextUInt()
	if err != nil {
		return
	}
	v = encoding.UnZigzag(x)
	return
}

func (s *longSR) nextUInt() (v uint64, err error) {
	if s.pos >= len(s.values) {
		s.pos = 0
		s.values = s.values[:0]

		if err = s.decoder.ReadValues(s.r, s.signed, s.values); err != nil {
			return
		}
	}

	v = s.values[s.pos]
	s.pos++
	return
}

// for small data like dict index, ignore s.signed
func (s *longSR) getAllUInts() (vs []uint64, err error) {
	for !s.r.readFinished() {
		if err = s.decoder.ReadValues(s.r, false, vs); err != nil {
			return
		}
	}
	return
}

func (s *longSR) finished() bool {
	return s.r.readFinished() && (s.pos == len(s.values))
}

type boolSR struct {
	r *sr

	values  []byte
	pos     int
	bytePos int

	decoder *encoding.BoolRunLength
}

func (s *boolSR) next() (v bool, err error) {
	if s.pos >= len(s.values) {
		s.pos = 0
		s.values = s.values[:0]

		if err=s.decoder.ReadValues(s.r, s.values);err!=nil {
			return
		}
	}

	v = s.values[s.pos]>>byte(7-s.bytePos) == 0x01
	s.bytePos++
	if s.bytePos == 8 {
		s.bytePos = 0
		s.pos++
	}
	return
}

func (s *boolSR) finished() bool {
	// fixme:
	return s.r.readFinished() && (s.pos == len(s.values))
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

func (stream *sr) ReadByte() (byte, error) {
	b, err := stream.buf.ReadByte()
	if err != nil {
		if err == io.EOF && !stream.readFinished() {

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
			return stream.buf.ReadByte()

		} else {
			return b, errors.WithStack(err)
		}
	}
	return b, nil
}

func (stream *sr) Read(p []byte) (n int, err error) {
	n, err = stream.buf.Read(p)
	if err != nil {
		if err == io.EOF && !stream.readFinished() {

			if _, err := stream.f.Seek(int64(stream.start+stream.readLength), 0); err != nil {
				return 0, errors.WithStack(err)
			}

			l, err := readAChunk(stream.opts, stream.f, stream.buf)
			log.Debugf("read %d", l)
			if err != nil {
				return 0, err
			}
			stream.readLength += l
			return stream.buf.Read(p)

		} else {
			return n, errors.WithStack(err)
		}
	}
	return n, nil
}

func (r *sr) ReadAChunk(buf *bytes.Buffer) (n int, err error) {
	if _, err := r.f.Seek(int64(r.start+r.readLength), 0); err != nil {
		return 0, errors.WithStack(err)
	}

	l, err := readAChunk(r.opts, r.f, buf)
	log.Debugf("read %d", n)
	if err != nil {
		return int(l), err
	}
	r.readLength += l
	n = int(l)

	return
}

/*func (stream *streamReader) Len() int {
	return stream.buf.Len()
}
*/
// read whole stream into memory
func (stream *sr) readWhole(opts *ReaderOptions, f *os.File) (err error) {
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

	for !stream.readFinished() {
		l, err := readAChunk(opts, f, stream.buf)
		if err != nil {
			return err
		}
		stream.readLength += l
	}
	return nil
}

func (stream *sr) readFinished() bool {
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
	fullLength := magicLength + 1
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
		// opti:
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
