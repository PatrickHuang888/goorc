package orc

import (
	"bytes"
	"compress/flate"
	"io"
	"os"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/patrickhuang888/goorc/orc/encoding"
	"github.com/patrickhuang888/goorc/pb/pb"
)

const (
	MIN_ROW_INDEX_STRIDE         = 1000
	DEFAULT_STRIPE_SIZE          = 256 * 1024 * 1024
	DefalutBufferSize            = 10 * 1024 * 2014
	DEFAULT_INDEX_SIZE           = 100 * 1024
	DEFAULT_PRESENT_SIZE         = 100 * 1024
	DEFAULT_DATA_SIZE            = 1 * 1024 * 1024
	DEFAULT_LENGTH_SIZE          = 100 * 1024
	DEFAULT_ENCODING_BUFFER_SIZE = 100 * 1024
	DEFAULT_CHUNK_SIZE           = 256 * 1024
	MAX_CHUNK_LENGTH             = uint64(32768) // 15 bit
)

var VERSION = []uint32{0, 12}

type WriterOptions struct {
	ChunkSize       int
	CompressionKind pb.CompressionKind
	StripeSize      uint64 // ~200MB
	BufferSize      uint   // written data in memory
	//FlushLater      bool //if true means every write, will write in memory,
	// then flush out at some threshold,
	//but if flush fail, previous write into memory would be lost
}

func DefaultWriterOptions() *WriterOptions {
	o := &WriterOptions{}
	o.CompressionKind = pb.CompressionKind_ZLIB
	o.StripeSize = DEFAULT_STRIPE_SIZE
	o.ChunkSize = DEFAULT_CHUNK_SIZE
	o.BufferSize = DefalutBufferSize
	return o
}

type Writer interface {
	GetSchema() *TypeDescription

	Write(batch *ColumnVector) error

	Close() error
}

type fileWriter struct {
	path string
	f    *os.File

	*writer
}

// current version always write new file, no append
func NewFileWriter(path string, schema *TypeDescription, opts *WriterOptions) (writer Writer, err error) {
	// todo: overwrite exist file warning
	log.Infof("open %stream", path)
	f, err := os.Create(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	w, err := newWriter(schema, opts, f)
	if err != nil {
		return nil, err
	}

	writer = &fileWriter{f: f, path: path, writer: w}
	return writer, nil
}

func (w *fileWriter) Close() error {
	return w.close()
}

func newWriter(schema *TypeDescription, opts *WriterOptions, out io.WriteCloser) (*writer, error) {
	schemas := schema.normalize()

	w := &writer{opts: opts, schemas: schemas, out: out}
	n, err := w.writeHeader()
	if err != nil {
		return nil, err
	}

	w.offset = n
	w.stripe, err = newStripe(w.offset, w.schemas, w.opts)
	if err != nil {
		return nil, err
	}
	return w, nil
}

// cannot used concurrently, not synchronized
// strip buffered in memory until the strip size
// Encode out by columns
type writer struct {
	schemas []*TypeDescription
	opts    *WriterOptions

	offset uint64

	stripe *stripeWriter

	stripeInfos []*pb.StripeInformation
	columnStats []*pb.ColumnStatistics

	ps *pb.PostScript

	out io.WriteCloser
}

// because stripe data in file is stream sequence, so every data should write to memory first
// before write to file
func (w *writer) Write(batch *ColumnVector) error {
	if err := w.stripe.writeColumn(batch); err != nil {
		return err
	}

	if w.stripe.size() > w.opts.StripeSize {
		if err := w.flushStripe(); err != nil {
			return err
		}
	}

	return nil
}

func (w *writer) flushStripe() error {
	if err := w.stripe.writeout(w.out); err != nil {
		return err
	}

	if _, err := w.stripe.writeFooter(w.out); err != nil {
		return err
	}

	//next
	w.offset += w.stripe.size()
	w.stripeInfos = append(w.stripeInfos, w.stripe.info)
	w.stripe.reset()
	*w.stripe.info.Offset = w.offset

	return nil
}

func newStripe(offset uint64, schemas []*TypeDescription, opts *WriterOptions) (stripe *stripeWriter, err error) {
	idxBuf := bytes.NewBuffer(make([]byte, DEFAULT_INDEX_SIZE))
	idxBuf.Reset()

	// prepare streams
	var columns []columnWriter
	for _, schema := range schemas {
		var column columnWriter
		if column, err = createColumnWriter(schema, opts); err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}

	o := offset
	info := &pb.StripeInformation{Offset: &o, IndexLength: new(uint64), DataLength: new(uint64), FooterLength: new(uint64),
		NumberOfRows: new(uint64)}
	s := &stripeWriter{opts: opts, idxBuf: idxBuf, columnWriters: columns, info: info, schemas: schemas}
	return s, nil
}

func createColumnWriter(schema *TypeDescription, opts *WriterOptions) (writer columnWriter, err error) {
	switch schema.Kind {
	case pb.Type_SHORT:
		fallthrough
	case pb.Type_INT:
		fallthrough
	case pb.Type_LONG:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			writer = newLongV2Writer(schema, opts)
			break
		}
		return nil, errors.New("encoding not impl")

	case pb.Type_FLOAT:
		writer = newFloatWriter(schema, opts)
	case pb.Type_DOUBLE:
		writer = newDoubleWriter(schema, opts)

	case pb.Type_CHAR:
		fallthrough
	case pb.Type_VARCHAR:
		fallthrough
	case pb.Type_STRING:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			writer = newStringDirectV2Writer(schema, opts)
			break
		}

		if schema.Encoding == pb.ColumnEncoding_DICTIONARY_V2 {
			writer = newStringDictV2Writer(schema, opts)
			break
		}

		return nil, errors.New("encoding not impl")

	case pb.Type_BOOLEAN:
		if schema.Encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("encoding error")
		}
		writer = newBoolWriter(schema, opts)

	case pb.Type_BYTE:
		if schema.Encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("encoding error")
		}
		writer = newByteWriter(schema, opts)

	case pb.Type_BINARY:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			writer = newBinaryDirectV2Writer(schema, opts)
			break
		}

		return nil, errors.New("encoding not impl")

	case pb.Type_DECIMAL:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			writer = newDecimal64DirectV2Writer(schema, opts)
			break
		}
		return nil, errors.New("encoding not impl")

	case pb.Type_DATE:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			writer = newDateDirectV2Writer(schema, opts)
			break
		}
		return nil, errors.New("encoding not impl")

	case pb.Type_TIMESTAMP:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			writer = newTimestampDirectV2Writer(schema, opts)
			break
		}
		return nil, errors.New("encoding not impl")

	case pb.Type_STRUCT:
		if schema.Encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("encoding error")
		}
		writer, err = newStructWriter(schema, opts)

	case pb.Type_LIST:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			// todo:
			return nil, errors.New("not impl")
			break
		}
		return nil, errors.New("encoding error")

	case pb.Type_MAP:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			// todo:
			return nil, errors.New("not impl")
			break
		}
		return nil, errors.New("encoding not impl")

	case pb.Type_UNION:
		if schema.Encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("encoding error")
		}
		// todo:
		return nil, errors.New("not impl")

	default:
		return nil, errors.New("column kind unknown")
	}
	return
}

type stripeWriter struct {
	opts    *WriterOptions
	schemas []*TypeDescription

	columnWriters []columnWriter

	idxBuf *bytes.Buffer // index area buffer

	info *pb.StripeInformation  // data in info not update every write,
}

// write to memory
func (s *stripeWriter) writeColumn(batch *ColumnVector) error {

	writer := s.columnWriters[batch.Id]
	rows, err := writer.write(batch)
	if err != nil {
		return err
	}

	if !s.schemas[batch.Id].HasFather {
		*s.info.NumberOfRows += rows
	}

	// todo: update index length
	*s.info.DataLength= 0
	for _, c := range s.columnWriters {
		for _, stream := range c.getStreams() {
			*s.info.DataLength+= stream.info.GetLength()
		}
	}

	// todo: update column stats

	return nil
}

func (s stripeWriter) getBufferedDataSize() uint {
	var l uint
	for _, cw := range s.columnWriters {
		for _, sw := range cw.getStreams() {
			l += uint(sw.buf.Len())
		}
	}
	return l
}

func (s stripeWriter) shouldFlushMemory() bool {
	return s.getBufferedDataSize() >= s.opts.BufferSize
}

// no footer length
func (s stripeWriter) size() uint64 {
	return s.info.GetIndexLength() + s.info.GetDataLength()+s.info.GetFooterLength()
}

// stripe should be self-contained
func (s *stripeWriter) writeout(out io.Writer) error {
	// todo: index length
	if _, err := s.idxBuf.WriteTo(out); err != nil {
		return errors.WithStack(err)
	}
	log.Tracef("flush index of length %d", s.info.GetIndexLength())

	for _, column := range s.columnWriters {
		for _, stream := range column.getStreams() {

			if _, err := stream.writeOut(out); err != nil {
				return err
			}

			log.Debugf("flush stream %s", stream.info.String())

		}
	}

	return nil
}

func (s *stripeWriter) writeFooter(out io.Writer) (footer *pb.StripeFooter, err error) {
	footer = &pb.StripeFooter{}

	for _, column := range s.columnWriters {
		for _, stream := range column.getStreams() {
			if stream.info.GetLength() != 0 {
				footer.Streams = append(footer.Streams, stream.info)
			}
		}
	}

	for _, schema := range s.schemas {
		footer.Columns = append(footer.Columns, &pb.ColumnEncoding{Kind: &schema.Encoding})
	}

	var footerBuf []byte
	footerBuf, err = proto.Marshal(footer)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var compressedFooterBuf []byte
	if compressedFooterBuf, err = compressByteSlice(s.opts.CompressionKind, s.opts.ChunkSize, footerBuf); err != nil {
		return
	}

	var n int
	if n, err = out.Write(compressedFooterBuf); err != nil {
		return nil, errors.WithStack(err)
	}

	*s.info.FooterLength = uint64(n)

	log.Infof("write out stripe footer %s", footer.String())
	return
}

func (s *stripeWriter) reset() {
	s.info = &pb.StripeInformation{Offset: new(uint64), IndexLength: new(uint64), DataLength: new(uint64),
		FooterLength: new(uint64), NumberOfRows: new(uint64)}

	s.idxBuf.Reset()

	for _, column := range s.columnWriters {
		for _, stream := range column.getStreams() {
			stream.reset()
		}
	}
}

// enhance: if presents all false, no write

type columnWriter interface {
	write(batch *ColumnVector) (rows uint64, err error)
	getStreams() []*streamWriter
}

type cwBase struct {
	schema  *TypeDescription
	present *streamWriter
}

type structWriter struct {
	*cwBase
	childrenWriters []columnWriter
}

func newStructWriter(schema *TypeDescription, opts *WriterOptions) (writer *structWriter, err error) {
	cw_ := &cwBase{schema: schema, present: newBoolStreamWriter(schema.Id, pb.Stream_PRESENT, opts)}
	if schema.Kind != pb.Type_STRUCT {
		panic("schema not struct")
	}
	var childrenWriters []columnWriter
	for _, childSchema := range schema.Children {
		var childWriter columnWriter
		childWriter, err = createColumnWriter(childSchema, opts)
		if err != nil {
			return
		}
		childrenWriters = append(childrenWriters, childWriter)
	}
	writer = &structWriter{cw_, childrenWriters}
	return
}

func (c *structWriter) write(batch *ColumnVector) (rows uint64, err error) {
	if err = batch.check(); err != nil {
		return 0, err
	}

	if c.schema.HasNulls && batch.Presents != nil {
		if err := c.present.writeValues(batch.Presents); err != nil {
			return 0, err
		}
	}
	childrenVector := batch.Vector.([]*ColumnVector)
	if len(c.childrenWriters) != len(childrenVector) {
		return 0, errors.New("children vector not match children writer")
	}
	for i, child := range childrenVector {
		// rethink: rows
		if rows, err = c.childrenWriters[i].write(child); err != nil {
			return 0, err
		}
	}
	return
}

func (c *structWriter) getStreams() []*streamWriter {
	ss := make([]*streamWriter, 1)
	ss[0] = c.present
	return ss
}

type timestampDirectV2Writer struct {
	*cwBase
	data      *streamWriter
	secondary *streamWriter
}

func newTimestampDirectV2Writer(schema *TypeDescription, opts *WriterOptions) *timestampDirectV2Writer {
	cw_ := &cwBase{schema: schema, present: newBoolStreamWriter(schema.Id, pb.Stream_PRESENT, opts)}
	data_ := newIntV2Stream(schema.Id, pb.Stream_DATA, true, opts)
	secondary_ := newIntV2Stream(schema.Id, pb.Stream_SECONDARY, false, opts)
	return &timestampDirectV2Writer{cw_, data_, secondary_}
}

func (c *timestampDirectV2Writer) write(batch *ColumnVector) (rows uint64, err error) {
	var seconds []uint64
	var nanos []uint64
	values := batch.Vector.([]Timestamp)
	rows = uint64(len(values))

	if c.schema.HasNulls {
		if len(batch.Presents) != len(values) {
			return 0, errors.New("rows of present != vector")
		}

		if err := c.present.writeValues(batch.Presents); err != nil {
			return 0, err
		}

		for i, p := range batch.Presents {
			if p {
				seconds = append(seconds, encoding.Zigzag(values[i].Seconds))
				nanos = append(nanos, uint64(values[i].Nanos))
			}
		}
	} else {
		for _, v := range values {
			seconds = append(seconds, encoding.Zigzag(v.Seconds))
			nanos = append(nanos, uint64(v.Nanos))
		}
	}

	if err := c.data.writeValues(seconds); err != nil {
		return 0, err
	}
	if err = c.secondary.writeValues(nanos); err != nil {
		return 0, err
	}
	return
}

func (c *timestampDirectV2Writer) getStreams() []*streamWriter {
	ss := make([]*streamWriter, 3)
	ss[0] = c.present
	ss[1] = c.data
	ss[2] = c.secondary
	return ss
}

type dateV2Writer struct {
	*cwBase
	data *streamWriter
}

func newDateDirectV2Writer(schema *TypeDescription, opts *WriterOptions) *dateV2Writer {
	cw_ := &cwBase{schema: schema, present: newBoolStreamWriter(schema.Id, pb.Stream_PRESENT, opts)}
	data_ := newIntV2Stream(schema.Id, pb.Stream_DATA, true, opts)
	return &dateV2Writer{cw_, data_}
}

func (c *dateV2Writer) write(batch *ColumnVector) (rows uint64, err error) {
	var vector []uint64
	values := batch.Vector.([]Date)
	rows = uint64(len(values))

	if c.schema.HasNulls {
		if len(batch.Presents) != len(values) {
			return 0, errors.New("rows of present != vector")
		}

		if err := c.present.writeValues(batch.Presents); err != nil {
			return 0, err
		}

		for i, p := range batch.Presents {
			if p {
				vector = append(vector, encoding.Zigzag(toDays(values[i])))
			}
		}
	} else {
		for _, v := range values {
			vector = append(vector, encoding.Zigzag(toDays(v)))
		}
	}

	if err := c.data.writeValues(vector); err != nil {
		return 0, err
	}
	return
}

func (c *dateV2Writer) getStreams() []*streamWriter {
	ss := make([]*streamWriter, 2)
	ss[0] = c.present
	ss[1] = c.data
	return ss
}

type decimal64DirectV2Writer struct {
	*cwBase
	data      *streamWriter
	secondary *streamWriter
}

func newDecimal64DirectV2Writer(schema *TypeDescription, opts *WriterOptions) *decimal64DirectV2Writer {
	cw_ := &cwBase{schema: schema, present: newBoolStreamWriter(schema.Id, pb.Stream_PRESENT, opts)}
	data_ := newBase128VarIntStreamWriter(schema.Id, pb.Stream_DATA, opts)
	secondary_ := newIntV2Stream(schema.Id, pb.Stream_SECONDARY, false, opts)
	return &decimal64DirectV2Writer{cw_, data_, secondary_}
}

func (c *decimal64DirectV2Writer) write(batch *ColumnVector) (rows uint64, err error) {
	var precisions []int64
	var scales []uint64
	values := batch.Vector.([]Decimal64)
	rows = uint64(len(values))

	if c.schema.HasNulls {
		if len(batch.Presents) != len(values) {
			return 0, errors.New("rows of present != vector")
		}

		if err := c.present.writeValues(batch.Presents); err != nil {
			return 0, err
		}

		for i, p := range batch.Presents {
			if p {
				precisions = append(precisions, values[i].Precision)
				scales = append(scales, uint64(values[i].Scale))
			}
		}
	} else {
		for _, v := range values {
			precisions = append(precisions, v.Precision)
			scales = append(scales, uint64(v.Scale))
		}
	}

	if err := c.data.writeValues(precisions); err != nil {
		return 0, err
	}

	if err = c.secondary.writeValues(scales); err != nil {
		return 0, err
	}

	return
}

func (c *decimal64DirectV2Writer) getStreams() []*streamWriter {
	ss := make([]*streamWriter, 3)
	ss[0] = c.present
	ss[1] = c.data
	ss[2] = c.secondary
	return ss
}

type doubleWriter struct {
	*cwBase
	data *streamWriter
}

func newDoubleWriter(schema *TypeDescription, opts *WriterOptions) *doubleWriter {
	c := &cwBase{schema: schema, present: newBoolStreamWriter(schema.Id, pb.Stream_PRESENT, opts)}
	data := newDoubleStream(schema.Id, pb.Stream_DATA, opts)
	return &doubleWriter{cwBase: c, data: data}
}

func (c *doubleWriter) write(batch *ColumnVector) (rows uint64, err error) {
	var vector []float64
	values := batch.Vector.([]float64)
	rows = uint64(len(values))

	if c.schema.HasNulls {
		if len(batch.Presents) != len(values) {
			return 0, errors.New("rows of present != vector")
		}

		if err := c.present.writeValues(batch.Presents); err != nil {
			return 0, err
		}

		for i, p := range batch.Presents {
			if p {
				vector = append(vector, values[i])
			}
		}
	} else {

		vector = values
	}

	if err := c.data.writeValues(vector); err != nil {
		return 0, err
	}

	return
}

func (c *doubleWriter) getStreams() []*streamWriter {
	ss := make([]*streamWriter, 2)
	ss[0] = c.present
	ss[1] = c.data
	return ss
}

type binaryDirectV2Writer struct {
	*cwBase
	data   *streamWriter
	length *streamWriter
}

func newBinaryDirectV2Writer(schema *TypeDescription, opts *WriterOptions) *binaryDirectV2Writer {
	cw_ := &cwBase{schema: schema, present: newBoolStreamWriter(schema.Id, pb.Stream_PRESENT, opts)}
	data_ := newStringContentsStream(schema.Id, pb.Stream_DATA, opts)
	length_ := newIntV2Stream(schema.Id, pb.Stream_LENGTH, false, opts)
	return &binaryDirectV2Writer{cw_, data_, length_}
}

func (c *binaryDirectV2Writer) write(batch *ColumnVector) (rows uint64, err error) {
	var vector [][]byte
	var lengthVector []uint64
	values := batch.Vector.([][]byte)
	rows = uint64(len(values))

	if c.schema.HasNulls {
		if len(batch.Presents) != len(values) {
			return 0, errors.New("rows of present != vector")
		}

		if err = c.present.writeValues(batch.Presents); err != nil {
			return
		}

		for i, p := range batch.Presents {
			if p {
				vector = append(vector, values[i])
				lengthVector = append(lengthVector, uint64(len(values[i])))
			}
		}
	} else {

		for _, v := range values {
			lengthVector = append(lengthVector, uint64(len(v)))
		}
	}

	if err = c.data.writeValues(vector); err != nil {
		return
	}

	if err = c.length.writeValues(lengthVector); err != nil {
		return
	}

	// rethink: presents, data, length writing transaction
	// it should be flush to file?
	return
}

func (c *binaryDirectV2Writer) getStreams() []*streamWriter {
	ss := make([]*streamWriter, 3)
	ss[0] = c.present
	ss[1] = c.data
	ss[2] = c.length
	return ss
}

type stringV2Writer struct {
	schema  *TypeDescription
	opts    *WriterOptions
	directW *stringDirectV2Writer
	dictW   *stringDictV2Writer
}

func newStringV2Writer(schema *TypeDescription, opts *WriterOptions) *stringV2Writer {
	return &stringV2Writer{schema: schema, opts: opts}
}

func (c *stringV2Writer) write(batch *ColumnVector) (rows uint64, err error) {
	if c.schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
		c.directW = newStringDirectV2Writer(c.schema, c.opts)
		return c.directW.write(batch)
	}
	if c.schema.Encoding == pb.ColumnEncoding_DICTIONARY_V2 {
		c.dictW = newStringDictV2Writer(c.schema, c.opts)
		return c.dictW.write(batch)
	}
	return 0, errors.New("column encoding not impl")
}

func (c *stringV2Writer) getStreams() []*streamWriter {
	if c.schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
		return c.directW.getStreams()
	}
	if c.schema.Encoding == pb.ColumnEncoding_DICTIONARY_V2 {
		return c.dictW.getStreams()
	}
	return nil
}

//rethink: how string encoding determined, right now is key/data less than 50%
const StringDictThreshold = 0.5

// cannot determine on runtime, encoding should be determined at start of stripe
func determineStringEncoding(batch *ColumnVector) pb.ColumnEncoding_Kind {
	values := batch.Vector.([]string)
	dict := make(map[string]bool)
	var keyCount int
	for _, v := range values {
		_, exist := dict[v]
		if !exist {
			keyCount++
			dict[v] = true
		}
	}
	var r pb.ColumnEncoding_Kind
	if float64(keyCount)/float64(len(values)) < StringDictThreshold {
		r = pb.ColumnEncoding_DICTIONARY_V2
	} else {
		r = pb.ColumnEncoding_DIRECT_V2
	}
	return r
}

type stringDictV2Writer struct {
	*cwBase
	data     *streamWriter
	dictData *streamWriter
	length   *streamWriter
}

func newStringDictV2Writer(schema *TypeDescription, opts *WriterOptions) *stringDictV2Writer {
	cw_ := &cwBase{schema: schema, present: newBoolStreamWriter(schema.Id, pb.Stream_PRESENT, opts)}
	data_ := newIntV2Stream(schema.Id, pb.Stream_DATA, false, opts)
	dictData_ := newStringContentsStream(schema.Id, pb.Stream_DICTIONARY_DATA, opts)
	length_ := newIntV2Stream(schema.Id, pb.Stream_LENGTH, false, opts)
	return &stringDictV2Writer{cwBase: cw_, data: data_, dictData: dictData_, length: length_}
}

func (c *stringDictV2Writer) write(batch *ColumnVector) (rows uint64, err error) {
	presents := batch.Presents
	vector := batch.Vector.([]string)
	rows = uint64(len(vector)) // is there rows +=1 if !present?
	var values []string

	if c.schema.HasNulls {
		if len(presents) != len(vector) {
			return 0, errors.New("rows of present != vector")
		}

		if err = c.present.writeValues(presents); err != nil {
			return
		}

		for i, p := range batch.Presents {
			if p {
				values = append(values, vector[i])
			}
		}
	} else {
		values = vector
	}

	d := &dict{}
	for _, v := range values {
		d.put(v)
	}

	// rethink: if data write sucsessful and length write fail, because right now data is in memory
	if err = c.data.writeValues(d.indexes); err != nil {
		return
	}
	if err = c.dictData.writeValues(d.contents); err != nil {
		return
	}
	if err = c.length.writeValues(d.lengths); err != nil {
		return
	}

	return
}

type dict struct {
	contents [][]byte
	lengths  []uint64
	indexes  []uint64
}

func (d *dict) put(s string) {
	idx := d.contains(s)
	if idx == -1 {
		d.contents = append(d.contents, []byte(s))
		d.lengths = append(d.lengths, uint64(len(s)))
		d.indexes = append(d.indexes, uint64(len(d.contents)-1))
	} else {
		d.indexes = append(d.indexes, uint64(idx))
	}
}

func (d dict) contains(s string) int {
	for i, c := range d.contents {
		// rethink: == on string
		if string(c) == s {
			return i
		}
	}
	return -1
}

func (c *stringDictV2Writer) getStreams() []*streamWriter {
	ss := make([]*streamWriter, 4)
	ss[0] = c.present
	ss[1] = c.data
	ss[2] = c.dictData
	ss[3] = c.length
	return ss
}

type stringDirectV2Writer struct {
	*cwBase
	data   *streamWriter
	length *streamWriter
}

func newStringDirectV2Writer(schema *TypeDescription, opts *WriterOptions) *stringDirectV2Writer {
	cw_ := &cwBase{schema: schema, present: newBoolStreamWriter(schema.Id, pb.Stream_PRESENT, opts)}
	data_ := newStringContentsStream(schema.Id, pb.Stream_DATA, opts)
	length_ := newIntV2Stream(schema.Id, pb.Stream_LENGTH, false, opts)
	return &stringDirectV2Writer{cw_, data_, length_}
}

func (c *stringDirectV2Writer) write(batch *ColumnVector) (rows uint64, err error) {
	var lengthVector []uint64
	var contents [][]byte
	values := batch.Vector.([]string)
	rows = uint64(len(values))

	if c.schema.HasNulls {
		if len(batch.Presents) != len(values) {
			return 0, errors.New("rows of present != vector")
		}

		if err = c.present.writeValues(batch.Presents); err != nil {
			return
		}

		for i, p := range batch.Presents {
			if p {
				contents = append(contents, []byte(values[i])) // rethink: string encoding
				lengthVector = append(lengthVector, uint64(len(values[i])))
			}
		}
	} else {

		for _, s := range values {
			contents = append(contents, []byte(s))
			lengthVector = append(lengthVector, uint64(len(s)))
		}
	}

	if err = c.data.writeValues(contents); err != nil {
		return
	}

	// rethink: if data write sucsessful and length write fail, because right now data is in memory
	if err = c.length.writeValues(lengthVector); err != nil {
		return
	}

	return
}

func (c *stringDirectV2Writer) getStreams() []*streamWriter {
	ss := make([]*streamWriter, 3)
	ss[0] = c.present
	ss[1] = c.data
	ss[2] = c.length
	return ss
}

type boolWriter struct {
	*cwBase
	data *streamWriter
}

func newBoolWriter(schema *TypeDescription, opts *WriterOptions) *boolWriter {
	cw_ := &cwBase{schema: schema, present: newBoolStreamWriter(schema.Id, pb.Stream_PRESENT, opts)}
	data_ := newBoolStreamWriter(schema.Id, pb.Stream_DATA, opts)
	return &boolWriter{cw_, data_}
}

func (c *boolWriter) write(batch *ColumnVector) (rows uint64, err error) {
	var vector []bool
	values := batch.Vector.([]bool)
	rows = uint64(len(values))

	if c.schema.HasNulls {
		if batch.Presents == nil || len(batch.Presents) == 0 {
			log.Warn("no presents")
		}
		if len(batch.Presents) != len(values) {
			return 0, errors.New("present error")
		}

		if err = c.present.writeValues(batch.Presents); err != nil {
			return
		}

		for i, p := range batch.Presents {
			if p {
				vector = append(vector, values[i])
			}
		}

	} else {
		vector = values
	}

	if err = c.data.writeValues(vector); err != nil {
		return
	}

	return
}

func (c *boolWriter) getStreams() []*streamWriter {
	ss := make([]*streamWriter, 2)
	ss[0] = c.present
	ss[1] = c.data
	return ss
}

type byteWriter struct {
	*cwBase
	data *streamWriter
}

func newByteWriter(schema *TypeDescription, opts *WriterOptions) *byteWriter {
	cw_ := &cwBase{schema: schema, present: newBoolStreamWriter(schema.Id, pb.Stream_PRESENT, opts)}
	data_ := newByteStreamWriter(schema.Id, pb.Stream_DATA, opts)
	return &byteWriter{cw_, data_}
}

func (c *byteWriter) write(batch *ColumnVector) (rows uint64, err error) {
	var vector []byte

	values := batch.Vector.([]byte)
	rows = uint64(len(values))

	if c.schema.HasNulls {
		if batch.Presents == nil || len(batch.Presents) == 0 {
			return 0, errors.New("no presents data")
		}
		if len(batch.Presents) != len(values) {
			return 0, errors.New("presents error")
		}

		if err = c.present.writeValues(batch.Presents); err != nil {
			return
		}

		for i, p := range batch.Presents {
			if p {
				vector = append(vector, values[i])
			}
		}

	} else {
		vector = values
	}

	if err = c.data.writeValues(vector); err != nil {
		return
	}

	return
}

func (c *byteWriter) getStreams() []*streamWriter {
	ss := make([]*streamWriter, 2)
	ss[0] = c.present
	ss[1] = c.data
	return ss
}

type longV2Writer struct {
	*cwBase
	data *streamWriter
}

func newLongV2Writer(schema *TypeDescription, opts *WriterOptions) *longV2Writer {
	c := &cwBase{schema: schema, present: newBoolStreamWriter(schema.Id, pb.Stream_PRESENT, opts)}
	data := newIntV2Stream(schema.Id, pb.Stream_DATA, true, opts)
	return &longV2Writer{cwBase: c, data: data}
}

func (c *longV2Writer) write(batch *ColumnVector) (rows uint64, err error) {
	var vector []uint64
	values := batch.Vector.([]int64)
	rows = uint64(len(values))

	if c.schema.HasNulls {
		if batch.Presents == nil || len(batch.Presents) == 0 {
			return 0, errors.New("no presents data")
		}
		if len(batch.Presents) != len(values) {
			return 0, errors.New("rows of presents != vector")
		}

		log.Tracef("writing: long column write %d presents", len(batch.Presents))
		if err = c.present.writeValues(batch.Presents); err != nil {
			return
		}

		for i, p := range batch.Presents {
			if p {
				vector = append(vector, encoding.Zigzag(values[i]))
			}
		}

	} else {
		for _, v := range values {
			vector = append(vector, encoding.Zigzag(v))
		}
	}

	if err = c.data.writeValues(vector); err != nil {
		return
	}

	return
}

func (c longV2Writer) getStreams() []*streamWriter {
	ss := make([]*streamWriter, 2)
	ss[0] = c.present
	ss[1] = c.data
	return ss
}

type floatWriter struct {
	*cwBase
	data *streamWriter
}

func (c *floatWriter) write(batch *ColumnVector) (rows uint64, err error) {
	var vector []float32
	values := batch.Vector.([]float32)
	rows = uint64(len(values))

	if c.schema.HasNulls {
		if batch.Presents == nil || len(batch.Presents) == 0 {
			return 0, errors.New("no presents data")
		}
		if len(batch.Presents) != len(values) {
			return 0, errors.New("rows of presents != vector")
		}

		log.Tracef("writing: float column write %d presents", len(batch.Presents))
		if err = c.present.writeValues(batch.Presents); err != nil {
			return
		}

		for i, p := range batch.Presents {
			if p {
				vector = append(vector, values[i])
			}
		}

	} else {
		vector = values
	}

	if err = c.data.writeValues(vector); err != nil {
		return
	}

	return
}

func (c floatWriter) getStreams() []*streamWriter {
	ss := make([]*streamWriter, 2)
	ss[0] = c.present
	ss[1] = c.data
	return ss
}

func newFloatWriter(schema *TypeDescription, opts *WriterOptions) *floatWriter {
	c := &cwBase{schema: schema, present: newBoolStreamWriter(schema.Id, pb.Stream_PRESENT, opts)}
	data := newFloatStream(schema.Id, pb.Stream_DATA, opts)
	return &floatWriter{cwBase: c, data: data}
}

type streamWriter struct {
	info *pb.Stream

	buf *bytes.Buffer

	opts *WriterOptions

	encodingBuf *bytes.Buffer
	encoder     encoding.Encoder
}

// write and compress data to stream buffer in 1 or more chunk if compressed
func compress(kind pb.CompressionKind, chunkSize int, dst *bytes.Buffer, src *bytes.Buffer) error {
	switch kind {
	case pb.CompressionKind_NONE:
		n, err := src.WriteTo(dst)
		log.Tracef("no compression write %d", n)
		if err != nil {
			return err
		}
	case pb.CompressionKind_ZLIB:
		if err := zlibCompressing(chunkSize, dst, src); err != nil {
			return err
		}

	default:
		return errors.New("compression kind error")
	}
	return nil
}

func (s *streamWriter) writeValues(values interface{}) (err error) {
	s.encodingBuf.Reset()
	if err = s.encoder.Encode(s.encodingBuf, values); err != nil {
		return
	}

	if err = compress(s.opts.CompressionKind, s.opts.ChunkSize, s.buf, s.encodingBuf); err != nil {
		return
	}

	*s.info.Length = uint64(s.buf.Len())

	// s.info.Get.. and s.info.Kind.St... invoked even with trace level
	log.Debugf("stream id %d - %s wrote", s.info.GetColumn(), s.info.Kind.String())
	return
}

func (s *streamWriter) writeOut(out io.Writer) (n int64, err error) {
	return s.buf.WriteTo(out)
}

func (s *streamWriter) reset() {
	*s.info.Length = 0

	s.buf.Reset()
	s.encodingBuf.Reset()
}

func (w *writer) GetSchema() *TypeDescription {
	return w.schemas[0]
}

func (w *writer) writeHeader() (uint64, error) {
	b := []byte(MAGIC)
	if _, err := w.out.Write(b); err != nil {
		return 0, errors.WithStack(err)
	}
	return uint64(len(b)), nil
}

var HEADER_LENGTH = uint64(3)
var MAGIC = "ORC"

func (w *writer) writeFileTail() error {
	// Encode footer
	// todo: rowsinstrde
	ft := &pb.Footer{HeaderLength: &HEADER_LENGTH, ContentLength: new(uint64), NumberOfRows: new(uint64)}

	for _, si := range w.stripeInfos {
		*ft.ContentLength += si.GetIndexLength() + si.GetDataLength() + si.GetFooterLength()
		*ft.NumberOfRows += si.GetNumberOfRows()
	}
	ft.Stripes = w.stripeInfos
	ft.Types = schemasToTypes(w.schemas)

	// metadata

	// statistics

	ftb, err := proto.Marshal(ft)
	if err != nil {
		return errors.WithStack(err)
	}
	ftCmpBuf, err := compressByteSlice(w.opts.CompressionKind, w.opts.ChunkSize, ftb)
	if err != nil {
		return err
	}
	ftl := uint64(len(ftCmpBuf))

	if _, err := w.out.Write(ftCmpBuf); err != nil {
		return errors.WithStack(err)
	}
	log.Infof("write out file tail %s (length: %d)", ft.String(), ftl)

	// postscript
	ps := &pb.PostScript{}
	ps.FooterLength = &ftl
	ps.Compression = &w.opts.CompressionKind
	c := uint64(w.opts.ChunkSize)
	ps.CompressionBlockSize = &c
	ps.Version = VERSION
	ps.Magic = &MAGIC
	psb, err := proto.Marshal(ps)
	if err != nil {
		return errors.WithStack(err)
	}
	var n int
	n, err = w.out.Write(psb)
	if err != nil {
		return errors.WithStack(err)
	}
	log.Infof("write out postscript (length %d)", n)
	// last byte is ps length
	if _, err = w.out.Write([]byte{byte(n)}); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (w *writer) close() error {
	if w.stripe.size() != 0 {
		if err := w.flushStripe(); err != nil {
			return err
		}
	}

	if err := w.writeFileTail(); err != nil {
		return err
	}

	w.out.Close()

	return nil
}

// zlib compress src valueBuf into dst, maybe to several chunks
// if whole src compressed then split to chunks, then cannot skip chunk decompress,
// so it should be compressing each chunk
func zlibCompressing(chunkSize int, dst *bytes.Buffer, src *bytes.Buffer) error {
	var start int
	remaining := src.Len()
	srcBytes := src.Bytes()

	cBuf := bytes.NewBuffer(make([]byte, chunkSize))
	cBuf.Reset()
	compressor, err := flate.NewWriter(cBuf, -1)
	if err != nil {
		return errors.WithStack(err)
	}

	log.Tracef("start zlib compressing, chunksize %d remaining %d", chunkSize, remaining)

	for remaining > chunkSize {

		if _, err = compressor.Write(srcBytes[start : start+chunkSize]); err != nil {
			return errors.WithStack(err)
		}
		if err = compressor.Close(); err != nil {
			return errors.WithStack(err)
		}

		if cBuf.Len() > chunkSize { // original
			header := encChunkHeader(chunkSize, true)
			if _, err = dst.Write(header); err != nil {
				return errors.WithStack(err)
			}

			log.Tracef("compressing original, write out %d, remaining %d", chunkSize, remaining-chunkSize)
			if _, err = dst.Write(srcBytes[start : start+chunkSize]); err != nil {
				return errors.WithStack(err)
			}

		} else {
			header := encChunkHeader(cBuf.Len(), false)
			if _, err = dst.Write(header); err != nil {
				return errors.WithStack(err)
			}

			log.Tracef("compressing zlib, write out after compressing %d, remaining %d", cBuf.Len(), remaining-chunkSize)
			if _, err = cBuf.WriteTo(dst); err != nil {
				return errors.WithStack(err)
			}
		}

		start += chunkSize
		remaining -= chunkSize
		compressor.Reset(cBuf)
	}

	if remaining > 0 {
		if _, err := compressor.Write(srcBytes[start : start+remaining]); err != nil {
			return errors.WithStack(err)
		}
		if err := compressor.Close(); err != nil {
			return errors.WithStack(err)
		}

		if cBuf.Len() > remaining {
			header := encChunkHeader(remaining, true)
			if _, err = dst.Write(header); err != nil {
				return errors.WithStack(err)
			}
			log.Tracef("compressing original, last write %d", remaining)
			if _, err = dst.Write(srcBytes[start : start+remaining]); err != nil {
				return errors.WithStack(err)
			}
		} else {
			header := encChunkHeader(cBuf.Len(), false)
			if _, err = dst.Write(header); err != nil {
				return errors.WithStack(err)
			}
			log.Tracef("compressing zlib,  last writing %d", cBuf.Len())
			if _, err = cBuf.WriteTo(dst); err != nil {
				return errors.WithStack(err)
			}
		}
	}

	return nil
}

func encChunkHeader(l int, orig bool) (header []byte) {
	header = make([]byte, 3)
	if orig {
		header[0] = 0x01 | byte(l<<1)
	} else {
		header[0] = byte(l << 1)
	}
	header[1] = byte(l >> 7)
	header[2] = byte(l >> 15)
	return
}

func decChunkHeader(h []byte) (length int, orig bool) {
	_ = h[2]
	return int(h[2])<<15 | int(h[1])<<7 | int(h[0])>>1, h[0]&0x01 == 0x01
}

// used in currentStripe footer, tail footer
func compressByteSlice(kind pb.CompressionKind, chunkSize int, b []byte) (compressed []byte, err error) {
	switch kind {
	case pb.CompressionKind_NONE:
		compressed = b
	case pb.CompressionKind_ZLIB:
		src := bytes.NewBuffer(b)
		dst := bytes.NewBuffer(make([]byte, len(b)))
		dst.Reset()
		if err = zlibCompressing(chunkSize, dst, src); err != nil {
			return nil, err
		}
		return dst.Bytes(), nil

	default:
		return nil, errors.New("compression not impl")
	}
	return
}

func newBoolStreamWriter(id uint32, kind pb.Stream_Kind, opts *WriterOptions) *streamWriter {
	kind_ := kind
	id_ := id
	length_ := uint64(0)
	info := &pb.Stream{Kind: &kind_, Column: &id_, Length: &length_}
	encoder := &encoding.BoolRunLength{}
	return &streamWriter{info: info, buf: &bytes.Buffer{}, opts: opts, encoder: encoder, encodingBuf: &bytes.Buffer{}}
}

func newByteStreamWriter(id uint32, kind pb.Stream_Kind, opts *WriterOptions) *streamWriter {
	id_ := id
	kind_ := kind
	length_ := uint64(0)
	info := &pb.Stream{Kind: &kind_, Column: &id_, Length: &length_}
	encoder := &encoding.ByteRunLength{}
	return &streamWriter{info: info, buf: &bytes.Buffer{}, opts: opts, encoder: encoder, encodingBuf: &bytes.Buffer{}}
}

func newIntV2Stream(id uint32, kind pb.Stream_Kind, signed bool, opts *WriterOptions) *streamWriter {
	id_ := id
	kind_ := kind
	length_ := uint64(0)
	info := &pb.Stream{Kind: &kind_, Column: &id_, Length: &length_}
	encoder := &encoding.IntRleV2{Signed: signed}
	return &streamWriter{info: info, buf: &bytes.Buffer{}, opts: opts, encoder: encoder, encodingBuf: &bytes.Buffer{}}
}

func newStringContentsStream(id uint32, kind pb.Stream_Kind, opts *WriterOptions) *streamWriter {
	id_ := id
	kind_ := kind
	length_ := uint64(0)
	info := &pb.Stream{Kind: &kind_, Column: &id_, Length: &length_}
	encoder := &encoding.BytesContent{}
	return &streamWriter{info: info, buf: &bytes.Buffer{}, opts: opts, encoder: encoder, encodingBuf: &bytes.Buffer{}}
}

func newBinaryV2Stream(id uint32, kind pb.Stream_Kind, opts *WriterOptions) *streamWriter {
	id_ := id
	kind_ := kind
	length_ := uint64(0)
	info := &pb.Stream{Kind: &kind_, Column: &id_, Length: &length_}
	encoder := &encoding.BytesContent{}
	return &streamWriter{info: info, buf: &bytes.Buffer{}, opts: opts, encoder: encoder, encodingBuf: &bytes.Buffer{}}
}

func newBase128VarIntStreamWriter(id uint32, kind pb.Stream_Kind, opts *WriterOptions) *streamWriter {
	id_ := id
	kind_ := pb.Stream_DATA
	length_ := uint64(0)
	encoder := &encoding.Base128VarInt{}
	info_ := &pb.Stream{Kind: &kind_, Column: &id_, Length: &length_}
	return &streamWriter{info: info_, buf: &bytes.Buffer{}, opts: opts, encoder: encoder, encodingBuf: &bytes.Buffer{}}
}

func newFloatStream(id uint32, kind pb.Stream_Kind, opts *WriterOptions) *streamWriter {
	id_ := id
	kind_ := kind
	length_ := uint64(0)
	info := &pb.Stream{Kind: &kind_, Column: &id_, Length: &length_}
	encoder := &encoding.Ieee754Float{}
	return &streamWriter{info: info, buf: &bytes.Buffer{}, opts: opts, encoder: encoder, encodingBuf: &bytes.Buffer{}}
}

func newDoubleStream(id uint32, kind pb.Stream_Kind, opts *WriterOptions) *streamWriter {
	id_ := id
	kind_ := kind
	length_ := uint64(0)
	info := &pb.Stream{Kind: &kind_, Column: &id_, Length: &length_}
	encoder := &encoding.Ieee754Double{}
	return &streamWriter{info: info, buf: &bytes.Buffer{}, opts: opts, encoder: encoder, encodingBuf: &bytes.Buffer{}}
}

func getColumnEncoding(opts *WriterOptions, kind pb.Type_Kind) pb.ColumnEncoding_Kind {
	switch kind {
	case pb.Type_SHORT:
		fallthrough
	case pb.Type_INT:
		fallthrough
	case pb.Type_LONG:
		return pb.ColumnEncoding_DIRECT_V2
	case pb.Type_FLOAT:
		fallthrough
	case pb.Type_DOUBLE:
		return pb.ColumnEncoding_DIRECT
	case pb.Type_STRING:
		// todo:
		return pb.ColumnEncoding_DIRECT_V2
	case pb.Type_BOOLEAN:
		return pb.ColumnEncoding_DIRECT
	case pb.Type_BYTE:
		return pb.ColumnEncoding_DIRECT
	case pb.Type_BINARY:
		return pb.ColumnEncoding_DIRECT_V2
	case pb.Type_DECIMAL:
		return pb.ColumnEncoding_DIRECT_V2
	case pb.Type_DATE:
		return pb.ColumnEncoding_DIRECT_V2
	case pb.Type_TIMESTAMP:
		return pb.ColumnEncoding_DIRECT_V2
	case pb.Type_STRUCT:
		return pb.ColumnEncoding_DIRECT
	case pb.Type_LIST:
		return pb.ColumnEncoding_DIRECT_V2
	case pb.Type_MAP:
		return pb.ColumnEncoding_DIRECT_V2
	case pb.Type_UNION:
		return pb.ColumnEncoding_DIRECT

	default:
		panic("column type unknown")
	}
}
