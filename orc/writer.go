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
	StripeSize      uint64
	FlushLater      bool //if true means every write, will write in memory,
	// then flush out at some threshold,
	//but if flush fail, previous write into memory would be lost
}

func DefaultWriterOptions() *WriterOptions {
	o := &WriterOptions{}
	o.CompressionKind = pb.CompressionKind_ZLIB
	o.StripeSize = DEFAULT_STRIPE_SIZE
	o.ChunkSize = DEFAULT_CHUNK_SIZE
	return o
}

type Writer interface {
	GetSchema() *TypeDescription

	Write(batch *ColumnVector) error

	Close() error
}

func NewFileWriter(path string, schema *TypeDescription, opts *WriterOptions) (writer Writer, err error) {
	// fixme: create new one, error when exist
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

type fileWriter struct {
	path string
	f    *os.File

	*writer
}

func (w *fileWriter) Close() error {
	return w.close(w.f)
}

func newWriter(schema *TypeDescription, opts *WriterOptions, out io.Writer) (*writer, error) {
	schemas := schema.normalize()
	for _, s := range schemas {
		s.Encoding = getColumnEncoding(opts, s.Kind)
	}

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

	out io.Writer
}

func (w *writer) Write(batch *ColumnVector) error {
	if err := w.stripe.writeColumn(batch); err != nil {
		return err
	}

	force := !w.opts.FlushLater
	if err := w.flushStripe(force); err != nil {
		return err
	}
	return nil
}

// refactoring: whole stripe buffered in memory and flush out?
func (w *writer) flushStripe(force bool) error {
	if force || w.stripe.shouldFlush() {
		if err := w.stripe.flush(w.out); err != nil {
			return errors.WithStack(err)
		}

		// todo: update column stats

		w.offset += w.stripe.info.GetOffset() + w.stripe.info.GetIndexLength() + w.stripe.info.GetDataLength()
		log.Debugf("flushed currentStripe %v", w.stripe.info)
		w.stripeInfos = append(w.stripeInfos, w.stripe.info)

		w.stripe.reset()
		*w.stripe.info.Offset = w.offset
	}
	return nil
}

func newStripe(offset uint64, schemas []*TypeDescription, opts *WriterOptions) (stripe *stripeWriter, err error) {
	idxBuf := bytes.NewBuffer(make([]byte, DEFAULT_INDEX_SIZE))
	idxBuf.Reset()

	// prepare streams
	var columns []columnWriter
	for _, schema := range schemas {
		var column columnWriter

		switch schema.Kind {
		case pb.Type_SHORT:
			fallthrough
		case pb.Type_INT:
			fallthrough
		case pb.Type_LONG:
			column = newLongV2Writer(schema, opts)

		case pb.Type_FLOAT:
			fallthrough
		case pb.Type_DOUBLE:
		// todo:

		case pb.Type_CHAR:
			fallthrough
		case pb.Type_VARCHAR:
			fallthrough
		case pb.Type_STRING:
			if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
				column = newStringV2Writer(schema, opts)
				break
			}

			if schema.Encoding == pb.ColumnEncoding_DICTIONARY_V2 {
				column = newStringDictV2Writer(schema, opts)
				break
			}

			return nil, errors.New("encoding not impl")

		case pb.Type_BOOLEAN:
			if schema.Encoding != pb.ColumnEncoding_DIRECT {
				return nil, errors.New("encoding error")
			}
			column = newBoolWriter(schema, opts)

		case pb.Type_BYTE:
			if schema.Encoding != pb.ColumnEncoding_DIRECT {
				return nil, errors.New("encoding error")
			}
			column = newByteWriter(schema, opts)

		case pb.Type_BINARY:
			if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
				column = newBinaryDirectV2Writer(schema, opts)
				break
			}

			return nil, errors.New("encoding not impl")

		case pb.Type_DECIMAL:
			if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
				column = newDecimal64DirectV2Writer(schema, opts)
				break
			}
			return nil, errors.New("encoding not impl")

		case pb.Type_DATE:
			if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
				column = newDateDirectV2Writer(schema, opts)
				break
			}
			return nil, errors.New("encoding not impl")

		case pb.Type_TIMESTAMP:
			if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
				column = newTimestampDirectV2Writer(schema, opts)
				break
			}
			return nil, errors.New("encoding not impl")

		case pb.Type_STRUCT:
			if schema.Encoding != pb.ColumnEncoding_DIRECT {
				return nil, errors.New("encoding error")
			}
			column = newStructWriter(schema, opts)

		case pb.Type_LIST:
			if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
				// todo:
				//streams[schema.Id][1] = newUnsignedIntV2Writer(schema.Id, pb.Stream_LENGTH, opts)
				break
			}
			return nil, errors.New("encoding not impl")

		case pb.Type_MAP:
			if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
				// todo:
				// streams[schema.Id][1] = newUnsignedIntV2Writer(schema.Id, pb.Stream_LENGTH, opts)
				break
			}
			return nil, errors.New("encoding not impl")

		case pb.Type_UNION:
			if schema.Encoding != pb.ColumnEncoding_DIRECT {
				return nil, errors.New("encoding error")
			}
			// todo:
			//streams[schema.Id][1]= newByteStream(schema.Id, pb.Stream_DIRECT)
		}

		columns = append(columns, column)
	}

	o := offset
	info := &pb.StripeInformation{Offset: &o}
	s := &stripeWriter{opts: opts, idxBuf: idxBuf, columnWriters: columns, info: info, schemas: schemas}
	return s, nil
}

type stripeWriter struct {
	opts    *WriterOptions
	schemas []*TypeDescription

	columnWriters []columnWriter

	idxBuf *bytes.Buffer // index area buffer

	info *pb.StripeInformation
}

func (s *stripeWriter) writeColumn(batch *ColumnVector) error {

	writer := s.columnWriters[batch.Id]
	rows, err := writer.write(batch)
	if err != nil {
		return err
	}

	// todo: update stripe datalength

	if !s.schemas[batch.Id].HasFather {
		*s.info.NumberOfRows += rows
	}
	return nil
}

func (s *stripeWriter) shouldFlush() bool {
	return s.info.GetIndexLength()+s.info.GetDataLength() >= s.opts.StripeSize
}

// stripe should be self-contained
// enhance: if buffer is empty or just flushed, no need write a new stripe
func (s *stripeWriter) flush(out io.Writer) error {
	var stripeFooter pb.StripeFooter

	// row number updated at write
	*s.info.IndexLength = uint64(s.idxBuf.Len())
	_, err := s.idxBuf.WriteTo(out)
	if err != nil {
		return errors.WithStack(err)
	}
	log.Debugf("flush index with length %d", s.info.GetIndexLength())

	for _, column := range s.columnWriters {
		for _, stream := range column.getStreams() {
			log.Tracef("flush stream %stream of column %d length %d", stream.info.GetKind().String(),
				stream.info.GetColumn(), stream.info.GetLength())
			if _, err := stream.flush(out); err != nil {
				return err
			}
			// todo: update stripe datalength here?

			stripeFooter.Streams = append(stripeFooter.Streams, stream.info)
		}
	}

	for _, schema := range s.schemas {
		stripeFooter.Columns = append(stripeFooter.Columns, &pb.ColumnEncoding{Kind: &schema.Encoding})
	}

	// write footer
	footerBuf, err := proto.Marshal(&stripeFooter)
	if err != nil {
		return errors.WithStack(err)
	}
	compressedFooterBuf, err := compressByteSlice(s.opts.CompressionKind, s.opts.ChunkSize, footerBuf)
	if err != nil {
		return err
	}
	*s.info.FooterLength = uint64(len(compressedFooterBuf))
	if _, err := out.Write(compressedFooterBuf); err != nil {
		return errors.WithStack(err)
	}
	log.Debugf("stripe footer wrote with length: %d", s.info.GetFooterLength())

	return nil
}

func (s *stripeWriter) reset() {
	s.info.Reset()
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
	schema *TypeDescription

	//opts *WriterOptions

	present *streamWriter
}

type structWriter struct {
	*cwBase
}

func newStructWriter(schema *TypeDescription, opts *WriterOptions) *structWriter  {
	cw_ := &cwBase{schema: schema, present: newBoolStream(schema.Id, pb.Stream_PRESENT, opts)}
	return &structWriter{cw_}
}

func (c *structWriter) write(batch *ColumnVector) (rows uint64, err error) {
	children := batch.Vector.([]*ColumnVector)
	for _, child := range children {
		// rethink: rows
		if rows, err = c.write(child); err != nil {
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

func newTimestampDirectV2Writer(schema *TypeDescription, opts *WriterOptions) *timestampDirectV2Writer  {
	cw_ := &cwBase{schema: schema, present: newBoolStream(schema.Id, pb.Stream_PRESENT, opts)}
	data_ := newIntV2Stream(schema.Id, pb.Stream_DATA, true, opts)
	secondary_ := newIntV2Stream(schema.Id, pb.Stream_SECONDARY, false, opts)
	return &timestampDirectV2Writer{cw_, data_, secondary_}
}

func (c *timestampDirectV2Writer) write(batch *ColumnVector) (rows uint64, err error) {
	var seconds []int64
	var nanos []uint64
	values := batch.Vector.([]Timestamp)
	rows = uint64(len(values))

	if c.schema.HasNulls {
		//
		if len(batch.Presents) != len(values) {
			return 0, errors.New("rows of present != vector")
		}
		for i, p := range batch.Presents {
			if p {
				seconds = append(seconds, values[i].Seconds)
				nanos = append(nanos, uint64(values[i].Nanos))
			}
		}
	} else {
		for _, v := range values {
			seconds = append(seconds, v.Seconds)
			nanos = append(nanos, uint64(v.Nanos))
		}
	}

	if _, err := c.data.writeValues(seconds); err != nil {
		return 0, err
	}
	if _, err = c.secondary.writeValues(nanos); err != nil {
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

type dateDirectV2Writer struct {
	*cwBase
	data *streamWriter
}

func newDateDirectV2Writer(schema *TypeDescription, opts *WriterOptions) *dateDirectV2Writer {
	cw_ := &cwBase{schema: schema, present: newBoolStream(schema.Id, pb.Stream_PRESENT, opts)}
	data_ := newIntV2Stream(schema.Id, pb.Stream_DATA, false, opts)
	return &dateDirectV2Writer{cw_, data_}
}

func (c *dateDirectV2Writer) write(batch *ColumnVector) (rows uint64, err error) {
	var vector []int64
	values := batch.Vector.([]Date)
	rows = uint64(len(values))

	if c.schema.HasNulls {
		//todo: batch.presents nil warning

		if len(batch.Presents) != len(values) {
			return 0, errors.New("rows of present != vector")
		}

		if _, err := c.present.writeValues(batch.Presents); err != nil {
			return 0, err
		}

		for i, p := range batch.Presents {
			if p {
				vector = append(vector, toDays(values[i]))
			}
		}
	} else {
		for _, v := range values {
			vector = append(vector, toDays(v))
		}
	}

	if _, err := c.data.writeValues(vector); err != nil {
		return 0, err
	}
	return
}

func (c *dateDirectV2Writer) getStreams() []*streamWriter {
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

func newDecimal64DirectV2Writer(schema *TypeDescription, opts *WriterOptions) *decimal64DirectV2Writer  {
	cw_ := &cwBase{schema: schema, present: newBoolStream(schema.Id, pb.Stream_PRESENT, opts)}
	data_ := newBase128VarIntStream(schema.Id, pb.Stream_DATA, opts)
	secondary_ := newIntV2Stream(schema.Id, pb.Stream_SECONDARY, false, opts)
	return &decimal64DirectV2Writer{cw_, data_, secondary_}
}

func (c *decimal64DirectV2Writer) write(batch *ColumnVector) (rows uint64, err error) {
	var precisions []int64
	var scales []uint64
	values := batch.Vector.([]Decimal64)
	rows = uint64(len(values))

	if c.schema.HasNulls {
		//todo: batch.presents nil warning

		if len(batch.Presents) != len(values) {
			return 0, errors.New("rows of present != vector")
		}

		if _, err := c.present.writeValues(batch.Presents); err != nil {
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

	if _, err := c.data.writeValues(precisions); err != nil {
		return 0, err
	}

	if _, err = c.secondary.writeValues(scales); err != nil {
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

func (c *doubleWriter) write(batch *ColumnVector) (rows uint64, err error) {
	var vector []float64
	values := batch.Vector.([]float64)
	rows = uint64(len(values))

	if c.schema.HasNulls {
		//todo: batch.presents nil warning

		if len(batch.Presents) != len(values) {
			return 0, errors.New("rows of present != vector")
		}

		if _, err := c.present.writeValues(batch.Presents); err != nil {
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

	if _, err := c.data.writeValues(vector); err != nil {
		return 0, err
	}

	return
}

type binaryDirectV2Writer struct {
	*cwBase
	data   *streamWriter
	length *streamWriter
}

func newBinaryDirectV2Writer(schema *TypeDescription, opts *WriterOptions) *binaryDirectV2Writer  {
	cw_ := &cwBase{schema: schema, present: newBoolStream(schema.Id, pb.Stream_PRESENT, opts)}
	data_ := newStringV2Stream(schema.Id, pb.Stream_DATA, opts)
	length_ := newIntV2Stream(schema.Id, pb.Stream_LENGTH, false, opts)
	return &binaryDirectV2Writer{cw_, data_, length_}
}

func (c *binaryDirectV2Writer) write(batch *ColumnVector) (rows uint64, err error) {
	var vector [][]byte
	var lengthVector []uint64
	values := batch.Vector.([][]byte)
	rows = uint64(len(values))

	if c.schema.HasNulls {
		//todo: batch.presents nil warning

		if len(batch.Presents) != len(values) {
			return 0, errors.New("rows of present != vector")
		}

		if _, err := c.present.writeValues(batch.Presents); err != nil {
			return 0, err
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

	if _, err := c.data.writeValues(vector); err != nil {
		return 0, err
	}

	if _, err = c.length.writeValues(lengthVector); err != nil {
		return 0, err
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

type stringDictV2Writer struct {
	*cwBase
	data      *streamWriter
	secondary *streamWriter
	length    *streamWriter
}

func newStringDictV2Writer(schema *TypeDescription, opts *WriterOptions) *stringDictV2Writer {
	cw_ := &cwBase{schema: schema, present: newBoolStream(schema.Id, pb.Stream_PRESENT, opts)}
	data_ := newIntV2Stream(schema.Id, pb.Stream_DATA, false, opts)
	secondary_ := newStringV2Stream(schema.Id, pb.Stream_DICTIONARY_DATA, opts)
	length_ := newIntV2Stream(schema.Id, pb.Stream_LENGTH, false, opts)
	return &stringDictV2Writer{cwBase: cw_, data: data_, secondary: secondary_, length: length_}
}

func (c *stringDictV2Writer) write(batch *ColumnVector) (rows uint64, err error) {
	// todo:

	/*values := batch.Vector.([]string)
	rows = uint64(len(values))

	var data []uint64
	var dictData []byte
	var lengthData []uint64*/

	return
}

func (c *stringDictV2Writer) getStreams() []*streamWriter {
	ss := make([]*streamWriter, 4)
	ss[0] = c.present
	ss[1] = c.data
	ss[2] = c.secondary
	ss[3] = c.length
	return ss
}

type stringV2Writer struct {
	*cwBase
	data   *streamWriter
	length *streamWriter
}

func newStringV2Writer(schema *TypeDescription, opts *WriterOptions) *stringV2Writer {
	cw_ := &cwBase{schema: schema, present: newBoolStream(schema.Id, pb.Stream_PRESENT, opts)}
	data_ := newStringV2Stream(schema.Id, pb.Stream_DATA, opts)
	length_ := newIntV2Stream(schema.Id, pb.Stream_LENGTH, false, opts)
	return &stringV2Writer{cw_, data_, length_}
}

func (c *stringV2Writer) write(batch *ColumnVector) (rows uint64, err error) {
	var lengthVector []uint64
	var contents [][]byte
	values := batch.Vector.([]string)
	rows = uint64(len(values))

	if c.schema.HasNulls {
		// todo: presents nil warning
		if len(batch.Presents) != len(values) {
			return 0, errors.New("rows of present != vector")
		}

		if _, err := c.present.writeValues(batch.Presents); err != nil {
			return 0, err
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

	if _, err := c.data.writeValues(contents); err != nil {
		return 0, err
	}

	// rethink: if data write sucsessful and length write fail, because right now data is in memory
	if _, err = c.length.writeValues(lengthVector); err != nil {
		return 0, err
	}

	return
}

func (c *stringV2Writer) getStreams() []*streamWriter {
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
	cw_ := &cwBase{schema: schema, present: newBoolStream(schema.Id, pb.Stream_PRESENT, opts)}
	data_ := newBoolStream(schema.Id, pb.Stream_DATA, opts)
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

		if _, err := c.present.writeValues(batch.Presents); err != nil {
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

	if _, err := c.data.writeValues(vector); err != nil {
		return 0, err
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

func newByteWriter(schema *TypeDescription, opts *WriterOptions) *byteWriter  {
	cw_ := &cwBase{schema: schema, present: newBoolStream(schema.Id, pb.Stream_PRESENT, opts)}
	data_ := newByteStream(schema.Id, pb.Stream_DATA, opts)
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

		if _, err := c.present.writeValues(batch.Presents); err != nil {
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

	if _, err := c.data.writeValues(vector); err != nil {
		return 0, err
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
	c := &cwBase{schema: schema, present: newBoolStream(schema.Id, pb.Stream_PRESENT, opts)}
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

		if _, err := c.present.writeValues(batch.Presents); err != nil {
			return 0, err
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

	if _, err := c.data.writeValues(vector); err != nil {
		return 0, err
	}

	return
}

func (c longV2Writer) getStreams() []*streamWriter {
	ss := make([]*streamWriter, 2)
	ss[0] = c.present
	ss[1] = c.data
	return ss
}

type streamWriter struct {
	info *pb.Stream

	buf *bytes.Buffer

	opts *WriterOptions

	encodingBuf *bytes.Buffer
	encoder     encoding.Encoder
}

func (s *streamWriter) write(data *bytes.Buffer) (written uint64, err error) {
	start := s.buf.Len()

	switch s.opts.CompressionKind {
	case pb.CompressionKind_NONE:
		if _, err = data.WriteTo(s.buf); err != nil {
			return 0, err
		}
	case pb.CompressionKind_ZLIB:
		if err = zlibCompressing(s.opts.ChunkSize, s.buf, data); err != nil {
			return 0, err
		}

	default:
		return 0, errors.New("compression kind error")
	}
	end := s.buf.Len()

	l := uint64(end - start)
	*s.info.Length += l
	return l, nil
}

func (s *streamWriter) writeValues(values interface{}) (written uint64, err error) {
	log.Tracef("stream %d %s writing values", s.info.GetColumn(), s.info.Kind.String())
	if err = s.encoder.Encode(s.encodingBuf, values); err != nil {
		return 0, err
	}

	return s.write(s.encodingBuf)
}

func (s *streamWriter) flush(w io.Writer) (n int64, err error) {
	n, err = s.buf.WriteTo(w)
	if err != nil {
		return n, errors.WithStack(err)
	}
	return
}

func (s *streamWriter) reset() {
	s.info.Reset()
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

func (w *writer) writeFileTail(out io.Writer) error {
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

	if _, err := out.Write(ftCmpBuf); err != nil {
		return errors.WithStack(err)
	}
	log.Debugf("Encode file footer with length: %d", ftl)

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
	n, err := out.Write(psb)
	if err != nil {
		return errors.Wrap(err, "write PS error")
	}
	log.Debugf("Encode postscript with length %d", n)
	// last byte is ps length
	if _, err = out.Write([]byte{byte(n)}); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (w *writer) close(out io.WriteCloser) error {
	if err := w.flushStripe(true); err != nil {
		return err
	}
	if err := w.writeFileTail(out); err != nil {
		return err
	}
	out.Close()
	return nil
}

// zlib compress src valueBuf into dst, maybe to several chunks
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

	for remaining > chunkSize {

		if _, err = compressor.Write(srcBytes[start : start+chunkSize]); err != nil {
			return errors.WithStack(err)
		}
		if err = compressor.Close(); err != nil {
			return errors.WithStack(err)
		}

		if cBuf.Len() > chunkSize {
			header := encChunkHeader(chunkSize, true)
			if _, err = dst.Write(header); err != nil {
				return errors.WithStack(err)
			}
			log.Tracef("write original chunksize %d remaining %d before writing", chunkSize, remaining)
			if _, err = dst.Write(srcBytes[start : start+chunkSize]); err != nil {
				return errors.WithStack(err)
			}
			cBuf.Reset()

		} else {
			header := encChunkHeader(cBuf.Len(), false)
			if _, err = dst.Write(header); err != nil {
				return errors.WithStack(err)
			}
			log.Tracef("zlib compressing chunkSize %d remaining %d before writing with compressed length %d",
				chunkSize, remaining, cBuf.Len())
			if _, err = cBuf.WriteTo(dst); err != nil {
				return errors.WithStack(err)
			}
		}

		start += chunkSize
		remaining -= chunkSize
		compressor.Reset(cBuf)
	}

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
		log.Tracef("write original remaining %d before writing", remaining)
		if _, err = dst.Write(srcBytes[start : start+remaining]); err != nil {
			return errors.WithStack(err)
		}
	} else {
		header := encChunkHeader(cBuf.Len(), false)
		if _, err = dst.Write(header); err != nil {
			return errors.WithStack(err)
		}
		log.Tracef("zlib compressing remaining %d before writing with chunklength %d", remaining, cBuf.Len())
		if _, err = cBuf.WriteTo(dst); err != nil {
			return errors.WithStack(err)
		}
	}

	src.Reset()
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

// compress byte slice into chunk slice, used in currentStripe footer, tail footer
// thinking should be smaller than chunksize
func compressByteSlice(kind pb.CompressionKind, chunkSize int, b []byte) (compressed []byte, err error) {
	switch kind {
	case pb.CompressionKind_ZLIB:
		src := bytes.NewBuffer(b)
		dst := bytes.NewBuffer(make([]byte, len(b)))
		dst.Reset()
		if err = zlibCompressing(chunkSize, dst, src); err != nil {
			return nil, err
		}
		return dst.Bytes(), nil

	default:
		return nil, errors.New("compression other than zlib not impl")
	}
	return
}

func newBoolStream(id uint32, kind pb.Stream_Kind, opts *WriterOptions) *streamWriter {
	kind_ := kind
	id_ := id
	length_ := uint64(0)
	info := &pb.Stream{Kind: &kind_, Column: &id_, Length: &length_}
	encoder := &encoding.BoolRunLength{}
	return &streamWriter{info: info, buf: &bytes.Buffer{}, opts: opts, encoder: encoder, encodingBuf: &bytes.Buffer{}}
}

func newByteStream(id uint32, kind pb.Stream_Kind, opts *WriterOptions) *streamWriter {
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

func newStringV2Stream(id uint32, kind pb.Stream_Kind, opts *WriterOptions) *streamWriter {
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

func newBase128VarIntStream(id uint32, kind pb.Stream_Kind, opts *WriterOptions) *streamWriter {
	id_ := id
	kind_ := pb.Stream_DATA
	length_ := uint64(0)
	encoder := &encoding.Base128VarInt{}
	info_ := &pb.Stream{Kind: &kind_, Column: &id_, Length: &length_}
	return &streamWriter{info: info_, buf: &bytes.Buffer{}, opts: opts, encoder: encoder, encodingBuf: &bytes.Buffer{}}
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
