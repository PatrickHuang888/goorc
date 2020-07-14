package orc

import (
	"bytes"
	"compress/flate"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
	"os"

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

	w := &writer{opts: opts, out: out}
	n, err := w.writeHeader()
	if err != nil {
		return nil, err
	}

	w.offset = n
	w.stripe, err = newStripeWriter(w.offset, schema, w.opts)
	if err != nil {
		return nil, err
	}
	return w, nil
}

// cannot used concurrently, not synchronized
// strip buffered in memory until the strip size
// Encode out by columns
type writer struct {
	//schemas []*TypeDescription
	opts *WriterOptions

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

	if w.stripe.size() >= int(w.opts.StripeSize) {
		if err := w.flushStripe(); err != nil {
			return err
		}
	}

	return nil
}

func (w *writer) flushStripe() error {
	var n int64
	var nf int
	var err error

	if n, err = w.stripe.flushOut(w.out); err != nil {
		return err
	}

	if nf, err = w.stripe.writeFooter(w.out); err != nil {
		return err
	}

	//next
	w.offset += uint64(n) + uint64(nf)
	w.stripeInfos = append(w.stripeInfos, w.stripe.info)
	w.stripe.reset()
	*w.stripe.info.Offset = w.offset

	return nil
}

func newStripeWriter(offset uint64, schema *TypeDescription, opts *WriterOptions) (stripe *stripeWriter, err error) {
	// normalize schema id from 0
	schemas := schema.normalize()

	idxBuf := bytes.NewBuffer(make([]byte, DEFAULT_INDEX_SIZE))
	idxBuf.Reset()

	// prepare streams
	var writers []columnWriter
	for _, schema := range schemas {
		var writer columnWriter
		if writer, err = createColumnWriter(schema, opts); err != nil {
			return nil, err
		}
		writers = append(writers, writer)
	}

	for _, schema := range schemas {
		switch schema.Kind {
		case pb.Type_STRUCT:
			for _, child := range schema.Children {
				w := writers[schema.Id].(*structWriter)
				w.children = append(w.children, writers[child.Id])
			}
		}
		// todo: case
	}

	info := &pb.StripeInformation{Offset: &offset, IndexLength: new(uint64), DataLength: new(uint64), FooterLength: new(uint64),
		NumberOfRows: new(uint64)}
	stripe = &stripeWriter{columnWriters: writers, info: info, schemas: schemas, chunkSize: opts.ChunkSize, compressionKind: opts.CompressionKind}
	return
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
	//opts    *WriterOptions
	schemas []*TypeDescription

	columnWriters []columnWriter

	//idxBuf *bytes.Buffer // index area buffer

	info *pb.StripeInformation // data in info not update every write,

	writeIndex bool

	//bufferSize      int
	compressionKind pb.CompressionKind
	chunkSize       int
}

// write to memory
func (stripe *stripeWriter) writeColumn(batch *ColumnVector) error {

	writer := stripe.columnWriters[batch.Id]
	rows, err := writer.write(batch.Presents, false, batch.Vector)
	if err != nil {
		return err
	}

	*stripe.info.NumberOfRows += uint64(rows)

	return nil
}

func (stripe stripeWriter) size() int {
	var n int
	for _, c := range stripe.columnWriters {
		n += c.size()
	}
	return n
}

/*// realSize have the real size after flush
func (stripe stripeWriter) realSize() uint64 {
	return stripe.info.GetIndexLength() + stripe.info.GetDataLength() + stripe.info.GetFooterLength()
}*/

func (stripe *stripeWriter) flushOut(out io.Writer) (n int64, err error) {

	for _, column := range stripe.columnWriters {
		if err = column.flush(); err != nil {
			return
		}
	}

	if stripe.writeIndex {
		for _, column := range stripe.columnWriters {
			index := column.getIndex()
			var indexData []byte
			if indexData, err = proto.Marshal(index); err != nil {
				return
			}

			var nd int
			if nd, err = out.Write(indexData); err != nil {
				return n, errors.WithStack(err)
			}
			n += int64(nd)

			*stripe.info.IndexLength += uint64(nd)
		}

		log.Tracef("flush index of length %d", stripe.info.GetIndexLength())
	}

	for _, column := range stripe.columnWriters {
		var ns int64
		if ns, err = column.writeOut(out); err != nil {
			return
		}
		n += ns
	}

	return
}

func (stripe *stripeWriter) writeFooter(out io.Writer) (n int, err error) {
	footer := &pb.StripeFooter{}

	for _, column := range stripe.columnWriters {
		// reassure: need make sure stream length != 0
		footer.Streams = append(footer.Streams, column.getStreamInfos()...)
	}

	for _, schema := range stripe.schemas {
		footer.Columns = append(footer.Columns, &pb.ColumnEncoding{Kind: &schema.Encoding})
	}

	var footerBuf []byte
	footerBuf, err = proto.Marshal(footer)
	if err != nil {
		return n, errors.WithStack(err)
	}

	var compressedFooterBuf []byte
	if compressedFooterBuf, err = compressByteSlice(stripe.compressionKind, stripe.chunkSize, footerBuf); err != nil {
		return
	}

	if n, err = out.Write(compressedFooterBuf); err != nil {
		return n, errors.WithStack(err)
	}

	*stripe.info.FooterLength = uint64(n)

	log.Infof("write out stripe footer %s", footer.String())
	return
}

func (stripe *stripeWriter) reset() {
	stripe.info = &pb.StripeInformation{Offset: new(uint64), IndexLength: new(uint64), DataLength: new(uint64),
		FooterLength: new(uint64), NumberOfRows: new(uint64)}

	//stripe.idxBuf.Reset()

	for _, column := range stripe.columnWriters {
		column.reset()
	}
}

type columnWriter interface {
	write(presents []bool, presentsFromParent bool, vec interface{}) (rows int, err error)

	// flush streams and update stats
	flush() error

	// flush first then write out, because maybe there is index to write first
	writeOut(out io.Writer) (n int64, err error)
	//after flush
	getIndex() *pb.RowIndex
	// after flush, used for writing stripe footer
	getStreamInfos() []*pb.Stream

	// for stripe reset
	reset()

	//sum of streams size, used for stripe flushing condition
	size() int
}

type treeWriter struct {
	schema *TypeDescription
	stats  *pb.ColumnStatistics

	present *EncodingStreamWriter

	children []columnWriter

	writeIndex  bool
	indexInRows int
	indexStride int
}

type structWriter struct {
	*treeWriter
}

func (c *structWriter) write(presents []bool, presentsFromParent bool, vec interface{}) (rows int, err error) {
	// todo: check batch, if has parent presents should no presents here

	if len(presents) != 0 && !presentsFromParent {
		for _, p := range presents {
			if err = c.present.Write(p); err != nil {
				return
			}
		}

		*c.stats.HasNull = true
	}

	childrenVector := vec.([]*ColumnVector)

	if len(c.children) != len(childrenVector) {
		return 0, errors.New("children vector not match children writer")
	}

	var r int
	var bc *batchInternal

	for i, child := range childrenVector {

		if len(presents) != 0 && !presentsFromParent {
			child.Presents = presents
			bc = &batchInternal{child, true}

			if r, err = c.children[i].write(presents, true, child); err != nil {
				return
			}

		} else {
			bc = &batchInternal{child, false}

			if r, err = c.children[i].write(presents, false, child); err != nil {
				return
			}
		}

		if r, err = c.children[i].write(bc); err != nil {
			return
		}
	}

	*c.stats.NumberOfValues += uint64(rows)

	if len(batch.Presents) == 0 {
		rows = r
	}

	return
}

func (c *structWriter) flush() error {
	panic("implement me")
}

func (c *structWriter) writeOut(out io.Writer) (n int64, err error) {
	panic("implement me")
}

func (c *structWriter) getIndex() *pb.RowIndex {
	panic("implement me")
}

func (c *structWriter) reset() {
	panic("implement me")
}

func (c *structWriter) size() int {
	panic("implement me")
}

func (c *structWriter) getStreamInfos() []*pb.Stream {
	return []*pb.Stream{c.present.info}
}

func newStructWriter(schema *TypeDescription, opts *WriterOptions) (writer *structWriter, err error) {
	//fixme: if only exist when there is presents?
	stats := &pb.ColumnStatistics{BytesOnDisk: new(uint64), HasNull: new(bool), NumberOfValues: new(uint64)}

	/*var childrenWriters []columnWriter
	for _, childSchema := range schema.Children {
		var childWriter columnWriter
		childWriter, err = createColumnWriter(childSchema, opts)
		if err != nil {
			return
		}
		childrenWriters = append(childrenWriters, childWriter)
	}*/

	writer = &structWriter{&treeWriter{schema: schema, present: newBoolStreamWriter(schema.Id, pb.Stream_PRESENT, opts),
		stats: stats}}
	return
}

type timestampDirectV2Writer struct {
	*treeWriter
	data      *EncodingStreamWriter
	secondary *EncodingStreamWriter
}

func newTimestampDirectV2Writer(schema *TypeDescription, opts *WriterOptions) *timestampDirectV2Writer {
	ts := &pb.TimestampStatistics{Maximum: new(int64), Minimum: new(int64), MinimumUtc: new(int64), MaximumUtc: new(int64)}
	stats := &pb.ColumnStatistics{BytesOnDisk: new(uint64), HasNull: new(bool), NumberOfValues: new(uint64), TimestampStatistics: ts}
	base := &treeWriter{schema: schema, present: newBoolStreamWriter(schema.Id, pb.Stream_PRESENT, opts), stats: stats}
	data := newIntV2Stream(schema.Id, pb.Stream_DATA, true, opts)
	secondary := newIntV2Stream(schema.Id, pb.Stream_SECONDARY, false, opts)
	return &timestampDirectV2Writer{base, data, secondary}
}

func (c *timestampDirectV2Writer) write(presents []bool, presentsFromParent bool, vec interface{}) (rows int, err error) {
	var seconds []uint64
	var nanos []uint64
	var values []Timestamp
	vector := vec.([]Timestamp)

	rows = len(vector)

	var pn, sn, nn int

	if len(presents) != 0 {

		if len(presents) != len(vector) {
			return 0, errors.New("rows of present != vector")
		}

		if !presentsFromParent {
			if pn, err = c.present.writeValues(batch.Presents); err != nil {
				return 0, err
			}

		}

		// todo: check presents should write in stripe or whole file
		// check opts HasNull==true than should always has Presents?

		for i, p := range presents {
			if p {
				values = append(values, vector[i])
			}
		}

	} else {
		values = vector
	}

	for _, v := range values {
		seconds = append(seconds, encoding.Zigzag(v.Seconds))
		nanos = append(nanos, encoding.EncodingNano(uint64(v.Nanos)))

		m := v.GetMilliSeconds()
		if *c.stats.TimestampStatistics.Minimum > m {
			*c.stats.TimestampStatistics.Minimum = m
		}
		if m > *c.stats.TimestampStatistics.Maximum {
			*c.stats.TimestampStatistics.Maximum = m
		}

		mu := v.GetMilliSecondsUtc()
		if *c.stats.TimestampStatistics.MinimumUtc > mu {
			*c.stats.TimestampStatistics.MinimumUtc = mu
		}
		if m > *c.stats.TimestampStatistics.MaximumUtc {
			*c.stats.TimestampStatistics.MaximumUtc = mu
		}
	}

	if sn, err = c.data.writeValues(seconds); err != nil {
		return 0, err
	}
	if nn, err = c.secondary.writeValues(nanos); err != nil {
		return 0, err
	}

	*c.stats.NumberOfValues += uint64(len(values))
	*c.stats.BytesOnDisk += uint64(pn + sn + nn)

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
	*treeWriter
	data *streamWriter
}

func newDateDirectV2Writer(schema *TypeDescription, opts *WriterOptions) *dateV2Writer {
	ds := &pb.DateStatistics{Minimum: new(int32), Maximum: new(int32)}
	stats := &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64), DateStatistics: ds}
	base := &treeWriter{schema: schema, present: newBoolStreamWriter(schema.Id, pb.Stream_PRESENT, opts), stats: stats}
	data := newIntV2Stream(schema.Id, pb.Stream_DATA, true, opts)
	return &dateV2Writer{base, data}
}

func (c *dateV2Writer) write(batch *batchInternal) (rows int, err error) {
	var values []uint64
	vector := batch.Vector.([]Date)
	rows = len(vector)

	var pn, dn int

	if len(batch.Presents) != 0 {
		if len(batch.Presents) != len(vector) {
			return 0, errors.New("rows of present != vector")
		}

		if !batch.presentsFromParent {
			*c.stats.HasNull = true
			if pn, err = c.present.writeValues(batch.Presents); err != nil {
				return 0, err
			}
		}

		for i, p := range batch.Presents {
			if p {
				values = append(values, encoding.Zigzag(toDays(vector[i])))
			}
		}

	} else {

		for _, v := range vector {
			values = append(values, encoding.Zigzag(toDays(v)))
		}
	}

	if dn, err = c.data.writeValues(values); err != nil {
		return 0, err
	}

	*c.stats.NumberOfValues += uint64(len(values))
	*c.stats.BytesOnDisk += uint64(pn + dn)

	return
}

func (c *dateV2Writer) getStreams() []*streamWriter {
	ss := make([]*streamWriter, 2)
	ss[0] = c.present
	ss[1] = c.data
	return ss
}

type decimal64DirectV2Writer struct {
	*treeWriter
	data      *streamWriter
	secondary *streamWriter
}

func newDecimal64DirectV2Writer(schema *TypeDescription, opts *WriterOptions) *decimal64DirectV2Writer {
	ds := &pb.DecimalStatistics{Maximum: new(string), Minimum: new(string), Sum: new(string)}
	stats := &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64), DecimalStatistics: ds}
	base := &treeWriter{schema: schema, present: newBoolStreamWriter(schema.Id, pb.Stream_PRESENT, opts), stats: stats}
	data := newBase128VarIntStreamWriter(schema.Id, pb.Stream_DATA, opts)
	secondary := newIntV2Stream(schema.Id, pb.Stream_SECONDARY, true, opts)
	return &decimal64DirectV2Writer{base, data, secondary}
}

func (c *decimal64DirectV2Writer) write(batch *batchInternal) (rows int, err error) {
	var precisions []int64
	var scales []uint64
	var values []Decimal64
	vector := batch.Vector.([]Decimal64)
	rows = len(vector)

	var pn, dn, sn int

	// todo: decimal
	//var min, max, sum Decimal64

	if len(batch.Presents) != 0 {
		if len(batch.Presents) != len(vector) {
			return 0, errors.New("rows of present != vector")
		}

		if !batch.presentsFromParent {
			if pn, err = c.present.writeValues(batch.Presents); err != nil {
				return 0, err
			}
			*c.stats.HasNull = true
		}

		for i, p := range batch.Presents {
			if p {
				values = append(values, vector[i])
			}
		}

	} else {
		values = vector
	}

	for _, v := range values {
		precisions = append(precisions, v.Precision)
		scales = append(scales, encoding.Zigzag(int64(v.Scale)))
	}

	if dn, err = c.data.writeValues(precisions); err != nil {
		return 0, err
	}
	if sn, err = c.secondary.writeValues(scales); err != nil {
		return 0, err
	}

	*c.stats.NumberOfValues += uint64(len(values))
	*c.stats.BytesOnDisk += uint64(pn + dn + sn)

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
	*treeWriter
	data *streamWriter
}

func newDoubleWriter(schema *TypeDescription, opts *WriterOptions) *doubleWriter {
	ds := &pb.DoubleStatistics{Minimum: new(float64), Maximum: new(float64), Sum: new(float64)}
	stats := &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64), DoubleStatistics: ds}
	base := &treeWriter{schema: schema, present: newBoolStreamWriter(schema.Id, pb.Stream_PRESENT, opts), stats: stats}
	data := newDoubleStream(schema.Id, pb.Stream_DATA, opts)
	return &doubleWriter{base, data}
}

func (c *doubleWriter) write(batch *batchInternal) (rows int, err error) {
	var values []float64
	vector := batch.Vector.([]float64)
	rows = len(vector)

	var pn, dn int

	if len(batch.Presents) != 0 {
		if len(batch.Presents) != len(vector) {
			return 0, errors.New("rows of present != vector")
		}

		if !batch.presentsFromParent {
			*c.stats.HasNull = true
			if pn, err = c.present.writeValues(batch.Presents); err != nil {
				return 0, err
			}
		}

		for i, p := range batch.Presents {
			if p {
				values = append(values, vector[i])
			}
		}

	} else {
		values = vector
	}

	for _, v := range values {
		if v < *c.stats.DoubleStatistics.Minimum {
			*c.stats.DoubleStatistics.Minimum = v
		}
		if v > *c.stats.DoubleStatistics.Maximum {
			*c.stats.DoubleStatistics.Maximum = v
		}
		*c.stats.DoubleStatistics.Sum += v
	}

	if dn, err = c.data.writeValues(values); err != nil {
		return 0, err
	}

	*c.stats.NumberOfValues += uint64(len(values))
	*c.stats.BytesOnDisk += uint64(pn + dn)

	return
}

func (c *doubleWriter) getStreams() []*streamWriter {
	ss := make([]*streamWriter, 2)
	ss[0] = c.present
	ss[1] = c.data
	return ss
}

type binaryDirectV2Writer struct {
	*treeWriter
	data   *streamWriter
	length *streamWriter
}

func newBinaryDirectV2Writer(schema *TypeDescription, opts *WriterOptions) *binaryDirectV2Writer {
	bs := &pb.BinaryStatistics{Sum: new(int64)}
	stats := &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64), BinaryStatistics: bs}
	base := &treeWriter{schema: schema, present: newBoolStreamWriter(schema.Id, pb.Stream_PRESENT, opts), stats: stats}
	data := newStringContentsStream(schema.Id, pb.Stream_DATA, opts)
	length := newIntV2Stream(schema.Id, pb.Stream_LENGTH, false, opts)
	return &binaryDirectV2Writer{base, data, length}
}

func (c *binaryDirectV2Writer) write(batch *batchInternal) (rows int, err error) {
	var values [][]byte
	var lengthValues []uint64
	vector := batch.Vector.([][]byte)
	rows = len(vector)

	var pn, dn, ln int

	if len(batch.Presents) != 0 {

		if len(batch.Presents) != len(vector) {
			return 0, errors.New("rows of present != vector")
		}

		if !batch.presentsFromParent {
			*c.stats.HasNull = true
			if pn, err = c.present.writeValues(batch.Presents); err != nil {
				return
			}
		}

		for i, p := range batch.Presents {
			if p {
				values = append(values, vector[i])
			}
		}

	} else {

		values = vector
	}

	for _, v := range values {
		l := len(v)
		lengthValues = append(lengthValues, uint64(l))
		*c.stats.BinaryStatistics.Sum += int64(l)
	}

	if dn, err = c.data.writeValues(values); err != nil {
		return
	}

	if ln, err = c.length.writeValues(lengthValues); err != nil {
		return
	}

	*c.stats.NumberOfValues += uint64(len(values))
	*c.stats.BytesOnDisk += uint64(pn + dn + ln)

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

func (c *stringV2Writer) write(batch *batchInternal) (rows int, err error) {
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
	*treeWriter
	data     *streamWriter
	dictData *streamWriter
	length   *streamWriter
}

func newStringDictV2Writer(schema *TypeDescription, opts *WriterOptions) *stringDictV2Writer {
	ss := &pb.StringStatistics{Sum: new(int64), Minimum: new(string), Maximum: new(string), LowerBound: new(string), UpperBound: new(string)}
	stats := &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64), StringStatistics: ss}
	base := &treeWriter{schema: schema, present: newBoolStreamWriter(schema.Id, pb.Stream_PRESENT, opts), stats: stats}
	data := newIntV2Stream(schema.Id, pb.Stream_DATA, false, opts)
	dictData := newStringContentsStream(schema.Id, pb.Stream_DICTIONARY_DATA, opts)
	length := newIntV2Stream(schema.Id, pb.Stream_LENGTH, false, opts)
	return &stringDictV2Writer{base, data, dictData, length}
}

func (c *stringDictV2Writer) write(batch *batchInternal) (rows int, err error) {
	var values []string
	vector := batch.Vector.([]string)
	rows = len(vector)

	var pn, dn, ddn, ln int

	if len(batch.Presents) != 0 {
		if len(batch.Presents) != len(vector) {
			return 0, errors.New("rows of present != vector")
		}

		if !batch.presentsFromParent {
			*c.stats.HasNull = true
			if pn, err = c.present.writeValues(batch.Presents); err != nil {
				return
			}
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

		// todo: stringStatistics
	}

	if dn, err = c.data.writeValues(d.indexes); err != nil {
		return
	}
	if ddn, err = c.dictData.writeValues(d.contents); err != nil {
		return
	}
	if ln, err = c.length.writeValues(d.lengths); err != nil {
		return
	}

	*c.stats.NumberOfValues += uint64(len(values))
	*c.stats.BytesOnDisk += uint64(pn + dn + ddn + ln)

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
	*treeWriter
	data   *streamWriter
	length *streamWriter
}

func newStringDirectV2Writer(schema *TypeDescription, opts *WriterOptions) *stringDirectV2Writer {
	ss := &pb.StringStatistics{Sum: new(int64), Minimum: new(string), Maximum: new(string), LowerBound: new(string), UpperBound: new(string)}
	stats := &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64), StringStatistics: ss}
	base := &treeWriter{schema: schema, present: newBoolStreamWriter(schema.Id, pb.Stream_PRESENT, opts), stats: stats}
	data := newStringContentsStream(schema.Id, pb.Stream_DATA, opts)
	length := newIntV2Stream(schema.Id, pb.Stream_LENGTH, false, opts)
	return &stringDirectV2Writer{base, data, length}
}

func (c *stringDirectV2Writer) write(batch *batchInternal) (rows int, err error) {
	var lengthVector []uint64
	var contents [][]byte
	vector := batch.Vector.([]string)
	var values []string
	rows = len(vector)

	var pn, cn, ln int

	if len(batch.Presents) != 0 {
		if len(batch.Presents) != len(vector) {
			return 0, errors.New("rows of present != vector")
		}

		if !batch.presentsFromParent {
			*c.stats.HasNull = true
			if pn, err = c.present.writeValues(batch.Presents); err != nil {
				return
			}
		}

		for i, p := range batch.Presents {
			if p {
				values = append(values, vector[i])
			}
		}

	} else {

		values = vector
	}

	for _, s := range values {
		// reassure: string encoding
		contents = append(contents, []byte(s))
		lengthVector = append(lengthVector, uint64(len(s)))

		// todo: string statistics
	}

	if cn, err = c.data.writeValues(contents); err != nil {
		return
	}
	if ln, err = c.length.writeValues(lengthVector); err != nil {
		return
	}

	*c.stats.NumberOfValues += uint64(len(values))
	*c.stats.BytesOnDisk += uint64(pn + cn + ln)

	return
}

func (c *stringDirectV2Writer) getStreams() []*encodingStreamWriter {
	ss := make([]*encodingStreamWriter, 3)
	ss[0] = c.present
	ss[1] = c.data
	ss[2] = c.length
	return ss
}

type boolWriter struct {
	*treeWriter
	data *EncodingStreamWriter
}

func newBoolWriter(schema *TypeDescription, opts *WriterOptions) *boolWriter {
	// todo: bucketstatitics?
	stats := &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64)}
	base := &treeWriter{schema: schema, present: newBoolStreamWriter(schema.Id, pb.Stream_PRESENT, opts), stats: stats}
	data := newBoolStreamWriter(schema.Id, pb.Stream_DATA, opts)
	return &boolWriter{base, data}
}

func (c *boolWriter) flush() error {
	if err := c.present.Flush(); err != nil {
		return err
	}
	if err := c.data.Flush(); err != nil {
		return err
	}
	return nil
}

func (c *boolWriter) writeOut(out io.Writer) (n int64, err error) {
	var pn, dn int64
	if pn, err = c.present.writeOut(out); err != nil {
		return
	}
	if dn, err = c.data.writeOut(out); err != nil {
		return
	}
	n = pn + dn
	return
}

func (c *boolWriter) getIndex() *pb.RowIndex {
	if c.writeIndex {
		index:= &pb.RowIndex{}
		for _, pp := range c.data.GetPositions() {
			entry:= &pb.RowIndexEntry{}
			entry.Positions= append(entry.Positions, pp...)
		}
	}
	return nil
}

func (c *boolWriter) getStreamInfos() []*pb.Stream {
	return []*pb.Stream{c.present.info, c.data.info}
}

func (c *boolWriter) reset() {
	c.treeWriter.reset()
	c.data.reset()
}

func (c *boolWriter) size() int {
	return c.present.size() + c.data.size()
}

func (c *boolWriter) write(presents []bool, presentsFromParent bool, vec interface{}) (rows int, err error) {
	vector := vec.([]bool)
	rows = len(vector)

	if len(presents) != 0 {
		if len(presents) != len(vector) {
			return 0, errors.New("present error")
		}

		for i, p := range presents {

			if !presentsFromParent {
				if err = c.present.Write(p); err != nil {
					return
				}
			}

			if p {
				if err = c.data.Write(vector[i]); err != nil {
					return
				}
				// todo: bucketstatustics?
				*c.stats.NumberOfValues++
			}

			if c.writeIndex {
				c.indexInRows++
				if c.indexInRows >= c.indexStride {
					if !presentsFromParent {
						c.present.MarkPosition()
					}
					c.data.MarkPosition()
					c.indexInRows = 0
				}
			}

		}

		if !presentsFromParent {
			*c.stats.HasNull = true
		}

		return
	}

	for _, v := range vector {
		if err = c.data.Write(v); err != nil {
			return
		}

		if c.writeIndex {
			c.indexInRows++
			if c.indexInRows >= c.indexStride {
				c.data.MarkPosition()
				c.indexInRows = 0
			}
		}

		*c.stats.NumberOfValues++
		// todo: bucketstatustics?
	}

	return
}

type byteWriter struct {
	*treeWriter
	data *EncodingStreamWriter
}

func newByteWriter(schema *TypeDescription, opts *WriterOptions) *byteWriter {
	bs := &pb.BinaryStatistics{Sum: new(int64)}
	stats := &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64), BinaryStatistics: bs}
	base := &treeWriter{schema: schema, present: newBoolStreamWriter(schema.Id, pb.Stream_PRESENT, opts), stats: stats}
	data := newByteStreamWriter(schema.Id, pb.Stream_DATA, opts)
	return &byteWriter{base, data}
}

func (c *byteWriter) write(presents []bool, presentsFromParent bool, vec interface{}) (rows int, err error) {
	vector := vec.([]byte)
	rows = len(vector)

	if len(presents) != 0 {

		if len(presents) != len(vector) {
			return 0, errors.New("presents error")
		}

		for i, p := range presents {

			if !presentsFromParent {
				if err = c.present.Write(p); err != nil {
					return
				}
			}

			if p {
				if err = c.data.Write(vector[i]); err != nil {
					return
				}
				// make sure ??
				*c.stats.BinaryStatistics.Sum++
				*c.stats.NumberOfValues++
			}

			if c.writeIndex {
				c.indexInRows++
				if c.indexInRows >= c.indexStride {
					if !presentsFromParent {
						c.present.MarkPosition()
					}
					c.data.MarkPosition()
					c.indexInRows = 0
				}
			}
		}

		if !presentsFromParent {
			*c.stats.HasNull = true
		}

		return
	}

	for _, v := range vector {
		if err = c.data.Write(v); err != nil {
			return
		}
		*c.stats.BinaryStatistics.Sum++
		*c.stats.NumberOfValues++

		if c.writeIndex {
			c.indexInRows++
			if c.indexInRows >= c.indexStride {
				c.data.MarkPosition()
				c.indexInRows = 0
			}
		}
	}

	return
}

func (c *byteWriter) flush() error {
	var err error
	if err = c.present.flush(); err != nil {
		return err
	}
	if err = c.data.flush(); err != nil {
		return err
	}
	*c.stats.BytesOnDisk += c.present.length()
	*c.stats.BytesOnDisk += c.data.length()
	return nil
}

func (c *byteWriter) size() int {
	return c.present.size() + c.data.size()
}

func (c *byteWriter) getStreamInfos() []*pb.Stream {
	return []*pb.Stream{c.present.info, c.data.info}
}

func (c *byteWriter) reset() {
	c.treeWriter.reset()
	c.data.reset()
}

func (c *byteWriter) writeOut(out io.Writer) (n int64, err error) {
	var np, nd int64
	if np, err = c.present.writeOut(out); err != nil {
		return
	}
	if nd, err = c.data.writeOut(out); err != nil {
		return
	}
	n = np + nd
	return
}

func (c *byteWriter) getIndex() *pb.RowIndex {
	if c.writeIndex {
		index := &pb.RowIndex{}
		for i, pp := range c.data.GetPositions() {
			entry := &pb.RowIndexEntry{}
			if len(c.present.GetPositions()) != 0 {
				entry.Positions = append(entry.Positions, c.present.GetPositions()[i]...)
			}
			entry.Positions = append(entry.Positions, pp...)
		}
		return index
	}
	return nil
}

type longV2Writer struct {
	*treeWriter
	data *streamWriter
}

func newLongV2Writer(schema *TypeDescription, opts *WriterOptions) *longV2Writer {
	is := &pb.IntegerStatistics{Minimum: new(int64), Maximum: new(int64), Sum: new(int64)}
	stats := &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64), IntStatistics: is}
	base := &treeWriter{schema: schema, present: newBoolStreamWriter(schema.Id, pb.Stream_PRESENT, opts), stats: stats}
	data := newIntV2Stream(schema.Id, pb.Stream_DATA, true, opts)
	return &longV2Writer{base, data}
}

func (c *longV2Writer) write(batch *batchInternal) (rows int, err error) {
	var uvalues []uint64
	var values []int64

	vector := batch.Vector.([]int64)
	rows = len(vector)

	var pn, dn int

	if len(batch.Presents) != 0 {

		if len(batch.Presents) != len(vector) {
			return 0, errors.New("rows of presents != vector")
		}

		if !batch.presentsFromParent {
			*c.stats.HasNull = true

			log.Tracef("writing: long column write %d presents", len(batch.Presents))
			if pn, err = c.present.writeValues(batch.Presents); err != nil {
				return
			}
		}

		for i, p := range batch.Presents {
			if p {
				values = append(values, vector[i])
			}
		}

	} else {

		values = vector
	}

	for _, v := range values {
		uvalues = append(uvalues, encoding.Zigzag(v))

		if v < *c.stats.IntStatistics.Minimum {
			*c.stats.IntStatistics.Minimum = v
		}
		if v > *c.stats.IntStatistics.Maximum {
			*c.stats.IntStatistics.Maximum = v
		}
		*c.stats.IntStatistics.Sum += v
	}

	if dn, err = c.data.writeValues(uvalues); err != nil {
		return
	}

	*c.stats.NumberOfValues += uint64(len(values))
	// reassure: bytes on disk includes presents
	*c.stats.BytesOnDisk += uint64(pn + dn)

	return
}

func (c longV2Writer) getStreams() []*streamWriter {
	ss := make([]*streamWriter, 2)
	ss[0] = c.present
	ss[1] = c.data
	return ss
}

type floatWriter struct {
	*treeWriter
	data *streamWriter
}

func newFloatWriter(schema *TypeDescription, opts *WriterOptions) *floatWriter {
	ds := &pb.DoubleStatistics{Sum: new(float64), Minimum: new(float64), Maximum: new(float64)}
	stats := &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64), DoubleStatistics: ds}
	base := &treeWriter{schema: schema, present: newBoolStreamWriter(schema.Id, pb.Stream_PRESENT, opts), stats: stats}
	data := newFloatStream(schema.Id, pb.Stream_DATA, opts)
	return &floatWriter{base, data}
}

func (c *floatWriter) write(batch *batchInternal) (rows int, err error) {
	var values []float32
	vector := batch.Vector.([]float32)
	rows = len(vector)

	var pn, dn int

	if len(batch.Presents) != 0 {
		if len(batch.Presents) != len(vector) {
			return 0, errors.New("rows of presents != vector")
		}

		if !batch.presentsFromParent {
			*c.stats.HasNull = true

			log.Tracef("writing: float column write %d presents", len(batch.Presents))
			if pn, err = c.present.writeValues(batch.Presents); err != nil {
				return
			}
		}

		for i, p := range batch.Presents {
			if p {
				values = append(values, vector[i])
			}
		}

	} else {
		values = vector
	}

	for _, v := range values {
		if float64(v) < *c.stats.DoubleStatistics.Minimum {
			*c.stats.DoubleStatistics.Minimum = float64(v)
		}
		if float64(v) > *c.stats.DoubleStatistics.Maximum {
			*c.stats.DoubleStatistics.Maximum = float64(v)
		}
		*c.stats.DoubleStatistics.Sum += float64(v)
	}

	if dn, err = c.data.writeValues(values); err != nil {
		return
	}

	*c.stats.NumberOfValues += uint64(len(values))
	*c.stats.BytesOnDisk += uint64(pn + dn)

	return
}

func (c floatWriter) getStreams() []*streamWriter {
	ss := make([]*streamWriter, 2)
	ss[0] = c.present
	ss[1] = c.data
	return ss
}

type streamWriter struct {
	info *pb.Stream

	buf           *bytes.Buffer
	compressedBuf *bytes.Buffer

	compressionKind pb.CompressionKind
	chunkSize       int
	chunkMark       int

	//opts *WriterOptions

	//encodingBuf *bytes.Buffer
	//encoder     encoding.Encoder
}

/*func (s *streamWriter) writeByte(c byte) error {
	var err error

	if err = s.buf.WriteByte(c); err != nil {
		return err
	}

	if s.compressionKind == pb.CompressionKind_NONE || s.buf.Len() < s.chunkSize {
		return err
	}

	return compressAChunk(s.compressionKind, s.chunkSize, s.compressedBuf, s.buf)
}*/

// Write write p to stream buf
func (s *streamWriter) write(p []byte) error {
	if len(p) == 0 {
		return nil
	}

	if _, err := s.buf.Write(p); err != nil {
		return err
	}

	if s.compressionKind == pb.CompressionKind_NONE || s.buf.Len() < s.chunkSize {
		return nil
	}

	// compress chunksize data from buf to compressedbuf
	// todo: move buf data after compression
	if err := compressAChunk(s.compressionKind, s.chunkSize, s.compressedBuf, s.buf); err != nil {
		return err
	}

	return nil
}

// return compressed data + uncompressed data
func (s *streamWriter) size() int {
	if s.compressionKind == pb.CompressionKind_NONE {
		return s.buf.Len()
	}

	return s.compressedBuf.Len() + s.buf.Len()
}

func (s *streamWriter) markPosition() []uint64 {
	if s.compressionKind == pb.CompressionKind_NONE {
		return []uint64{uint64(s.buf.Len())}
	}

	return []uint64{uint64(s.compressedBuf.Len()), uint64(s.buf.Len())}
}

func (s *streamWriter) flush() error {
	if s.compressionKind == pb.CompressionKind_NONE {
		*s.info.Length = uint64(s.buf.Len())
		return nil
	}

	// compressing remaining
	if s.buf.Len() != 0 {
		if err := compressAChunk(s.compressionKind, s.compressedBuf, s.buf); err != nil {
			return err
		}
	}

	*s.info.Length = uint64(s.compressedBuf.Len())

	return nil
}

func (s *streamWriter) length() uint64 {
	return s.info.GetLength()
}

func (s *streamWriter) writeOut(out io.Writer) (n int64, err error) {
	if s.compressionKind == pb.CompressionKind_NONE {
		n, err = s.buf.WriteTo(out)
		return
	}

	n, err = s.compressedBuf.WriteTo(out)
	return
}

type EncodingStreamWriter struct {
	stream *streamWriter
	encoder encoding.Encoder
}

// mark position and collect stats
func (w EncodingStreamWriter) markPosition() {
	w.encoder.Flush()
	w.encoder.MarkPosition()
	w.stream.markPosition()

	// todo: update statistics
}

// info will update after flush
func (w EncodingStreamWriter) info() *pb.Stream {
	return w.stream.info
}

func (w *EncodingStreamWriter) reset() {
	w.stream.reset()
	w.encoder.Reset()
}

func (w *EncodingStreamWriter) write(v interface{}) error {
	var err error
	var data []byte
	if data, err = w.encoder.Encode(v); err != nil {
		return err
	}
	err = w.stream.write(data)
	return err
}

func (w *EncodingStreamWriter) flush() error {
	var err error
	var data []byte
	if data, err = w.encoder.Flush(); err != nil {
		return err
	}
	if err = w.stream.write(data); err != nil {
		return err
	}
	return w.stream.flush()
}

func (w EncodingStreamWriter) getPositions() [][]uint64 {

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

/*// todo: refactoring write []int64
func (s *streamWriter) writeValues(values interface{}) (n int, err error) {
	mark := s.buf.Len()

	s.encodingBuf.Reset()
	if err = s.encoder.Encode(s.encodingBuf, values); err != nil {
		return
	}

	if err = compress(s.opts.CompressionKind, s.opts.ChunkSize, s.buf, s.encodingBuf); err != nil {
		return
	}

	*s.info.Length = uint64(s.buf.Len())

	n = s.buf.Len() - mark

	// s.info.Get.. and s.info.Kind.St... invoked even with trace level
	log.Debugf("stream id %d - %s wrote length %d", s.info.GetColumn(), s.info.Kind.String(), n)
	return
}
*/

func (s *streamWriter) reset() {
	*s.info.Length = 0

	s.buf.Reset()
}

func (w *writer) GetSchema() *TypeDescription {
	return w.stripe.schemas[0]
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
	ft.Types = schemasToTypes(w.stripe.schemas)

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

	return w.out.Close()

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

func newBoolStreamWriter(id uint32, kind pb.Stream_Kind, opts *WriterOptions) *EncodingStreamWriter {
	kind_ := kind
	id_ := id
	length := uint64(0)
	info := &pb.Stream{Kind: &kind_, Column: &id_, Length: &length}
	var stream *streamWriter
	if opts.CompressionKind == pb.CompressionKind_NONE {
		stream = &streamWriter{info: info, buf: bytes.NewBuffer(make([]byte, opts.ChunkSize))}
	} else {
		stream = &streamWriter{info: info, buf: bytes.NewBuffer(make([]byte, opts.ChunkSize)), compressedBuf: bytes.NewBuffer(make([]byte, opts.ChunkSize))}
	}
	return &encodingStreamWriter{stream: stream, encoder: encoding.NewBoolEncoder()}
}

func newByteStreamWriter(id uint32, kind pb.Stream_Kind, opts *WriterOptions) *encodingStreamWriter {
	id_ := id
	kind_ := kind
	length := uint64(0)
	info := &pb.Stream{Kind: &kind_, Column: &id_, Length: &length}

	var compresssedBuf *bytes.Buffer
	if opts.CompressionKind != pb.CompressionKind_NONE {
		compresssedBuf = bytes.NewBuffer(make([]byte, opts.ChunkSize))
	}

	stream := &streamWriter{info: info, buf: bytes.NewBuffer(make([]byte, opts.ChunkSize)), compressedBuf: compresssedBuf}
	return &encodingStreamWriter{stream: stream, encoder: encoding.NewByteEncoder()}
}

func newIntV2Stream(id uint32, kind pb.Stream_Kind, signed bool, opts *WriterOptions) *streamWriter {
	info := &pb.Stream{Kind: &kind, Column: &id, Length: new(uint64)}
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
	encoder := &encoding.Base128VarInt{}
	info_ := &pb.Stream{Kind: &kind, Column: &id, Length: new(uint64)}
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
