package orc

import (
	"bytes"
	"compress/flate"
	"os"

	"github.com/gogo/protobuf/proto"
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
	ChunkSize  int
	CMPKind    pb.CompressionKind
	RowSize    int
	StripeSize int
}

func DefaultWriterOptions() *WriterOptions {
	o := &WriterOptions{}
	o.CMPKind = pb.CompressionKind_ZLIB
	o.StripeSize = DEFAULT_STRIPE_SIZE
	o.ChunkSize = DEFAULT_CHUNK_SIZE
	return o
}

type Writer interface {
	GetSchema() *TypeDescription

	Write(batch *ColumnVector) error

	Close() error
}

// cannot used concurrently, not synchronized
// strip buffered in memory until the strip size
// writeValues out by columns
type writer struct {
	path   string
	f      *os.File
	offset uint64

	ps *pb.PostScript

	stripe *stripeW

	stripeInfos []*pb.StripeInformation
	columnStats []*pb.ColumnStatistics

	schemas []*TypeDescription

	opts *WriterOptions
}

type stripeW struct {
	schemas []*TypeDescription

	opts *WriterOptions

	// streams <id, streamWriter{present, data, length}>
	streams map[uint32][]*valuesWriter

	idxBuf *bytes.Buffer // index area buffer
	//dataBuf *bytes.Buffer // data area buffer

	info *pb.StripeInformation

	bufferedSize int
}

func NewWriter(path string, schema *TypeDescription, opts *WriterOptions) (Writer, error) {
	// fixme: create new one, error when exist
	log.Infof("open %s", path)
	f, err := os.Create(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	schemas := schema.normalize()
	for _, s := range schemas {
		s.Encoding = getColumnEncoding(opts, s.Kind)
	}
	w := &writer{opts: opts, path: path, f: f, schemas: schemas}
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

func newStripe(offset uint64, schemas []*TypeDescription, opts *WriterOptions) (stripe *stripeW, err error) {
	idxBuf := bytes.NewBuffer(make([]byte, DEFAULT_INDEX_SIZE))
	idxBuf.Reset()

	// prepare streams
	streams := make(map[uint32][]*valuesWriter)
	for _, schema := range schemas {
		switch schema.Kind {
		case pb.Type_SHORT:
			fallthrough
		case pb.Type_INT:
			fallthrough
		case pb.Type_LONG:
			streams[schema.Id] = make([]*valuesWriter, 2)
			streams[schema.Id][1] = newSignedIntStreamV2(schema.Id, pb.Stream_DATA, opts)

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
				streams[schema.Id] = make([]*valuesWriter, 3)
				streams[schema.Id][1] = newStringStreamV2(schema.Id, pb.Stream_DATA, opts)
				streams[schema.Id][2] = newUnsignedIntStreamV2(schema.Id, pb.Stream_LENGTH, opts)
				break
			}

			if schema.Encoding == pb.ColumnEncoding_DICTIONARY_V2 {
				streams[schema.Id] = make([]*valuesWriter, 4)
				streams[schema.Id][1] = newUnsignedIntStreamV2(schema.Id, pb.Stream_DATA, opts)
				streams[schema.Id][2] = newStringStreamV2(schema.Id, pb.Stream_DICTIONARY_DATA, opts)
				streams[schema.Id][3] = newUnsignedIntStreamV2(schema.Id, pb.Stream_LENGTH, opts)
				break
			}

			return nil, errors.New("encoding not impl")

		case pb.Type_BOOLEAN:
			if schema.Encoding != pb.ColumnEncoding_DIRECT {
				return nil, errors.New("encoding error")
			}
			streams[schema.Id] = make([]*valuesWriter, 2)
			streams[schema.Id][1] = newBoolDataStream(schema.Id, opts)

		case pb.Type_BYTE:
			if schema.Encoding != pb.ColumnEncoding_DIRECT {
				return nil, errors.New("encoding error")
			}
			streams[schema.Id] = make([]*valuesWriter, 2)
			streams[schema.Id][1] = newByteStream(schema.Id, pb.Stream_DATA, opts)

		case pb.Type_BINARY:
			if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
				streams[schema.Id] = make([]*valuesWriter, 3)
				streams[schema.Id][1] = newStringStreamV2(schema.Id, pb.Stream_DATA, opts)
				streams[schema.Id][2] = newUnsignedIntStreamV2(schema.Id, pb.Stream_LENGTH, opts)
				break
			}

			return nil, errors.New("encoding not impl")

		case pb.Type_DECIMAL:
			if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
				streams[schema.Id] = make([]*valuesWriter, 3)
				streams[schema.Id][1] = newBase128VarIntsDataStream(schema.Id, opts)
				streams[schema.Id][2] = newUnsignedIntStreamV2(schema.Id, pb.Stream_SECONDARY, opts)
				break
			}
			return nil, errors.New("encoding not impl")

		case pb.Type_DATE:
			if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
				streams[schema.Id] = make([]*valuesWriter, 2)
				streams[schema.Id][1] = newUnsignedIntStreamV2(schema.Id, pb.Stream_DATA, opts)
				break
			}
			return nil, errors.New("encoding not impl")

		case pb.Type_TIMESTAMP:
			if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
				streams[schema.Id] = make([]*valuesWriter, 3)
				streams[schema.Id][1] = newIntDataStreamV2(schema.Id, opts)
				streams[schema.Id][2] = newUnsignedIntStreamV2(schema.Id, pb.Stream_SECONDARY, opts)
				break
			}
			return nil, errors.New("encoding not impl")

		case pb.Type_STRUCT:
			if schema.Encoding != pb.ColumnEncoding_DIRECT {
				return nil, errors.New("encoding error")
			}
			streams[schema.Id] = make([]*valuesWriter, 1)

		case pb.Type_LIST:
			if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
				streams[schema.Id] = make([]*valuesWriter, 2)
				streams[schema.Id][1] = newUnsignedIntStreamV2(schema.Id, pb.Stream_LENGTH, opts)
				break
			}
			return nil, errors.New("encoding not impl")

		case pb.Type_MAP:
			if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
				streams[schema.Id] = make([]*valuesWriter, 2)
				streams[schema.Id][1] = newUnsignedIntStreamV2(schema.Id, pb.Stream_LENGTH, opts)
				break
			}
			return nil, errors.New("encoding not impl")

		case pb.Type_UNION:
			if schema.Encoding != pb.ColumnEncoding_DIRECT {
				return nil, errors.New("encoding error")
			}
			streams[schema.Id] = make([]*valuesWriter, 2)
			// todo:
			//streams[schema.Id][1]= newByteStream(schema.Id, pb.Stream_DIRECT)
		}

		if schema.HasNulls {
			streams[schema.Id][0] = newPresentStream(schema.Id, opts)
		}
	}

	info := &pb.StripeInformation{}
	info.Offset = &offset
	s := &stripeW{opts: opts, idxBuf: idxBuf, streams: streams, info: info, schemas: schemas}
	return s, nil
}

func (w *writer) Write(batch *ColumnVector) error {

	if err := w.stripe.write(batch); err != nil {
		return err
	}

	return w.flushStripe(false)
}

// refactoring: whole stripe buffered in memory and flush out?
func (w *writer) flushStripe(force bool) error {
	// a currentStripe should contains whole row
	if w.stripe.shouldFlush() || force {
		if err := w.stripe.flush(w.f); err != nil {
			return errors.WithStack(err)
		}
		// todo: update column stats
		// reset current currentStripe
		w.offset += w.stripe.info.GetOffset() + w.stripe.info.GetIndexLength() + w.stripe.info.GetDataLength()
		w.stripeInfos = append(w.stripeInfos, w.stripe.info)
		log.Debugf("flushed currentStripe %v", w.stripe.info)

		// todo:
		//w.stripe.reset()
	}
	return nil
}

func (s *stripeW) shouldFlush() bool {
	/*var l int
	for _, td := range s.schemas {
		for _, stream := range s.streams[td.Id] {
			if s != nil {
				l += stream.compressedBuf.Len()
			}
		}
	}*/
	return s.bufferedSize >= s.opts.StripeSize
}

func (stripe *stripeW) write(batch *ColumnVector) error {

	var rows int

	// presents
	if stripe.schemas[batch.Id].HasNulls {
		if len(batch.Presents) == 0 {
			return errors.New("column has nulls, but column present length 0")
		}
		present := stripe.streams[batch.Id][0]
		written, err := present.write(batch.Presents)
		if err != nil {
			return err
		}
		*stripe.info.DataLength += uint64(written)
	}

	data := stripe.streams[batch.Id][1]
	columnEncoding := stripe.schemas[batch.Id].Encoding

	switch stripe.schemas[batch.Id].Kind {
	case pb.Type_BOOLEAN:
		var vector []bool
		values := batch.Vector.([]bool)
		rows = len(values)

		if stripe.schemas[batch.Id].HasNulls {
			if len(batch.Presents) != len(batch.Vector.([]bool)) {
				return errors.New("present error")
			}

			for i, p := range batch.Presents {
				if p {
					vector = append(vector, values[i])
				}
			}
		} else {
			vector = values
		}

		written, err := data.write(vector)
		if err != nil {
			return err
		}
		*stripe.info.DataLength += uint64(written)

	case pb.Type_BYTE:
		var vector []byte
		values := batch.Vector.([]byte)
		rows = len(values)

		if stripe.schemas[batch.Id].HasNulls {
			// todo: presents data check

			for i, p := range batch.Presents {
				if p {
					vector = append(vector, values[i])
				}
			}
		} else {
			vector = values
		}

		written, err := data.write(vector)
		if err != nil {
			return err
		}
		*stripe.info.DataLength += uint64(written)

	case pb.Type_SHORT:
		fallthrough
	case pb.Type_INT:
		fallthrough
	case pb.Type_LONG:
		var vector []uint64

		if columnEncoding == pb.ColumnEncoding_DIRECT_V2 {
			if stripe.schemas[batch.Id].HasNulls {
				rows = len(batch.Presents)
				// todo: presents data check
				for i, p := range batch.Presents {
					if p {
						vector = append(vector, encoding.Zigzag(batch.Vector.([]int64)[i]))
					}
				}

			} else {
				rows = len(batch.Vector.([]int64))
				for _, v := range batch.Vector.([]int64) {
					vector = append(vector, encoding.Zigzag(v))
				}
			}

			written, err := data.write(vector)
			if err != nil {
				return err
			}

			*stripe.info.DataLength += uint64(written)
		}

	case pb.Type_STRING:

		var lengthVector []uint64
		var contents [][]byte
		values := batch.Vector.([]string)
		rows = len(values)

		if stripe.schemas[batch.Id].HasNulls {
			// todo: check presents data

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

		if columnEncoding == pb.ColumnEncoding_DIRECT_V2 {
			written, err := data.write(contents)
			if err != nil {
				return err
			}
			*stripe.info.DataLength += uint64(written)

			lengthStream := stripe.streams[batch.Id][2]
			written, err = lengthStream.write(lengthVector)
			if err != nil {
				return err
			}
			*stripe.info.DataLength += uint64(written)

			break
		}

	case pb.Type_BINARY:

		var vector [][]byte
		var lengthVector []uint64
		values := batch.Vector.([][]byte)
		rows = len(values)

		if stripe.schemas[batch.Id].HasNulls {
			//todo: check presents data

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

		if columnEncoding == pb.ColumnEncoding_DIRECT_V2 {
			written, err := data.write(vector)
			if err != nil {
				return err
			}
			*stripe.info.DataLength += uint64(written)

			written, err = stripe.streams[batch.Id][2].write(lengthVector)
			if err != nil {
				return err
			}
			*stripe.info.DataLength += uint64(written)

			break
		}

	case pb.Type_DOUBLE:

		var vector []float64
		values := batch.Vector.([]float64)
		rows = len(values)

		if stripe.schemas[batch.Id].HasNulls {
			// todo: check presents data
			for i, p := range batch.Presents {
				if p {
					vector = append(vector, values[i])
				}
			}
		} else {
			vector = values
		}

		written, err := data.write(vector)
		if err != nil {
			return err
		}
		*stripe.info.DataLength += uint64(written)

	case pb.Type_DECIMAL:

		var precisions []int64
		var scales []uint64
		values := batch.Vector.([]Decimal64)
		rows = len(values)

		if stripe.schemas[batch.Id].HasNulls {
			// todo: check presents data

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

		if columnEncoding == pb.ColumnEncoding_DIRECT_V2 {
			written, err := data.write(precisions)
			if err != nil {
				return err
			}
			*stripe.info.DataLength += uint64(written)

			written, err = stripe.streams[batch.Id][2].write(scales)
			if err != nil {
				return err
			}
			*stripe.info.DataLength += uint64(written)
			break
		}

	case pb.Type_DATE:

		var vector []int64
		values := batch.Vector.([]Date)
		rows = len(values)

		if stripe.schemas[batch.Id].HasNulls {
			// todo: check presents data

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

		if columnEncoding == pb.ColumnEncoding_DIRECT_V2 {
			written, err := data.write(vector)
			if err != nil {
				return err
			}
			*stripe.info.DataLength += uint64(written)

			break
		}

	case pb.Type_TIMESTAMP:

		var seconds []int64
		var nanos []uint64
		values := batch.Vector.([]Timestamp)
		rows = len(values)

		if stripe.schemas[batch.Id].HasNulls {
			//

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

		if columnEncoding == pb.ColumnEncoding_DIRECT_V2 {
			written, err := data.write(seconds)
			if err != nil {
				return err
			}
			*stripe.info.DataLength += uint64(written)

			// secondary
			written, err = stripe.streams[batch.Id][1].write(nanos)
			if err != nil {
				return err
			}
			*stripe.info.DataLength += uint64(written)

			break
		}

	case pb.Type_STRUCT:
		// todo: presents check

		values := batch.Vector.([]*ColumnVector)
		rows = len(values)

		for _, v := range values {
			if err := stripe.write(v); err != nil {
				return err
			}
		}

	case pb.Type_LIST:
	// todo:

	case pb.Type_MAP:
		// todo:

	default:
		return errors.New("type not known")
	}

	*stripe.info.NumberOfRows += uint64(rows)

	return nil
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

// 1 currentStripe should be self-contained
func (s *stripeW) flush(f *os.File) error {

	stripeFooter := &pb.StripeFooter{}

	// row number updated at writeValues
	idxLength := uint64(s.idxBuf.Len())
	// valueBuf will be reset after writeTo
	_, err := s.idxBuf.WriteTo(f)
	if err != nil {
		return errors.WithStack(err)
	}
	log.Debugf("flush index with length %d", idxLength)
	s.info.IndexLength = &idxLength

	//var dataLength uint64
	for _, schema := range s.schemas {
		for _, stream := range s.streams[schema.Id] {
			if stream != nil {
				log.Tracef("flush stream %s of column %d length %d", stream.s.info.GetKind().String(),
					stream.s.info.GetColumn(), stream.s.info.GetLength())
				if _, err := stream.valueBuf.WriteTo(f); err != nil {
					return errors.WithStack(err)
				}
				//dataLength += uint64(n)

				stripeFooter.Streams = append(stripeFooter.Streams, stream.s.info)
			}
		}

		stripeFooter.Columns = append(stripeFooter.Columns, &pb.ColumnEncoding{Kind: &schema.Encoding})
	}

	// write footer
	footerBuf, err := proto.Marshal(stripeFooter)
	if err != nil {
		return errors.WithStack(err)
	}
	compressedFooterBuf, err := compressByteSlice(s.opts.CMPKind, s.opts.ChunkSize, footerBuf)
	if err != nil {
		return err
	}
	ftLength := uint64(len(compressedFooterBuf))
	log.Debugf("writing stripe footer with length: %d", ftLength)
	if _, err := f.Write(compressedFooterBuf); err != nil {
		return errors.WithStack(err)
	}
	s.info.FooterLength = &ftLength

	return nil
}

type valuesWriter struct {
	s *streamWriter

	encoder encoding.Encoder

	opts *WriterOptions

	valueBuf *bytes.Buffer
}

func (w *valuesWriter) write(values interface{}) (written int, err error) {

	if err = w.encoder.WriteValues(w.valueBuf, values); err != nil {
		return 0, err
	}

	return w.s.write(w.valueBuf)
}

type streamWriter struct {
	info *pb.Stream
	//encoding      *pb.ColumnEncoding

	buf *bytes.Buffer
	//compressedBuf *bytes.Buffer

	//encoder encoding.Encoder

	opts *WriterOptions
}

func (s *streamWriter) write(data *bytes.Buffer) (written int, err error) {
	log.Debugf("stream %d-%s write ", s.info.GetColumn(), s.info.Kind.String())

	/*if err = s.encoder.WriteValues(s.valueBuf, values); err != nil {
		return 0, err
	}*/

	start := s.buf.Len()
	switch s.opts.CMPKind {
	case pb.CompressionKind_ZLIB:
		if err := zlibCompressing(s.opts.ChunkSize, s.buf, data); err != nil {
			return 0, err
		}

	default:
		return 0, errors.New("compress other than ZLIB not impl")
	}
	end := s.buf.Len()

	l := end - start
	*s.info.Length += uint64(l)
	return l, nil
}

/*func (s *byteStream) writeBytes(values []byte) error {
	log.Debugf("streamR %d-%s writeValues bytes", s.info.GetColumn(), s.info.Kind.String())
	if err := s.encoder.WriteValues(s.valueBuf, values); err != nil {
		return err
	}
	return nil
}

type longStream struct {
	stream
	encoder encoding.IntRleV2
}

func (s *longStream) writeULongsV2(values []uint64) error {
	log.Debugf("streamR %d-%s writeValues ulongs", s.info.GetColumn(), s.info.Kind.String())
	if err := s.encoder.WriteValues(s.valueBuf, false, values); err != nil {
		return err
	}
	return nil
}

func (s *streamWriter) writeLongsV2(v []int64) error {
	irl := s.encoder.(*intRleV2)
	irl.literals = v
	log.Debugf("streamR %d-%s writeValues longs", s.info.GetColumn(), s.info.Kind.String())
	if err := irl.writeValues(s.valueBuf); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (s *streamWriter) writeBytesDirectV2(bs [][]byte) error {
	enc := s.encoder.(*bytesContent)
	enc.content = bs
	log.Debugf("streamR %d-%s writeValues byte slice", s.info.GetColumn(), s.info.Kind.String())
	if err := enc.writeValues(s.valueBuf); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (s *streamWriter) writeBase128VarInts(values []int64) error {
	enc := s.encoder.(*base128VarInt)
	enc.values = values
	log.Debugf("streamR %d-%s writeValues base128varints", s.info.GetColumn(), s.info.Kind.String())
	if err := enc.writeValues(s.valueBuf); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (s *streamWriter) writeDoubles(values []float64) error {
	enc := s.encoder.(*ieee754Double)
	enc.values = values
	log.Debugf("streamR %d-%s writeValues doubles", s.info.GetColumn(), s.info.Kind.String())
	if err := enc.writeValues(s.valueBuf); err != nil {
		return errors.WithStack(err)
	}
	return nil
}*/

/*func (s *streamWriter) compress() error {
	switch s.opts.CMPKind {
	case pb.CompressionKind_ZLIB:
		if err := zlibCompressing(s.opts.ChunkSize, s.compressedBuf, s.valueBuf); err != nil {
			return err
		}

	default:
		return errors.New("compress other than ZLIB not impl")
	}
	return nil
}*/

func (s *streamWriter) reset() {
	*s.info.Length = 0
	s.buf.Reset()
}

func (w *writer) GetSchema() *TypeDescription {
	return w.schemas[0]
}

func (w *writer) Close() error {
	if err := w.flushStripe(true); err != nil {
		return errors.WithStack(err)
	}
	if err := w.writeFileTail(); err != nil {
		return errors.WithStack(err)
	}

	w.f.Close()
	return nil
}

func (w *writer) writeHeader() (uint64, error) {
	b := []byte(MAGIC)
	if _, err := w.f.Write(b); err != nil {
		return 0, errors.WithStack(err)
	}
	return uint64(len(b)), nil
}

func (w *writer) writeFileTail() error {
	// writeValues footer
	// todo: rowsinstrde
	ft := &pb.Footer{HeaderLength: new(uint64), ContentLength: new(uint64), NumberOfRows: new(uint64)}
	*ft.HeaderLength = 3 // always 3
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
		return errors.Wrap(err, "marshall footer error")
	}
	ftCmpBuf, err := compressByteSlice(w.opts.CMPKind, w.opts.ChunkSize, ftb)
	if err != nil {
		return errors.WithStack(err)
	}
	ftl := uint64(len(ftCmpBuf))
	if _, err := w.f.Write(ftCmpBuf); err != nil {
		return errors.WithStack(err)
	}
	log.Debugf("writeValues file footer with length: %d", ftl)

	// writeValues postscript
	ps := &pb.PostScript{}
	ps.FooterLength = &ftl
	ps.Compression = &w.opts.CMPKind
	c := uint64(w.opts.ChunkSize)
	ps.CompressionBlockSize = &c
	ps.Version = VERSION
	m := MAGIC
	ps.Magic = &m
	psb, err := proto.Marshal(ps)
	if err != nil {
		return errors.WithStack(err)
	}
	n, err := w.f.Write(psb)
	if err != nil {
		return errors.Wrap(err, "writeValues PS error")
	}
	log.Debugf("writeValues postscript with length %d", n)
	// last byte is ps length
	if _, err = w.f.Write([]byte{byte(n)}); err != nil {
		return errors.Wrap(err, "writeValues PS length error")
	}

	return nil
}

// zlib compress src valueBuf into dst, maybe to several chunks
func zlibCompressing(chunkSize int, dst *bytes.Buffer, src *bytes.Buffer) error {
	originalLen := src.Len()
	srcBytes := src.Bytes()

	compressorBuf := bytes.NewBuffer(make([]byte, chunkSize))
	// default level
	compressor, err := flate.NewWriter(compressorBuf, -1)
	if err != nil {
		return errors.WithStack(err)
	}
	if _, err := src.WriteTo(compressor); err != nil {
		return errors.WithStack(err)
	}
	if err := compressor.Close(); err != nil {
		return errors.WithStack(err)
	}
	compressedBytes := compressorBuf.Bytes()
	remaining := compressorBuf.Len()

	orig := compressorBuf.Len() >= originalLen

	var start int

	for ; remaining > chunkSize; {

		header := encChunkHeader(chunkSize, orig)
		if _, err = dst.Write(header); err != nil {
			return errors.WithStack(err)
		}

		if orig {
			log.Tracef("stream writer write original data %d at %d", chunkSize, start)
			if _, err = dst.Write(srcBytes[start : start+chunkSize]); err != nil {
				return errors.WithStack(err)
			}
		} else {
			log.Tracef("stream writer write data %d at %d", chunkSize, start)
			if _, err = dst.Write(compressedBytes[start : start+chunkSize]); err != nil {
				return errors.WithStack(err)
			}
		}

		start += chunkSize
		remaining -= chunkSize
	}

	header := encChunkHeader(remaining, orig)
	if _, err = dst.Write(header); err != nil {
		return errors.WithStack(err)
	}

	if orig {
		if _, err = dst.Write(srcBytes[start : start+remaining]); err != nil {
			return errors.WithStack(err)
		}
	} else {
		if _, err = dst.Write(compressedBytes[start : start+remaining]); err != nil {
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

func newPresentStream(id uint32, opts *WriterOptions) *valuesWriter {
	k := pb.Stream_PRESENT
	sw := &streamWriter{info: &pb.Stream{Kind: &k, Column: &id}, opts: opts}
	return &valuesWriter{s: sw, valueBuf: &bytes.Buffer{}, encoder: &encoding.BoolRunLength{}}
}

func newBoolDataStream(id uint32, opts *WriterOptions) *valuesWriter {
	k := pb.Stream_DATA
	sw := &streamWriter{info: &pb.Stream{Kind: &k, Column: &id}, buf: &bytes.Buffer{}, opts: opts}
	encoder := &encoding.BoolRunLength{}
	return &valuesWriter{s: sw, valueBuf: &bytes.Buffer{}, encoder: encoder}
}

func newByteStream(id uint32, kind pb.Stream_Kind, opts *WriterOptions) *valuesWriter {
	sw := &streamWriter{info: &pb.Stream{Kind: &kind, Column: &id}, buf: &bytes.Buffer{}, opts: opts}
	encoder := &encoding.ByteRunLength{}
	return &valuesWriter{s: sw, valueBuf: &bytes.Buffer{}, encoder: encoder}
}

func newIntDataStreamV2(id uint32, opts *WriterOptions) *valuesWriter {
	k := pb.Stream_DATA
	sw := &streamWriter{info: &pb.Stream{Kind: &k, Column: &id}, buf: &bytes.Buffer{}, opts: opts}
	encoder := &encoding.IntRleV2{Signed: true}
	return &valuesWriter{s: sw, valueBuf: &bytes.Buffer{}, encoder: encoder}
}

func newStringStreamV2(id uint32, kind pb.Stream_Kind, opts *WriterOptions) *valuesWriter {
	sw := &streamWriter{info: &pb.Stream{Kind: &kind, Column: &id}, buf: &bytes.Buffer{}, opts: opts}
	encoder := &encoding.BytesContent{}
	return &valuesWriter{s: sw, valueBuf: &bytes.Buffer{}, encoder: encoder}
}

func newLengthStreamV2(id uint32, opts *WriterOptions) *valuesWriter {
	k := pb.Stream_LENGTH
	sw := &streamWriter{&pb.Stream{Kind: &k, Column: &id}, &bytes.Buffer{}, opts}
	encoder := &encoding.IntRleV2{Signed: false}
	return &valuesWriter{s: sw, valueBuf: &bytes.Buffer{}, encoder: encoder}
}

func newBinaryDataStreamV2(id uint32, opts *WriterOptions) *valuesWriter {
	k := pb.Stream_DATA
	sw := &streamWriter{info: &pb.Stream{Kind: &k, Column: &id}, buf: &bytes.Buffer{}, opts: opts}
	encoder := &encoding.BytesContent{}
	return &valuesWriter{s: sw, valueBuf: &bytes.Buffer{}, encoder: encoder}
}

func newSignedIntStreamV2(id uint32, kind pb.Stream_Kind, opts *WriterOptions) *valuesWriter {
	sw := &streamWriter{info: &pb.Stream{Kind: &kind, Column: &id}, buf: &bytes.Buffer{}, opts: opts}
	encoder := &encoding.IntRleV2{Signed: true}
	return &valuesWriter{s: sw, valueBuf: &bytes.Buffer{}, encoder: encoder, opts: opts}
}

func newBase128VarIntsDataStream(id uint32, opts *WriterOptions) *valuesWriter {
	k := pb.Stream_DATA
	sw := &streamWriter{info: &pb.Stream{Kind: &k, Column: &id}, buf: &bytes.Buffer{}, opts: opts}
	encoder := &encoding.Base128VarInt{}
	return &valuesWriter{s: sw, valueBuf: &bytes.Buffer{}, encoder: encoder}
}

func newUnsignedIntStreamV2(id uint32, kind pb.Stream_Kind, opts *WriterOptions) *valuesWriter {
	sw := &streamWriter{info: &pb.Stream{Kind: &kind, Column: &id}, buf: &bytes.Buffer{}, opts: opts}
	encoder := &encoding.IntRleV2{Signed: false}
	return &valuesWriter{s: sw, valueBuf: &bytes.Buffer{}, encoder: encoder}
}

func newDoubleDataStream(id uint32, opts *WriterOptions) *valuesWriter {
	k := pb.Stream_DATA
	sw := &streamWriter{info: &pb.Stream{Kind: &k, Column: &id}, buf: &bytes.Buffer{}, opts: opts}
	encoder := &encoding.Ieee754Double{}
	return &valuesWriter{s: sw, valueBuf: &bytes.Buffer{}, encoder: encoder}
}
