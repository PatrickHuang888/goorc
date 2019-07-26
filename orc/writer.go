package orc

import (
	"bytes"
	"compress/flate"
	"github.com/PatrickHuang888/goorc/pb/pb"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"math"
	"os"
)

const (
	MIN_ROW_INDEX_STRIDE         = 1000
	STRIPE_LIMIT                 = 256 * 1024 * 1024
	DEFAULT_INDEX_SIZE           = 100 * 1024
	DEFAULT_PRESENT_SIZE         = 100 * 1024
	DEFAULT_DATA_SIZE            = 1 * 1024 * 1024
	DEFAULT_LENGTH_SIZE          = 100 * 1024
	DEFAULT_ENCODING_BUFFER_SIZE = 100 * 1024
	DEFAULT_CHUNK_SIZE           = 256 * 1024
	MAX_CHUNK_LENGTH             = uint64(32768) // 15 bit
)

type WriterOptions struct {
	chunkSize  uint64
	cmpKind    pb.CompressionKind
	stripeSize int
	RowSize int
}

func DefaultWriterOptions() *WriterOptions {
	o := &WriterOptions{}
	o.cmpKind = pb.CompressionKind_ZLIB
	o.stripeSize = STRIPE_LIMIT
	o.chunkSize = DEFAULT_CHUNK_SIZE
	return o
}

type Writer interface {
	GetSchema() *TypeDescription

	Write(batch ColumnVector) error

	Close() error
}

// cannot used concurrently, not synchronized
// strip buffered in memory until the strip size
// write out by columns
type writer struct {
	path   string
	f      *os.File
	offset uint64

	ps *pb.PostScript

	currentStripe *stripeWriter

	stripeInfos []*pb.StripeInformation
	columnStats []*pb.ColumnStatistics

	schemas []*TypeDescription
	opts    *WriterOptions
}

type stripeWriter struct {
	schemas []*TypeDescription
	opts    *WriterOptions

	// streams <id, stream{present, data, length}>
	streams map[uint32][]*streamWriter

	idxBuf *bytes.Buffer // index area buffer
	//dataBuf *bytes.Buffer // data area buffer

	info *pb.StripeInformation
}

func NewWriter(path string, schema *TypeDescription, opts *WriterOptions) (Writer, error) {
	// fixme: create new one, error when exist
	log.Infof("open %s", path)
	f, err := os.Create(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	w := &writer{opts: opts, path: path, f: f, schemas: schema.normalize()}
	n, err := w.writeHeader()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	w.offset = n
	return w, nil
}

func newStripeWriter(offset uint64, schemas []*TypeDescription, opts *WriterOptions) *stripeWriter {
	idxBuf := bytes.NewBuffer(make([]byte, DEFAULT_INDEX_SIZE))
	idxBuf.Reset()
	ss := make(map[uint32][]*streamWriter)
	si := &pb.StripeInformation{}
	var r uint64
	si.NumberOfRows = &r
	o := offset
	si.Offset = &o
	stp := &stripeWriter{opts: opts, idxBuf: idxBuf, streams: ss, info: si, schemas: schemas}
	return stp
}

func (w *writer) Write(cv ColumnVector) error {
	stripe := w.currentStripe
	if stripe == nil {
		stripe = newStripeWriter(w.offset, w.schemas, w.opts)
		w.currentStripe = stripe
	}
	if err := stripe.write(cv); err != nil {
		return errors.WithStack(err)
	}
	if err := w.flushStripe(false); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (w *writer) flushStripe(force bool) error {
	// fixme: assuming 1 currentStripe all in memory
	// a currentStripe should contains whole row
	stripe := w.currentStripe
	if stripe.shouldFlush() || force {
		if err := stripe.flush(w.f); err != nil {
			return errors.WithStack(err)
		}
		// todo: update column stats
		// reset current currentStripe
		w.offset += stripe.info.GetOffset() + stripe.info.GetIndexLength() + stripe.info.GetDataLength()
		w.stripeInfos = append(w.stripeInfos, stripe.info)
		log.Debugf("flushed currentStripe %v", stripe.info)
		stripe = newStripeWriter(w.offset, w.schemas, w.opts)
		w.currentStripe = stripe
	}
	return nil
}

func (stripe *stripeWriter) shouldFlush() bool {
	var l uint64
	for _, td := range stripe.schemas {
		for _, s := range stripe.streams[td.Id] {
			if s != nil {
				l += uint64(s.compressedBuf.Len())
			}
		}
	}
	return l >= stripe.opts.chunkSize
}

// should not change columnVector when write started!
func (stripe *stripeWriter) write(cv ColumnVector) error {
	id := cv.ColumnId()
	encoding := stripe.columnEncoding(id)

	streams := stripe.streams[id]
	if streams == nil {
		streams = make([]*streamWriter, 4)
		stripe.streams[id] = streams
	}
	present := stripe.streams[id][0]
	data := stripe.streams[id][1]

	if cv.HasNulls() {
		if present == nil {
			present = newPresentStream(id, encoding)
			stripe.streams[id][0] = present
		}
		if err := present.writeBools(cv.presents()); err != nil {
			return errors.WithStack(err)
		}
	}

	switch stripe.schemas[id].Kind {
	case pb.Type_BOOLEAN:
		column := cv.(*BoolColumn)
		if data == nil {
			data = newBoolDataStream(id)
			stripe.streams[id][1] = data
		}
		var vector []bool
		if column.HasNulls() {
			for i, v := range column.Vector {
				if !column.Nulls[i] {
					vector = append(vector, v)
				}
			}
		} else {
			vector = column.Vector
		}
		if err := data.writeBools(vector); err != nil {
			return errors.WithStack(err)
		}

	case pb.Type_BYTE:
		column := cv.(*TinyIntColumn)
		if data == nil {
			data = newByteDataStream(id)
			stripe.streams[id][1] = data
		}
		var vector []byte
		if column.HasNulls() { // toAssure: using copy when hasNulls
			var vector []byte
			for i, p := range column.Nulls {
				if !p {
					vector = append(vector, column.Vector[i])
				}
			}
		} else {
			vector = column.Vector
		}
		if err := data.writeBytes(vector); err != nil {
			return errors.WithStack(err)
		}

	case pb.Type_SHORT:
		fallthrough
	case pb.Type_INT:
		fallthrough
	case pb.Type_LONG:
		column := cv.(*LongColumn)

		var vector []int64
		if column.HasNulls() {
			for i, p := range column.Nulls {
				if !p {
					vector = append(vector, int64(column.Vector[i]))
				}
			}
		} else {
			vector = column.Vector
		}

		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			if data == nil {
				data = newIntDataStreamV2(id)
				stripe.streams[id][1] = data
			}
			if err := data.writeLongsV2(vector); err != nil {
				return errors.WithStack(err)
			}
			break
		}

		return errors.Errorf("writing encoding %s for int64 not impl",
			pb.ColumnEncoding_Kind_name[int32(encoding)])

	case pb.Type_STRING:
		column := cv.(*StringColumn)

		var lengthVector []uint64
		var contents [][]byte
		if column.hasNulls {
			for i, n := range column.Nulls {
				if !n {
					s := column.Vector[i]
					contents = append(contents, []byte(s))              // convert into bytes encoding utf-8
					lengthVector = append(lengthVector, uint64(len(s))) // len return bytes size
				}
			}
		} else {
			for _, s := range column.Vector {
				contents = append(contents, []byte(s))
				lengthVector = append(lengthVector, uint64(len(s)))
			}
		}

		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			if data == nil {
				data = newStringDataStreamV2(id)
				stripe.streams[id][1] = data
			}
			if err := data.writeBytesDirectV2(contents); err != nil {
				return errors.WithStack(err)
			}

			lengthStream := stripe.streams[id][2]
			if lengthStream == nil {
				lengthStream = newLengthStreamV2(id)
				stripe.streams[cv.ColumnId()][2] = lengthStream
			}
			if err := lengthStream.writeULongsV2(lengthVector); err != nil {
				return errors.WithStack(err)
			}
			break
		}

		return errors.Errorf("writing encoding %s for string not impl",
			pb.ColumnEncoding_Kind_name[int32(encoding)])

	case pb.Type_BINARY:
		column := cv.(*BinaryColumn)

		var vector [][]byte
		var lengthVector []uint64
		if column.hasNulls {
			for i, n := range column.Nulls {
				if !n {
					vector = append(vector, column.Vector[i])
					lengthVector = append(lengthVector, uint64(len(column.Vector[i])))
				}
			}
		} else {
			vector = column.Vector
		}

		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			if data == nil {
				data = newBinaryDataStreamV2(id)
				stripe.streams[id][1] = data
				if err := data.writeBytesDirectV2(vector); err != nil {
					return errors.WithStack(err)
				}
			}

			lengthStream := stripe.streams[id][2]
			if lengthStream == nil {
				lengthStream = newLengthStreamV2(id)
				stripe.streams[cv.ColumnId()][2] = lengthStream
			}
			if err := lengthStream.writeULongsV2(lengthVector); err != nil {
				return errors.WithStack(err)
			}
			break
		}

		return errors.Errorf("writing encoding %s for binary not impl",
			pb.ColumnEncoding_Kind_name[int32(encoding)])

	case pb.Type_DATE:
		column := cv.(*DateColumn)
		var vector []int64

		if cv.HasNulls() {
			for i, b := range column.Nulls {
				if !b {
					vector = append(vector, toDays(column.Vector[i]))
				}
			}
		} else {
			for _, d := range column.Vector {
				vector = append(vector, toDays(d))
			}
		}

		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			if data == nil {
				data = newSignedIntStreamV2(id, pb.Stream_DATA)
				stripe.streams[id][1] = data
			}
			if err := data.writeLongsV2(vector); err != nil {
				return errors.WithStack(err)
			}
			break
		}

		return errors.Errorf("writing encoding %s for date not impl",
			pb.ColumnEncoding_Kind_name[int32(encoding)])

	case pb.Type_TIMESTAMP:
		/*column := cv.(*TimestampColumn)

		var seconds []int64
		var nanos []uint64

		if column.hasNulls {
			for i, b := range column.Nulls {
				if !b {
					s, n := column.Vector[i].getSecondsAndNanos()
					seconds = append(seconds, s)
					nanos = append(nanos, uint64(n))
				}
			}
		} else {
			for _, t := range column.Vector {
				s, n := t.getSecondsAndNanos()
				seconds = append(seconds, s)
				nanos = append(nanos, uint64(n))
			}
		}

		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			if data == nil {
				data = newSignedIntStreamV2(id, pb.Stream_DATA)
				stripe.streams[id][1] = data
			}
			if err := data.writeLongsV2(seconds); err != nil {
				return errors.WithStack(err)
			}

			secondary := stripe.streams[id][2]
			if secondary == nil {
				secondary = newUnsignedIntStreamV2(id, pb.Stream_SECONDARY)
				stripe.streams[id][2] = secondary
			}
			if err := secondary.writeULongsV2(nanos); err != nil {
				return errors.WithStack(err)
			}
			break
		}*/

		return errors.Errorf("writing encoding %s for timestamp not impl",
			pb.ColumnEncoding_Kind_name[int32(encoding)])

	case pb.Type_STRUCT:
		column := cv.(*StructColumn)
		for _, c := range column.Fields {
			if err := stripe.write(c); err != nil {
				return errors.WithStack(err)
			}
		}

	default:
		return errors.New("no impl")
	}

	for _, s := range streams {
		if s != nil {
			if err := s.compress(stripe.opts.cmpKind, stripe.opts.chunkSize); err != nil {
				return errors.WithStack(err)
			}
		}
	}

	*stripe.info.NumberOfRows += uint64(cv.Rows())
	return nil
}

func (stripe *stripeWriter) columnEncoding(id uint32) pb.ColumnEncoding_Kind {
	switch stripe.schemas[id].Kind {
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
func (stripe *stripeWriter) flush(f *os.File) error {
	// row number updated at write
	idxLength := uint64(stripe.idxBuf.Len())
	// buf will be reset after writeTo
	_, err := stripe.idxBuf.WriteTo(f)
	if err != nil {
		return errors.WithStack(err)
	}
	log.Debugf("flush index with length %d", idxLength)

	var dataL uint64
	for _, td := range stripe.schemas {
		for _, s := range stripe.streams[td.Id] {
			if s != nil {
				*s.info.Length = uint64(s.compressedBuf.Len())
				log.Tracef("flush stream %s of column %d length %d", s.info.GetKind().String(),
					s.info.GetColumn(), *s.info.Length)
				n, err := s.compressedBuf.WriteTo(f)
				if err != nil {
					return errors.WithStack(err)
				}
				dataL += uint64(n)
			}
		}
	}

	// stripe footer
	footer := &pb.StripeFooter{}
	for _, schema := range stripe.schemas {
		for _, s := range stripe.streams[schema.Id] {
			if s != nil {
				footer.Streams = append(footer.Streams, s.info)
				footer.Columns = append(footer.Columns, s.encoding)
			}
		}
	}
	mf, err := proto.Marshal(footer)
	if err != nil {
		return errors.WithStack(err)
	}
	cmf, err := compressByteSlice(stripe.opts.cmpKind, stripe.opts.chunkSize, mf)
	if err != nil {
		return errors.WithStack(err)
	}
	ftLength := uint64(len(cmf))
	if _, err := f.Write(cmf); err != nil {
		return errors.WithStack(err)
	}
	log.Debugf("flush stripe footer with length: %d", ftLength)

	stripe.info.IndexLength = &idxLength
	stripe.info.DataLength = &dataL
	stripe.info.FooterLength = &ftLength

	return nil
}

type streamWriter struct {
	info          *pb.Stream
	encoding      *pb.ColumnEncoding
	encoder       Encoder
	buf           *bytes.Buffer
	compressedBuf *bytes.Buffer
}

func (s *streamWriter) writeBools(bb []bool) error {
	enc := s.encoder.(*boolRunLength)
	enc.bools = bb
	if err := enc.writeValues(s.buf); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (s *streamWriter) writeBytes(bb []byte) error {
	enc := s.encoder.(*byteRunLength)
	enc.literals = bb
	if err := enc.writeValues(s.buf); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (s *streamWriter) writeULongsV2(v []uint64) error {
	irl := s.encoder.(*intRleV2)
	irl.uliterals = v
	if err := irl.writeValues(s.buf); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (s *streamWriter) writeLongsV2(v []int64) error {
	irl := s.encoder.(*intRleV2)
	irl.literals = v
	if err := irl.writeValues(s.buf); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (s *streamWriter) writeBytesDirectV2(bs [][]byte) error {
	enc := s.encoder.(*bytesDirectV2)
	enc.content = bs
	if err := enc.writeValues(s.buf); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (s *streamWriter) compress(cmpKind pb.CompressionKind, chunkSize uint64) error {
	switch cmpKind {
	case pb.CompressionKind_ZLIB:
		if _, err := compressZlibTo(cmpKind, chunkSize, s.compressedBuf, s.buf);
			err != nil {
			return errors.Wrap(err, "compress bool present stream error")
		}
	default:
		return errors.New("compress other than ZLIB not impl")
	}
	return nil
}

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
	// write footer
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
	ftCmpBuf, err := compressByteSlice(w.opts.cmpKind, w.opts.chunkSize, ftb)
	if err != nil {
		return errors.WithStack(err)
	}
	ftl := uint64(len(ftCmpBuf))
	if _, err := w.f.Write(ftCmpBuf); err != nil {
		return errors.WithStack(err)
	}
	log.Debugf("write file footer with length: %d", ftl)

	// write postscript
	ps := &pb.PostScript{}
	ps.FooterLength = &ftl
	ps.Compression = &w.opts.cmpKind
	ps.CompressionBlockSize = &w.opts.chunkSize
	m := MAGIC
	ps.Magic = &m
	psb, err := proto.Marshal(ps)
	if err != nil {
		return errors.WithStack(err)
	}
	n, err := w.f.Write(psb)
	if err != nil {
		return errors.Wrap(err, "write PS error")
	}
	log.Debugf("write postscript with length %d", n)
	// last byte is ps length
	if _, err = w.f.Write([]byte{byte(n)}); err != nil {
		return errors.Wrap(err, "write PS length error")
	}

	return nil
}

// compress src buf into dst, maybe to several chunks
func compressZlibTo(kind pb.CompressionKind, chunkSize uint64, dst *bytes.Buffer, src *bytes.Buffer) (cmpLength int64,
	err error) {

	srcBytes := src.Bytes()
	chunkLength := MinUint64(MAX_CHUNK_LENGTH, chunkSize)
	buf := bytes.NewBuffer(make([]byte, chunkLength))
	buf.Reset()
	w, err := flate.NewWriter(buf, -1)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	srcLen := src.Len()
	if uint64(srcLen) < chunkLength {
		if _, err := src.WriteTo(w); err != nil {
			return 0, errors.WithStack(err)
		}
		if err = w.Close(); err != nil {
			return 0, errors.WithStack(err)
		}
		var header []byte
		orig := buf.Len() >= srcLen
		if orig {
			header = encChunkHeader(srcLen, orig)
		} else {
			header = encChunkHeader(buf.Len(), orig)
		}
		if _, err = dst.Write(header); err != nil {
			return 0, err
		}
		if orig {
			if n, err := dst.Write(srcBytes); err != nil {
				return int64(n), errors.WithStack(err)
			}
		} else {
			if n, err := buf.WriteTo(dst); err != nil {
				return n, errors.WithStack(err)
			}
		}

	} else {
		// todo: several chunk
		return 0, errors.New("no impl")
	}
	return
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
func compressByteSlice(kind pb.CompressionKind, chunkSize uint64, b []byte) (compressed []byte, err error) {
	switch kind {
	case pb.CompressionKind_ZLIB:
		src := bytes.NewBuffer(b)
		dst := bytes.NewBuffer(make([]byte, len(b)))
		dst.Reset()
		if _, err = compressZlibTo(kind, chunkSize, dst, src); err != nil {
			return nil, err
		}
		return dst.Bytes(), nil

	default:
		return nil, errors.New("compression other than zlib not impl")
	}
	return
}

func newPresentStream(id uint32, encoding pb.ColumnEncoding_Kind) *streamWriter {
	k := pb.Stream_PRESENT
	info := &pb.Stream{Kind: &k, Column: &id, Length: new(uint64)}
	buf := &bytes.Buffer{}
	buf.Reset()
	cb := &bytes.Buffer{}
	cb.Reset()
	enc := &boolRunLength{}
	e := &pb.ColumnEncoding{Kind: &encoding}
	return &streamWriter{info: info, buf: buf, compressedBuf: cb, encoding: e, encoder: enc}
}

func newBoolDataStream(id uint32) *streamWriter {
	k := pb.Stream_DATA
	info := &pb.Stream{Kind: &k, Column: &id, Length: new(uint64)}
	buf := &bytes.Buffer{}
	buf.Reset()
	cb := &bytes.Buffer{}
	cb.Reset()
	enc := &boolRunLength{}
	ce := pb.ColumnEncoding_DIRECT
	e := &pb.ColumnEncoding{Kind: &ce}
	return &streamWriter{info: info, buf: buf, compressedBuf: cb, encoding: e, encoder: enc}
}

func newByteDataStream(id uint32) *streamWriter {
	k := pb.Stream_DATA
	info := &pb.Stream{Kind: &k, Column: &id, Length: new(uint64)}
	buf := &bytes.Buffer{}
	buf.Reset()
	cb := &bytes.Buffer{}
	cb.Reset()
	enc := &byteRunLength{}
	ce := pb.ColumnEncoding_DIRECT
	e := &pb.ColumnEncoding{Kind: &ce}
	return &streamWriter{info: info, buf: buf, compressedBuf: cb, encoding: e, encoder: enc}
}

func newIntDataStreamV2(id uint32) *streamWriter {
	k := pb.Stream_DATA
	info := &pb.Stream{Kind: &k, Column: &id, Length: new(uint64)}
	buf := &bytes.Buffer{}
	buf.Reset()
	cb := &bytes.Buffer{}
	cb.Reset()
	enc := &intRleV2{}
	enc.signed = true
	ce := pb.ColumnEncoding_DIRECT_V2
	e := &pb.ColumnEncoding{Kind: &ce}
	return &streamWriter{info: info, buf: buf, compressedBuf: cb, encoding: e, encoder: enc}
}

func newStringDataStreamV2(id uint32) *streamWriter {
	k := pb.Stream_DATA
	info := &pb.Stream{Kind: &k, Column: &id, Length: new(uint64)}
	buf := &bytes.Buffer{}
	buf.Reset()
	cb := &bytes.Buffer{}
	cb.Reset()
	enc := &bytesDirectV2{}
	ce := pb.ColumnEncoding_DIRECT_V2
	e := &pb.ColumnEncoding{Kind: &ce}
	return &streamWriter{info: info, buf: buf, compressedBuf: cb, encoding: e, encoder: enc}
}

func newLengthStreamV2(id uint32) *streamWriter {
	k := pb.Stream_LENGTH
	info := &pb.Stream{Kind: &k, Column: &id, Length: new(uint64)}
	buf := &bytes.Buffer{}
	buf.Reset()
	cb := &bytes.Buffer{}
	cb.Reset()
	enc := &intRleV2{}
	dv2 := pb.ColumnEncoding_DIRECT_V2
	encoding := &pb.ColumnEncoding{Kind: &dv2}
	return &streamWriter{info: info, buf: buf, compressedBuf: cb, encoding: encoding, encoder: enc}
}

func newBinaryDataStreamV2(id uint32) *streamWriter {
	k := pb.Stream_DATA
	info := &pb.Stream{Kind: &k, Column: &id, Length: new(uint64)}
	buf := &bytes.Buffer{}
	buf.Reset()
	cb := &bytes.Buffer{}
	cb.Reset()
	enc := &bytesDirectV2{}
	ce := pb.ColumnEncoding_DIRECT_V2
	e := &pb.ColumnEncoding{Kind: &ce}
	return &streamWriter{info: info, buf: buf, compressedBuf: cb, encoding: e, encoder: enc}
}

func newSignedIntStreamV2(id uint32, kind pb.Stream_Kind) *streamWriter {
	info := &pb.Stream{Kind: &kind, Column: &id, Length: new(uint64)}
	buf := &bytes.Buffer{}
	buf.Reset()
	cb := &bytes.Buffer{}
	cb.Reset()
	enc := &intRleV2{}
	enc.signed = true
	ce := pb.ColumnEncoding_DIRECT_V2
	e := &pb.ColumnEncoding{Kind: &ce}
	return &streamWriter{info: info, buf: buf, compressedBuf: cb, encoding: e, encoder: enc}
}

func newUnsignedIntStreamV2(id uint32, kind pb.Stream_Kind) *streamWriter {
	info := &pb.Stream{Kind: &kind, Column: &id, Length: new(uint64)}
	buf := &bytes.Buffer{}
	buf.Reset()
	cb := &bytes.Buffer{}
	cb.Reset()
	enc := &intRleV2{}
	enc.signed = false
	ce := pb.ColumnEncoding_DIRECT_V2
	e := &pb.ColumnEncoding{Kind: &ce}
	return &streamWriter{info: info, buf: buf, compressedBuf: cb, encoding: e, encoder: enc}
}

func encodingNano(nano uint64) (encoded uint64) {
	t := trailingZeros(nano)
	if t > 2 {
		n := uint64(float64(nano) / math.Pow10(t))
		encoded = n<<3 | uint64(t-2)
	} else {
		return nano
	}
	return
}

func decodingNano(encoded uint64) (nano uint64) {
	return 0
}

func trailingZeros(x uint64) (count int) {
	for int(math.Mod(float64(x), 10)) == 0 {
		count++
		x /= 10
	}
	return
}
