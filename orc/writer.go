package orc

import (
	"bytes"
	"github.com/PatrickHuang888/goorc/pb/pb"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"os"
)

const (
	MIN_ROW_INDEX_STRIDE         = 1000
	STRIPE_LIMIT                 = 256 * 1024 * 1024
	DEFAULT_INDEX_SIZE           = 1 * 1024 * 1024
	DEFAULT_DATA_SIZE            = 10 * 1024 * 1024
	DEFAULT_ENCODING_BUFFER_SIZE = 1 * 1024 * 1024
)

type WriterOptions struct {
	schemas   []*TypeDescription
	chunkSize uint64
	cmpKind   pb.CompressionKind
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

	// current stripe and info
	stripe *stripeWriter

	stripeInfos []*pb.StripeInformation
	columnStats []*pb.ColumnStatistics

	opts *WriterOptions
}

type stripeWriter struct {
	opts *WriterOptions

	// streams <id, stream{present, data, length}>
	streams map[uint32][3]*streamWriter

	idxBuf  *bytes.Buffer // index area buffer
	dataBuf *bytes.Buffer // data area buffer

	info *pb.StripeInformation
}

type streamWriter struct {
	info     *pb.Stream
	encoding *pb.ColumnEncoding
	enc      Encoder
	buf      *bytes.Buffer
}

func NewWriter(path string, opts *WriterOptions) (Writer, error) {
	// fixme: create new one, error when exist
	f, err := os.Create(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	stp := newStripeWriter(opts)
	w := &writer{opts: opts, path: path, f: f, stripe: stp}
	n, err := w.writeHeader()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	w.offset = n
	return w, nil
}

func newStripeWriter(opts *WriterOptions) *stripeWriter {
	idxBuf := bytes.NewBuffer(make([]byte, DEFAULT_INDEX_SIZE))
	dataBuf := bytes.NewBuffer(make([]byte, DEFAULT_DATA_SIZE))
	ss := make(map[uint32][3]*streamWriter)
	si := &pb.StripeInformation{}
	stp := &stripeWriter{opts: opts, idxBuf: idxBuf, dataBuf: dataBuf, streams: ss, info: si}
	return stp
}

func (w *writer) Write(cv ColumnVector) error {
	// todo: verify cv type and column type

	stp := w.stripe
	if err := stp.write(cv); err != nil {
		return errors.WithStack(err)
	}
	// refactor: update stripe info in 1 place?
	*stp.info.NumberOfRows += uint64(cv.Rows())

	// todo: update column stats

	// fixme: assuming 1 stripe all in memory
	// a stripe should contains whole row
	if stp.needFlush() {
		if err := stp.flushStripe(w.f); err != nil {
			return errors.WithStack(err)
		}

		// reset current stripe
		w.offset += stp.info.GetOffset() + stp.info.GetIndexLength() + stp.info.GetDataLength()
		w.stripeInfos = append(w.stripeInfos, stp.info)
		stp.reset()
		*stp.info.Offset = w.offset
	}

	return nil
}

func (stp *stripeWriter) reset() {
	stp.dataBuf.Reset()
	stp.idxBuf.Reset()
	stp.info = &pb.StripeInformation{}
	for _, v := range stp.streams {
		if v[0] != nil {
			v[0].reset()
		}
		v[1].reset()
		if v[2] != nil {
			v[2].reset()
		}
	}
}

func (stp *stripeWriter) write(cv ColumnVector) error {
	switch stp.opts.schemas[cv.ColumnId()].Kind {
	case pb.Type_STRUCT:
		return errors.New("struct not impl")
	case pb.Type_INT:
		ss, ok := stp.streams[cv.ColumnId()]
		if !ok {
			ss = make([3]*streamWriter, 3)
			stp.streams[cv.ColumnId()] = ss
		}
		if ss[1] == nil {
			s := &pb.Stream{}
			*s.Kind = pb.Stream_DATA
			*s.Column = uint32(cv.ColumnId())
			b := bytes.NewBuffer(make([]byte, stp.opts.chunkSize))
			enc := &intRleV2{signed: true}
			dv2 := pb.ColumnEncoding_DIRECT_V2
			encoding := &pb.ColumnEncoding{Kind: &dv2}
			sw := &streamWriter{info: s, buf: b, encoding: encoding, enc: enc}
			stp.dataStreams[cv.ColumnId()] = sw
		}

		v, ok := cv.(*LongColumnVector)
		if !ok {
			return errors.New("column type int should be vector long")
		}
		stm.buf.Reset()
		err := stm.writeIrlV2(v)
		if err != nil {
			return errors.WithStack(err)
		}

		n, err := compressTo(stp.opts.cmpKind, stm.buf, stp.dataBuf)
		if err != nil {
			return errors.Wrap(err, "compressing data stream error")
		}
		*stm.info.Length += n

	default:
		return errors.New("no impl")
	}
	return nil
}

// 1 stripe should be self-contained
func (stp *stripeWriter) flushStripe(f *os.File) error {
	// row number updated at write
	*stp.info.IndexLength = uint64(stp.idxBuf.Len())
	*stp.info.DataLength = uint64(stp.dataBuf.Len())

	// buf will be reset after writeTo
	_, err := stp.idxBuf.WriteTo(f)
	if err != nil {
		return errors.WithStack(err)
	}
	_, err = stp.dataBuf.WriteTo(f)
	if err != nil {
		return errors.WithStack(err)
	}

	// write stripe footer
	sf := &pb.StripeFooter{}
	for _, v := range stp.idxStreams {
		sf.Streams = append(sf.Streams, v.info)
		sf.Columns = append(sf.Columns, v.encoding)
	}
	for _, v := range stp.dataStreams {
		sf.Streams = append(sf.Streams, v.info)
		sf.Columns = append(sf.Columns, v.encoding)
	}
	sfm, err := proto.Marshal(sf)
	if err != nil {
		return errors.WithStack(err)
	}
	b, err := compressByteSlice(stp.opts.cmpKind, sfm)
	if err != nil {
		return errors.WithStack(err)
	}
	*stp.info.FooterLength = uint64(len(b))
	if _, err := f.Write(b); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (stm *streamWriter) writeIrlV2(lcv *LongColumnVector) error {
	vector := lcv.GetVector()
	irl := stm.enc.(*intRleV2)
	irl.literals = vector
	irl.numLiterals = uint32(len(vector))
	if err := irl.writeValues(stm.buf); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (stm *streamWriter) reset() {
	*stm.info.Length = 0
}

func (w *writer) GetSchema() *TypeDescription {
	return w.opts.schemas[0]
}

func (w *writer) Close() error {
	if err := w.writeFileTail(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (w *writer) createRowIndexEntry() error {
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
	// metadata

	var footer *pb.Footer
	// header length

	// content length

	//footer.Stripes=
	// stripe info array

	//footer.Types=
	// types: from schema to types array

	// metadata

	w.writeFooter(footer)

	var ps *pb.PostScript
	w.writePostScript(ps)
	return nil
}

func (w *writer) writeFooter(footer *pb.Footer) error {
	w.buf.Reset()
	err := w.buf.Marshal(footer)
	if err != nil {
		return errors.Wrap(err, "marshall footer error")
	}
	_, err = compressProtoBuf(w.kind, w.buf, w.cmpBuf)
	if err != nil {
		return errors.Wrap(err, "compress buffer error")
	}
	_, err = w.cmpBuf.WriteTo(w.f)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// compress buf into 1 buffer, maybe to several chunked buffer,
// assuming all can be in memory
func compressTo(kind pb.CompressionKind, src *bytes.Buffer, dst *bytes.Buffer) (cmpLength uint64, err error) {
	return nil
}

func compressByteSlice(kind pb.CompressionKind, b []byte) ([]byte, error) {

}

func compressProtoBuf(kind pb.CompressionKind, src *proto.Buffer, dst *bytes.Buffer) (uint64, error) {
	return 0, nil
}

func (w *writer) writePostScript(ps *pb.PostScript) error {
	bs, err := proto.Marshal(ps)
	if err != nil {
		return errors.WithStack(err)
	}
	n, err := w.f.Write(bs)
	if err != nil {
		return errors.Wrap(err, "write PS error")
	}
	// last byte is ps length
	if _, err = w.f.Write([]byte{byte(n)}); err != nil {
		return errors.Wrap(err, "write PS length error")
	}
	return nil
}

type PhysicalWriter interface {
	WriteHeader() error
	WriteIndex(index pb.RowIndex) error
	WriteBloomFilter(bloom pb.BloomFilterIndex) error
	FinalizeStripe(footer pb.StripeFooter, dirEntry pb.StripeInformation) error
	WriteFileMetadata(metadate pb.Metadata) error
	WriteFileFooter(footer pb.Footer) error
	WritePostScript(ps pb.PostScript) error
	Close() error
	Flush() error
}

type physicalFsWriter struct {
	path string
	opts *WriterOptions
	f    *os.File
}

func (w *physicalFsWriter) WriteHeader() (err error) {
	if _, err = w.f.Write([]byte(MAGIC)); err != nil {
		return errors.Wrapf(err, "write header error")
	}
	return err
}

func (*physicalFsWriter) WriteIndex(index pb.RowIndex) error {
	panic("implement me")
}

func (*physicalFsWriter) WriteBloomFilter(bloom pb.BloomFilterIndex) error {
	panic("implement me")
}

func (*physicalFsWriter) FinalizeStripe(footer pb.StripeFooter, dirEntry pb.StripeInformation) error {
	panic("implement me")
}

func (*physicalFsWriter) WriteFileMetadata(metadate pb.Metadata) error {
	panic("implement me")
}

func (*physicalFsWriter) WriteFileFooter(footer pb.Footer) error {
	panic("implement me")
}

func (*physicalFsWriter) WritePostScript(ps pb.PostScript) error {
	panic("implement me")
}

func (*physicalFsWriter) Close() error {
	panic("implement me")
}

func (*physicalFsWriter) Flush() error {
	panic("implement me")
}
