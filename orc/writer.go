package orc

import (
	"bytes"
	"github.com/PatrickHuang888/goorc/pb/pb"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"os"
)

const (
	MIN_ROW_INDEX_STRIDE = 1000
)

type WriterOptions struct {
	Schema         *TypeDescription
	RowIndexStride int
}

type Writer interface {
	GetSchema() *TypeDescription

	AddRowBatch(batch ColumnVector) error

	Close() error
}

// cannot used concurrently, not synchronized
// strip buffered in memory until the strip size
// write out by columns
type writer struct {
	path           string
	schema         *TypeDescription
	buildIndex     bool
	rowsInStripe   int64
	pw             PhysicalWriter
	rowIndexStride int
	rowsInIndex    int
	f              *os.File
	buf            *proto.Buffer // chunk data buffer
	cmpBuf         *bytes.Buffer // compressed chunk buffer
	kind           pb.CompressionKind
	chunkSize      uint64

	stripeBuf *bytes.Buffer

	ps      *pb.PostScript
	sw      *stripeWriter
	stpinfo *pb.StripeInformation
	stpstat *pb.StripeStatistics
}

type stripeWriter struct {
	sw *streamWriter

	info  *pb.StripeInformation
	stats *pb.StripeStatistics
}

type streamWriter struct {
	td       *TypeDescription
	info     *pb.Stream
	encoding *pb.ColumnEncoding
	enc      Encoder
	writeBuf *proto.Buffer
	cmpBuf   *bytes.Buffer
}

func (w *writer) write(cv ColumnVector) error {
	// fixme: no multiple stripe right now
	if err := w.sw.write(cv, w.stripeBuf); err != nil {
		return errors.WithStack(err)
	}

	if (w.stripeBuf.Len()> STRIPE_LIMIT) {
		w.flushStrip()
	}

	return nil
}

func (writer *stripeWriter) write(cv ColumnVector) error {
	switch cv.T() {
	case pb.Type_STRUCT:
		return errors.New("struct not impl")
	case pb.Type_INT:
		// todo: write present stream

		// write data stream
		if err := writer.sw.writeData(cv); err != nil {
			return errors.WithStack(err)
		}
	default:
		return errors.New("stripe write other than int not impl")
	}
	return nil
}

// write data stream
func (sw *streamWriter) writeData(cv ColumnVector) error {
	switch sw.td.Kind {
	case pb.Type_INT:
		if err := sw.writeLongData(cv.(*LongColumnVector)); err != nil {
			return errors.WithStack(err)
		}
	default:
		return errors.New("write data sream other than int not impl")
	}

	if err := compress(pb.CompressionKind_ZLIB, sw.writeBuf, sw.cmpBuf); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

// write long vector data stream
func (sw *streamWriter) writeLongData(vector *LongColumnVector) error {
	switch sw.encoding.GetKind() {
	case pb.ColumnEncoding_DIRECT_V2:
		return sw.writeIrlV2(vector)
	default:
		return errors.New("write int encoding other than direct v2 not impl")
	}
	return nil
}

func (sw *streamWriter) writeIrlV2(lcv *LongColumnVector) error {
	if sw.enc == nil {
		sw.enc = &intRleV2{signed: true}
	}
	// todo: write present stream
	vector := lcv.GetVector()
	irl := sw.enc.(*intRleV2)
	irl.literals = vector
	irl.numLiterals = uint32(len(vector))
	irl.writeValues(sw.writeBuf)
	return nil
}

func (w *writer) GetSchema() *TypeDescription {
	return w.schema
}

func (w *writer) AddRowBatch(batch ColumnVector) error {
	switch batch.T() {
	case pb.Type_INT:
		bch := batch.(*LongColumnVector)
		bch.GetVector()
	default:
		return errors.New("type other than int not impl")
	}
	return nil
}

func (w *writer) Close() error {
	if err := w.writeFileTail(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (w *writer) flushStrip() error {
	// stripeBuf will be reset after writeTo
	_, err := w.stripeBuf.WriteTo(w.f)
	if err != nil {
		return errors.WithStack(err)
	}
	w.sw

	return nil
}

func (w *writer) createRowIndexEntry() error {

	w.rowsInIndex = 0
	return nil
}

func (w *writer) writeHeader() error {
	if _, err := w.f.Write([]byte(MAGIC)); err != nil {
		return errors.Wrap(err, "write header errror")
	}
	return nil
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
	err = compress(w.kind, w.buf, w.cmpBuf)
	if err != nil {
		return errors.Wrap(err, "compress buffer error")
	}
	_, err = w.cmpBuf.WriteTo(w.f)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// compress buf into 1 buffer may contain several chunked buffer,
// assuming all can be in memory
func compress(kind pb.CompressionKind, buf *proto.Buffer, cmpBuf *bytes.Buffer) error {

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

func NewWriter(path string, schema *TypeDescription) (Writer, error) {
	// fixme: create new one, error when exist
	f, err := os.Create(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	w := &writer{schema: schema, path: path, f: f}
	if err = w.writeHeader(); err != nil {
		return nil, errors.WithStack(err)
	}
	return w, nil
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
