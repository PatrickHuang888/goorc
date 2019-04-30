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
}

func (w *writer) GetSchema() *TypeDescription {
	return w.schema
}

func (w *writer) AddRowBatch(batch ColumnVector) error {
	if w.buildIndex {

	} else {

	}
	return nil
}

func (w *writer) Close() error {

	return nil
}

func (w *writer) flushStrip() error {
	if w.buildIndex && w.rowsInIndex != 0 {
		w.createRowIndexEntry()
	}
	return nil
}

func (w *writer) createRowIndexEntry() error {

	w.rowsInIndex = 0
	return nil
}

func (w *writer) writeFileTail(tail *pb.FileTail) error {

}

func (w *writer) writeFooter(footer *pb.Footer) error {
	w.buf.Reset()
	err := w.buf.Marshal(footer)
	if err != nil {
		return errors.Wrap(err, "marshall footer error")
	}
	compress(w.kind, w.buf, w.cmpBuf)
}

// compress buf into chunked buffer, into 1 buffer, assuming all can be in memory
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
	// fixme: truncate if exist?
	f, err := os.Create(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &writer{schema: schema, path: path, f: f}, nil
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
