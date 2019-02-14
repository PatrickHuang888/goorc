package orc

import (
	"github.com/PatrickHuang888/goorc/hive"
	"github.com/PatrickHuang888/goorc/pb/pb"
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

	AddRowBatch(batch *hive.VectorizedRowBatch) error

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
}

func (w *writer) GetSchema() *TypeDescription {
	return w.schema
}

func (w *writer) AddRowBatch(batch *hive.VectorizedRowBatch) error {
	if w.buildIndex {

	} else {
		w.rowsInStripe += batch.Size
	}
}

func (w *writer) Close() error {

}

func (w *writer) flushStrip() error {
	if w.buildIndex && w.rowsInIndex != 0 {
		w.createRowIndexEntry()
	}
}

func (w *writer) createRowIndexEntry() error {
	
	w.rowsInIndex= 0
}

func NewWriter(path string, opts *WriterOptions) (Writer, error) {
	buildIndex := opts.RowIndexStride > 0
	pw := &physicalFsWriter{path: path, opts: opts}
	pw.WriteHeader()

	if buildIndex && opts.RowIndexStride < MIN_ROW_INDEX_STRIDE {
		return nil, errors.Errorf("row stride must be at least %d", MIN_ROW_INDEX_STRIDE)
	}

	return &writer{schema: opts.Schema, pw: pw}, nil
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
