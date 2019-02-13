package orc

import (
	"github.com/PatrickHuang888/goorc/hive"
)

type WriterOptions struct {
	Schema *TypeDescription
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
	path         string
	schema       *TypeDescription
	buildIndex   bool
	rowsInStripe int64
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

func NewWriter(path string, opts WriterOptions) (Writer, error) {
	return &writer{schema: opts.Schema}, nil
}
