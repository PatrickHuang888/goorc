package hive

import (
	"io"

	"github.com/pkg/errors"
)

const (
	VectorizedRowBatch_DEFAULT_SIZE = 1024
)

type VectorizedRowBatch struct {
	NumCols int
	Size    int
	Cols    []ColumnVector
}

func (vrb *VectorizedRowBatch) Write(out io.Writer) error {
	return errors.New("unsupported operation")
}

func (vrb *VectorizedRowBatch) ReadFields(in io.Reader) error {
	return errors.New("unsupported operation")
}
