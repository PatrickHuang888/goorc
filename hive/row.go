package hive

import (
	"io"

	"github.com/pkg/errors"
)

const (
	DEFAULT_ROW_SIZE = 1024
)

type VectorizedRowBatch struct {
	NumCols int  // number of columns
	Size    int  // number of rows
	Cols    []ColumnVector
}

func (vrb *VectorizedRowBatch) Write(out io.Writer) error {
	return errors.New("unsupported operation")
}

func (vrb *VectorizedRowBatch) ReadFields(in io.Reader) error {
	return errors.New("unsupported operation")
}
