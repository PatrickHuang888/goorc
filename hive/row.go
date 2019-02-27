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

