package hive

import "github.com/PatrickHuang888/goorc/pb/pb"

const (
	DEFAULT_ROW_SIZE = 1024
)

type VectorizedRowBatch struct {
	NumCols int // number of columns
	Size    int // number of rows
	Cols    []ColumnVector
}

type ColumnVector interface {
	T() pb.Type_Kind
}

// nullable int column vector for all integer types
type LongColumnVector struct {
	Vector []int64
	Repeating bool
}

func (*LongColumnVector) T() pb.Type_Kind {
	return pb.Type_LONG
}

type TimestampColumnVector struct {
	Vector []uint64
}

func (*TimestampColumnVector) T() pb.Type_Kind {
	return pb.Type_TIMESTAMP
}

type DoubleColumnVector struct {
	Vector []float64
}

func (*DoubleColumnVector) T() pb.Type_Kind {
	return pb.Type_DOUBLE
}

type BytesColumnVector struct {
	Vector [][]byte
}

func (*BytesColumnVector) T() pb.Type_Kind {
	return pb.Type_VARCHAR
}

type StructColumnVector struct {
	Fields []ColumnVector
}

func (*StructColumnVector) T() pb.Type_Kind {
	return pb.Type_STRUCT
}
