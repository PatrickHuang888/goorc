package orc

import "github.com/PatrickHuang888/goorc/pb/pb"

const (
	DEFAULT_ROW_SIZE     = 1024
	DEFAULT_MAX_ROW_SIZE = 10 * DEFAULT_ROW_SIZE
)

type ColumnVector interface {
	T() pb.Type_Kind
	ColumnId() uint32
	Rows() uint64
	Reset()
}

type columnVector struct {
	Id   uint32
	rows uint64
}

func (cv *columnVector) ColumnId() uint32 {
	return cv.Id
}

func (cv *columnVector) Rows() uint64 {
	return cv.rows
}

func (cv *columnVector) Reset() {
	cv.rows = 0
}

// nullable int column vector for all integer types
type LongColumnVector struct {
	columnVector
	Vector    []int64
	Repeating bool
}

func (*LongColumnVector) T() pb.Type_Kind {
	return pb.Type_LONG
}

type TimestampColumnVector struct {
	columnVector
	Vector []uint64
}

func (*TimestampColumnVector) T() pb.Type_Kind {
	return pb.Type_TIMESTAMP
}

type DoubleColumnVector struct {
	columnVector
	Vector []float64
}

func (*DoubleColumnVector) T() pb.Type_Kind {
	return pb.Type_DOUBLE
}

type BytesColumnVector struct {
	columnVector
	Vector [][]byte
}

func (*BytesColumnVector) T() pb.Type_Kind {
	return pb.Type_VARCHAR
}

type StructColumnVector struct {
	columnVector
	Fields []ColumnVector
}

func (*StructColumnVector) T() pb.Type_Kind {
	return pb.Type_STRUCT
}
