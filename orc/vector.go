package orc

import "github.com/PatrickHuang888/goorc/pb/pb"

const (
	DEFAULT_ROW_SIZE     = 1024
)

type ColumnVector interface {
	T() pb.Type_Kind
	ColumnId() uint32
	Rows() int
}

type columnVector struct {
	id   uint32
	rows int
}

func (cv *columnVector) ColumnId() uint32 {
	return cv.id
}

func (cv *columnVector) Rows() int {
	// fixme: compound vector
	return cv.rows
}

// nullable int column vector for all integer types
type LongColumnVector struct {
	columnVector
	vector    []int64
	repeating bool
}

func (*LongColumnVector) T() pb.Type_Kind {
	return pb.Type_LONG
}

func (cv *LongColumnVector) GetVector() []int64 {
	return cv.vector[:cv.rows]
}

func (cv *LongColumnVector) SetVector(vector []int64) {
	cv.vector = vector
	cv.rows = len(vector)
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

type StringColumnVector struct {
	columnVector
	vector []string
}

func (cv *StringColumnVector) GetVector() []string {
	return cv.vector[:cv.rows]
}

func (cv *StringColumnVector) SetVector(v []string) {
	cv.rows= len(v)
	cv.vector = v
}

func (*StringColumnVector) T() pb.Type_Kind {
	return pb.Type_STRING
}

type StructColumnVector struct {
	columnVector
	fields []ColumnVector
}

func (cv *StructColumnVector) GetFields() []ColumnVector {
	return cv.fields
}

func (*StructColumnVector) T() pb.Type_Kind {
	return pb.Type_STRUCT
}
