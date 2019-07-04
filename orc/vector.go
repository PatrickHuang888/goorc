package orc

import "github.com/PatrickHuang888/goorc/pb/pb"

const (
	DEFAULT_ROW_SIZE     = 1024
)

type ColumnVector interface {
	T() pb.Type_Kind
	ColumnId() uint32
	Rows() int
	reset()
}

type column struct {
	id   uint32
}

func (c column) ColumnId() uint32 {
	return c.id
}

// nullable int column vector for all integer types
type LongColumn struct {
	column
	vector    []int64
}

func (*LongColumn) T() pb.Type_Kind {
	return pb.Type_LONG
}

func (lc *LongColumn) GetVector() []int64 {
	return lc.vector
}

func (lc *LongColumn) SetVector(vector []int64) {
	lc.vector = vector
}

func (lc *LongColumn) Rows() int  {
	return len(lc.vector)
}

func (lc *LongColumn) reset()  {
	lc.vector= lc.vector[:0]
}

type TimestampColumn struct {
	column
	vector []uint64
}

func (*TimestampColumn) T() pb.Type_Kind {
	return pb.Type_TIMESTAMP
}


type DoubleColumn struct {
	column
	vector []float64
}

func (*DoubleColumn) T() pb.Type_Kind {
	return pb.Type_DOUBLE
}

type StringColumn struct {
	column
	vector []string
}

func (sc *StringColumn) GetVector() []string {
	return sc.vector
}

func (sc *StringColumn) SetVector(v []string) {
	sc.vector = v
}

func (sc *StringColumn) Rows() int{
	return len(sc.vector)
}

func (*StringColumn) T() pb.Type_Kind {
	return pb.Type_STRING
}

func (sc *StringColumn) reset()  {
	sc.vector= sc.vector[:0]
}

type StructColumn struct {
	column
	fields []ColumnVector
}

func (sc *StructColumn) GetFields() []ColumnVector {
	return sc.fields
}

func (*StructColumn) T() pb.Type_Kind {
	return pb.Type_STRUCT
}

func (sc *StructColumn) Rows() int {
	// toReAssure: impl with arbitrary column
	return sc.fields[0].Rows()
}
