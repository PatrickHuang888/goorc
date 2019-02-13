package hive

const (
	// column vector type
	NONE Type = iota
	LONG
	DOUBLE
	BYTES
	DECIMAL
	DECIMAL_64
	TIMESTAMP
	INTERVAL_DAY_TIME
	STRUCT
	LIST
	MAP
	UNION
	VOID

	DefaultRowSize = 1024
)

type Type int

type ColumnVector interface {
	T() Type
}

// nullable int column vector for all integer types
type LongColumnVector struct {
	Vector []int64
}

func (LongColumnVector) T() Type {
	return LONG
}

func NewLongColumnVector(len int) ColumnVector {
	v := make([]int64, len)
	return &LongColumnVector{Vector: v}
}

type timestampColumnVector struct {
	time []uint64
}

func (timestampColumnVector) T() Type {
	return TIMESTAMP
}

func NewTimestampColumnVector(len int) ColumnVector {
	t := make([]uint64, len)
	return &timestampColumnVector{time: t}
}

type doubleColumnVector struct {
	vector []float64
}

func (doubleColumnVector) T() Type {
	return DOUBLE
}

func NewDoubleColumnVector(len int) ColumnVector {
	v := make([]float64, len)
	return &doubleColumnVector{vector: v}
}

type bytesColumnVector struct {
	vector [][]byte
}

func (bytesColumnVector) T() Type {
	return BYTES
}

func NewBytesColumnVector(len int) ColumnVector {
	v := make([][]byte, len)
	return &bytesColumnVector{vector: v}
}

type structColumnVector struct {
	fields []ColumnVector
}

func (structColumnVector) T() Type {
	return STRUCT
}

func NewStructColumnVector(len int, fields ...ColumnVector) ColumnVector {
	var v []ColumnVector
	v = append(v, fields...)
	return &structColumnVector{fields: v}
}
