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
)

type Type int

type ColumnVector interface {
	T() Type
}

// nullable int column vector for all integer types
type LongColumnVector struct {
	Vector []int64
}

func (*LongColumnVector) T() Type {
	return LONG
}

type TimestampColumnVector struct {
	Vector []uint64
}

func (*TimestampColumnVector) T() Type {
	return TIMESTAMP
}

type DoubleColumnVector struct {
	Vector []float64
}

func (*DoubleColumnVector) T() Type {
	return DOUBLE
}

type BytesColumnVector struct {
	Vector [][]byte
}

func (*BytesColumnVector) T() Type {
	return BYTES
}

type StructColumnVector struct {
	fields []ColumnVector
}

func (*StructColumnVector) T() Type {
	return STRUCT
}

func NewStructColumnVector(len int, fields ...ColumnVector) ColumnVector {
	var v []ColumnVector
	v = append(v, fields...)
	return &StructColumnVector{fields: v}
}
