package orc

import (
	"github.com/PatrickHuang888/goorc/pb/pb"
	"time"
)

const (
	DEFAULT_ROW_SIZE = 1024
)

type ColumnVector interface {
	T() pb.Type_Kind
	ColumnId() uint32
	Rows() int
	HasNulls() bool
	presents() []bool
	// todo: Presents and Vector verify, length should be equal if has nulls
	reset()
}

type column struct {
	id       uint32
	nullable bool
	setNulls bool
	hasNulls bool
	Nulls    []bool
}

func (c *column) ColumnId() uint32 {
	return c.id
}

// call this function will not reflect Nulls change
func (c *column) HasNulls() bool {
	if !c.nullable {
		return false
	}
	if c.setNulls {
		return c.hasNulls
	}
	for _, b := range c.Nulls {
		if b {
			c.setNulls = true
			c.hasNulls = true
		}
	}
	c.setNulls = true
	return false
}

func (c *column) presents() []bool {
	bb := make([]bool, len(c.Nulls))
	for i, b := range c.Nulls {
		bb[i] = !b
	}
	return bb
}

func (c *column) reset() {
	c.Nulls = c.Nulls[:0]
	c.setNulls= false
	c.hasNulls= false
}

type BoolColumn struct {
	column
	Vector []bool
}

func (*BoolColumn) T() pb.Type_Kind {
	return pb.Type_BOOLEAN
}

func (bc *BoolColumn) reset() {
	bc.Vector = bc.Vector[:0]
	bc.column.reset()
}

func (bc *BoolColumn) Rows() int {
	return len(bc.Vector)
}

type TinyIntColumn struct {
	column
	Vector []byte
}

func (*TinyIntColumn) T() pb.Type_Kind {
	return pb.Type_BYTE
}
func (tic *TinyIntColumn) reset() {
	tic.Vector = tic.Vector[:0]
	tic.column.reset()
}
func (tic *TinyIntColumn) Rows() int {
	return len(tic.Vector)
}

type SmallIntColumn struct {
	column
	Vector []int16
}

func (*SmallIntColumn) T() pb.Type_Kind {
	return pb.Type_SHORT
}
func (sic *SmallIntColumn) reset() {
	sic.Vector = sic.Vector[:0]
	sic.column.reset()
}
func (sic *SmallIntColumn) Rows() int {
	return len(sic.Vector)
}

type IntColumn struct {
	column
	Vector []int32
}

func (*IntColumn) T() pb.Type_Kind {
	return pb.Type_INT
}
func (ic *IntColumn) reset() {
	ic.column.reset()
	ic.Vector = ic.Vector[:0]
}
func (ic *IntColumn) Rows() int {
	return len(ic.Vector)
}

// nullable int column vector for all integer types
type BigIntColumn struct {
	column
	Vector []int64
}

func (*BigIntColumn) T() pb.Type_Kind {
	return pb.Type_LONG
}

func (bic *BigIntColumn) Rows() int {
	return len(bic.Vector)
}

func (bic *BigIntColumn) reset() {
	bic.column.reset()
	bic.Vector = bic.Vector[:0]
}

type BinaryColumn struct {
	column
	vector [][]byte
}

func (*BinaryColumn) T() pb.Type_Kind {
	return pb.Type_BINARY
}
func (bc *BinaryColumn) reset() {
	bc.vector = bc.vector[:0]
}
func (bc *BinaryColumn) Rows() int {
	return len(bc.vector)
}

type DecimalColumn struct {
	column
	Vector [][16]byte // 38 digits, 128 bits
}

func (*DecimalColumn) T() pb.Type_Kind {
	return pb.Type_DECIMAL
}
func (dc *DecimalColumn) Rows() int {
	return len(dc.Vector)
}
func (dc *DecimalColumn) reset() {
	dc.Vector = dc.Vector[:0]
}

type Date uint64
type DateColumn struct {
	column
	Vector []Date
}

func (*DateColumn) T() pb.Type_Kind {
	return pb.Type_DATE
}

func (dc *DateColumn) Rows() int {
	return len(dc.Vector)
}

func (dc *DateColumn) reset() {
	dc.Vector = dc.Vector[:0]
}

type Timestamp struct {
	Date  Date
	Nanos time.Duration
}
type TimestampColumn struct {
	column
	Vector []Timestamp
}

func (*TimestampColumn) T() pb.Type_Kind {
	return pb.Type_TIMESTAMP
}

func (tc *TimestampColumn) Rows() int {
	return len(tc.Vector)
}

func (tc *TimestampColumn) reset() {
	tc.Vector = tc.Vector[:0]
}

type FloatColumn struct {
	column
	vector []float32
}

func (*FloatColumn) T() pb.Type_Kind {
	return pb.Type_FLOAT
}
func (fc *FloatColumn) reset() {
	fc.vector = fc.vector[:0]
}
func (fc *FloatColumn) Rows() int {
	return len(fc.vector)
}
func (fc *FloatColumn) SetVector(vector []float32) {
	fc.vector = vector
}
func (fc *FloatColumn) GetVector() []float32 {
	return fc.vector
}

type DoubleColumn struct {
	column
	vector []float64
}

func (*DoubleColumn) T() pb.Type_Kind {
	return pb.Type_DOUBLE
}
func (dc *DoubleColumn) reset() {
	dc.vector = dc.vector[:0]
}
func (dc *DoubleColumn) Rows() int {
	return len(dc.vector)
}
func (dc *DoubleColumn) SetVector(vector []float64) {
	dc.vector = vector
}
func (dc *DoubleColumn) GetVector() []float64 {
	return dc.vector
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

func (sc *StringColumn) Rows() int {
	return len(sc.vector)
}

func (*StringColumn) T() pb.Type_Kind {
	return pb.Type_STRING
}

func (sc *StringColumn) reset() {
	sc.vector = sc.vector[:0]
}

type StructColumn struct {
	column
	fields []ColumnVector
}

func (sc *StructColumn) AddFields(subColumn ColumnVector) {
	sc.fields = append(sc.fields, subColumn)
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

func (sc *StructColumn) reset() {
	for _, c := range sc.fields {
		c.reset()
	}
}

type ListColumn struct {
	column
	Child ColumnVector
}

func (*ListColumn) T() pb.Type_Kind {
	return pb.Type_LIST
}

func (lc *ListColumn) Rows() int {
	return lc.Child.Rows()
}

func (lc *ListColumn) reset() {
	lc.Child.reset()
}

type MapColumn struct {
	column
	vector map[ColumnVector]ColumnVector
}

func (*MapColumn) T() pb.Type_Kind {
	return pb.Type_MAP
}

type UnionColumn struct {
	column
	tags   []int
	fields []ColumnVector
}

func (*UnionColumn) T() pb.Type_Kind {
	return pb.Type_UNION
}
