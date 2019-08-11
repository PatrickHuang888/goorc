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
	c.setNulls = false
	c.hasNulls = false
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

// nullable int column vector for all integer types
type LongColumn struct {
	column
	Vector []int64
}

func (*LongColumn) T() pb.Type_Kind {
	return pb.Type_LONG
}

func (lc *LongColumn) Rows() int {
	return len(lc.Vector)
}

func (lc *LongColumn) reset() {
	lc.column.reset()
	lc.Vector = lc.Vector[:0]
}

type BinaryColumn struct {
	column
	Vector [][]byte
}

func (*BinaryColumn) T() pb.Type_Kind {
	return pb.Type_BINARY
}
func (bc *BinaryColumn) reset() {
	bc.Vector = bc.Vector[:0]
	bc.column.reset()
}
func (bc *BinaryColumn) Rows() int {
	return len(bc.Vector)
}

// hive 0.13 support 38 digits
type Decimal64Column struct {
	column
	Vector []int64 // precision 18
	Scale  uint16
}

func (*Decimal64Column) T() pb.Type_Kind {
	return pb.Type_DECIMAL
}
func (dc *Decimal64Column) Rows() int {
	return len(dc.Vector)
}
func (dc *Decimal64Column) reset() {
	dc.Vector = dc.Vector[:0]
}

type Date time.Time

func NewDate(year int, month time.Month, day int) Date {
	return Date(time.Date(year, month, day, 0, 0, 0, 0, time.UTC))
}

func (d *Date) String() string {
	return time.Time(*d).Format("2006-01-02")
}

func fromDays(days int64) Date {
	d := time.Duration(days * 24 * int64(time.Hour))
	t := Date(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).Add(d))
	return t
}

// days from 1970, Jan, 1 UTC
func toDays(d Date) int64 {
	s := time.Time(d).Sub(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC))
	return int64(s.Hours() / 24)
}

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

type Timestamp time.Time
type TimestampColumn struct {
	column
	Vector []Timestamp
}

// return seconds from 2015, Jan, 1 and nano seconds
func getSecondsAndNanos(t Timestamp) (seconds int64, nanos uint64) {
	base := time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC)
	seconds = int64(time.Time(t).Second()- base.Second())
	nanos = uint64(time.Time(t).Nanosecond()- base.Nanosecond())
	return
}

func getTimestamp(seconds int64, nanos uint64) Timestamp {
	base := time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC)
	return Timestamp(base.Add(time.Duration(float64(seconds) + float64(nanos/1e9))))
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
	Vector []float32
}

func (*FloatColumn) T() pb.Type_Kind {
	return pb.Type_FLOAT
}
func (fc *FloatColumn) reset() {
	fc.Vector = fc.Vector[:0]
}
func (fc *FloatColumn) Rows() int {
	return len(fc.Vector)
}

type DoubleColumn struct {
	column
	Vector []float64
}

func (*DoubleColumn) T() pb.Type_Kind {
	return pb.Type_DOUBLE
}
func (dc *DoubleColumn) reset() {
	dc.Vector = dc.Vector[:0]
}
func (dc *DoubleColumn) Rows() int {
	return len(dc.Vector)
}

type StringColumn struct {
	column
	Vector []string
}

func (sc *StringColumn) Rows() int {
	return len(sc.Vector)
}

func (*StringColumn) T() pb.Type_Kind {
	return pb.Type_STRING
}

func (sc *StringColumn) reset() {
	sc.Vector = sc.Vector[:0]
	sc.column.reset()
}

type StructColumn struct {
	column
	Fields []ColumnVector
}

func (*StructColumn) T() pb.Type_Kind {
	return pb.Type_STRUCT
}

func (sc *StructColumn) Rows() int {
	// toAssure
	return sc.Fields[0].Rows()
}

func (sc *StructColumn) reset() {
	for _, c := range sc.Fields {
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
