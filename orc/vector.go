package orc

import (
	"fmt"
	"time"

	"github.com/PatrickHuang888/goorc/pb/pb"
)

const (
	DEFAULT_ROW_SIZE = 1024 * 10
)

type ColumnVector interface {
	T() pb.Type_Kind
	ColumnId() uint32
	Rows() int
	Presents() []bool
	// todo: Presents and Vector verify, length should be equal if has nulls
	reset()
}

type col struct {
	id uint32
	//nullable bool
	//setNulls bool
	//hasNulls bool
	presents []bool
}

func (c *col) ColumnId() uint32 {
	return c.id
}

// call this function will not reflect Nulls change
/*func (c *col) HasNulls() bool {
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
}*/

/*func (c *col) presents() []bool {
	bb := make([]bool, len(c.Nulls))
	for i, b := range c.Nulls {
		bb[i] = !b
	}
	return bb
}*/

func (c *col) Presents() []bool {
	return c.presents
}

func (c *col) reset() {
	c.presents = c.presents[:0]
	/*c.setNulls = false
	c.hasNulls = false*/
	//c.nullable= false
}

type BoolColumn struct {
	col
	Vector []bool
}

func (*BoolColumn) T() pb.Type_Kind {
	return pb.Type_BOOLEAN
}

func (bc *BoolColumn) reset() {
	bc.Vector = bc.Vector[:0]
	bc.col.reset()
}

func (bc *BoolColumn) Rows() int {
	return len(bc.Vector)
}

type TinyIntColumn struct {
	col
	Vector []byte
}

func (*TinyIntColumn) T() pb.Type_Kind {
	return pb.Type_BYTE
}
func (tic *TinyIntColumn) reset() {
	tic.Vector = tic.Vector[:0]
	tic.col.reset()
}
func (tic *TinyIntColumn) Rows() int {
	return len(tic.Vector)
}

// nullable int column vector for all integer types
type LongColumn struct {
	col
	Vector []int64
}

func (*LongColumn) T() pb.Type_Kind {
	return pb.Type_LONG
}

func (lc *LongColumn) Rows() int {
	return len(lc.Vector)
}

func (lc *LongColumn) reset() {
	lc.col.reset()
	lc.Vector = lc.Vector[:0]
}

type BinaryColumn struct {
	col
	Vector [][]byte
}

func (*BinaryColumn) T() pb.Type_Kind {
	return pb.Type_BINARY
}
func (bc *BinaryColumn) reset() {
	bc.Vector = bc.Vector[:0]
	bc.col.reset()
}
func (bc *BinaryColumn) Rows() int {
	return len(bc.Vector)
}

// hive 0.13 support 38 digits
type Decimal64 struct {
	Precision int64
	Scale     uint16
}

func (d Decimal64) String() string {
	return fmt.Sprintf("precision %d, scale %d", d.Precision, d.Scale)
}

type Decimal64Column struct {
	col
	Vector []Decimal64
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
	col
	Vector []Date
}

func (*DateColumn) T() pb.Type_Kind {
	return pb.Type_DATE
}

func (dc *DateColumn) Rows() int {
	return len(dc.Vector)
}

func (dc *DateColumn) reset() {
	dc.col.reset()
	dc.Vector = dc.Vector[:0]
}

// todo: local timezone
type Timestamp struct {
	Seconds int64
	Nanos   uint32
}
type TimestampColumn struct {
	col
	Vector []Timestamp
}

// return seconds from 2015, Jan, 1 and nano seconds
func GetTime(ts Timestamp) (t time.Time) {
	base := time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	return time.Unix(base+ts.Seconds, int64(ts.Nanos)).UTC()
}

func GetTimestamp(t time.Time) Timestamp {
	base := time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	return Timestamp{t.Unix() - base, uint32(t.Nanosecond())}
}

func (*TimestampColumn) T() pb.Type_Kind {
	return pb.Type_TIMESTAMP
}

func (tc *TimestampColumn) Rows() int {
	return len(tc.Vector)
}

func (tc *TimestampColumn) reset() {
	tc.col.reset()
	tc.Vector = tc.Vector[:0]
}

type FloatColumn struct {
	col
	Vector []float32
}

func (*FloatColumn) T() pb.Type_Kind {
	return pb.Type_FLOAT
}
func (fc *FloatColumn) reset() {
	fc.col.reset()
	fc.Vector = fc.Vector[:0]
}
func (fc *FloatColumn) Rows() int {
	return len(fc.Vector)
}

type DoubleColumn struct {
	col
	Vector []float64
}

func (*DoubleColumn) T() pb.Type_Kind {
	return pb.Type_DOUBLE
}
func (dc *DoubleColumn) reset() {
	dc.col.reset()
	dc.Vector = dc.Vector[:0]
}
func (dc *DoubleColumn) Rows() int {
	return len(dc.Vector)
}

type StringColumn struct {
	col
	encoding string
	Vector   []string
}

func (sc *StringColumn) Rows() int {
	return len(sc.Vector)
}

func (*StringColumn) T() pb.Type_Kind {
	return pb.Type_STRING
}

func (sc *StringColumn) reset() {
	sc.col.reset()
	sc.Vector = sc.Vector[:0]
}

type StructColumn struct {
	col
	Fields []ColumnVector
}

func (*StructColumn) T() pb.Type_Kind {
	return pb.Type_STRUCT
}

func (sc *StructColumn) Rows() int {
	// fixme:
	return sc.Fields[0].Rows()
}

// rethink:
func (sc *StructColumn) reset() {
	/*for _, c := range sc.Fields {
		c.reset()
	}*/
}

type ListColumn struct {
	col
	Child ColumnVector
}

func (*ListColumn) T() pb.Type_Kind {
	return pb.Type_LIST
}

func (lc *ListColumn) Rows() int {
	return lc.Child.Rows()
}

// rethink:
func (lc *ListColumn) reset() {
	lc.Child.reset()
}

// todo:
type MapColumn struct {
	col
	vector map[ColumnVector]ColumnVector
}

func (*MapColumn) T() pb.Type_Kind {
	return pb.Type_MAP
}

// todo:
type UnionColumn struct {
	col
	tags   []int
	fields []ColumnVector
}

func (*UnionColumn) T() pb.Type_Kind {
	return pb.Type_UNION
}
