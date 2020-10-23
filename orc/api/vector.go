package api

import (
	"fmt"
	"github.com/patrickhuang888/goorc/pb/pb"
	"time"
)

type ColumnVector struct {
	Id       uint32
	Kind     pb.Type_Kind

	HasNull bool
	Vector   []Value

	Children []ColumnVector
}

type Value struct {
	Null bool
	V interface{}
}

type batchInternal struct {
	*ColumnVector
	presentsFromParent bool  // for struct writer
}

/*func (cv ColumnVector) check() error {
	if cv.Kind == pb.Type_STRUCT && cv.Presents != nil {
		var presentCounts int
		for _, p := range cv.Presents {
			if p {
				presentCounts++
			}
		}
		for i, childVector := range cv.Vector.([]*ColumnVector) {
			switch childVector.Kind {
			case pb.Type_INT:
				fallthrough
			case pb.Type_LONG:
				vector := childVector.Vector.([]int64)
				if len(vector) < presentCounts {
					return errors.Errorf("column %d vector data less than presents data in struct column", childVector.Id)
				}
				if len(vector) > presentCounts {
					log.Warnf("column %d vector data large than prensents in struct column, extra data will be discard", childVector.Id)
					vector = vector[:presentCounts]
					cv.Vector.([]*ColumnVector)[i].Vector = vector
				}
			}
		}

	}
	return nil
}*/

// hive 0.13 support 38 digits
type Decimal64 struct {
	Precision int64
	Scale     int
}

func (d Decimal64) String() string {
	return fmt.Sprintf("precision %d, scale %d", d.Precision, d.Scale)
}

func (d Decimal64) Float64() float64  {
	return float64(d.Precision) * float64(10*d.Scale)
}

/*type Decimal64Column struct {
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
}*/

type Date time.Time

func NewDate(year int, month time.Month, day int) Date {
	return Date(time.Date(year, month, day, 0, 0, 0, 0, time.UTC))
}

func (d *Date) String() string {
	return time.Time(*d).Format("2006-01-02")
}

func FromDays(days int64) Date {
	d := time.Duration(days * 24 * int64(time.Hour))
	t := Date(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).Add(d))
	return t
}

// days from 1970, Jan, 1 UTC
func ToDays(d Date) int64 {
	s := time.Time(d).Sub(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC))
	return int64(s.Hours() / 24)
}

// todo: timezone
type Timestamp struct {
	Seconds int64
	Nanos   uint32
}

func (ts Timestamp) Time(loc *time.Location) time.Time {
	if loc==nil {
		loc= time.UTC
	}
	base := time.Date(2015, time.January, 1, 0, 0, 0, 0, loc).Unix()
	return time.Unix(base+ts.Seconds, int64(ts.Nanos)).In(loc)
}

func (ts Timestamp) GetMilliSeconds() int64  {
	// todo:
	return 0
}

func (ts Timestamp) GetMilliSecondsUtc() int64  {
	// todo:
	return 0
}

func GetTimestamp(t time.Time) Timestamp {
	base := time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	return Timestamp{t.UTC().Unix() - base, uint32(t.Nanosecond())}
}

/*func (*TimestampColumn) T() pb.Type_Kind {
	return pb.Type_TIMESTAMP
}

func (tc *TimestampColumn) Rows() int {
	return len(tc.Vector)
}

func (tc *TimestampColumn) reset() {
	tc.col.reset()
	tc.Vector = tc.Vector[:0]
}*/

/*type FloatColumn struct {
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

func (DoubleColumn) T() pb.Type_Kind {
	return pb.Type_DOUBLE
}

func (c *DoubleColumn) reset() {
	c.col.reset()
	c.Vector = c.Vector[:0]
}

func (c DoubleColumn) Rows() int {
	return len(c.Vector)
}

type StringColumn struct {
	col
	encoding string
	Vector   []string
}

func (c StringColumn) Rows() int {
	return len(c.Vector)
}

func (StringColumn) T() pb.Type_Kind {
	return pb.Type_STRING
}

func (c *StringColumn) reset() {
	c.col.reset()
	c.Vector = c.Vector[:0]
}*/

/*type Struct struct {
	Presents []bool
	Children []*ColumnVector
}*/

/*type ListColumn struct {
	col
	Child ColumnVector
}

func (ListColumn) T() pb.Type_Kind {
	return pb.Type_LIST
}*/

/*func (lc *ListColumn) Rows() int {
	return lc.Child.Rows()
}*/

/*// todo:
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
}*/
