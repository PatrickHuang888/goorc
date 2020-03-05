package orc

import (
	"fmt"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"time"
)

const (
	DEFAULT_ROW_SIZE = 1024 * 10
)

/*type ColumnVector interface {
	T() pb.Type_Kind
	Id() uint32
	Rows() int
	Presents() []bool
	reset()
}*/

type ColumnVector struct {
	Id       uint32
	Kind     pb.Type_Kind
	Presents []bool
	Vector   interface{}
}

func (cv ColumnVector) check() error {
	if cv.Kind == pb.Type_STRUCT && cv.Presents != nil {
		var presentCounts int
		for _, p := range cv.Presents {
			if p {
				presentCounts++
			}
		}
		for _, childVector := range cv.Vector.([]*ColumnVector) {
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
				}
			}
		}

	}
	return nil
}

/*type BoolColumn struct {
	col
	Vector []bool
}

func (BoolColumn) T() pb.Type_Kind {
	return pb.Type_BOOLEAN
}

func (bc *BoolColumn) reset() {
	bc.Vector = bc.Vector[:0]
	bc.col.reset()
}

func (c BoolColumn) Rows() int {
	return len(c.Vector)
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
	ColumnVector
	Vector []int64
}

func (*LongColumn) T() pb.Type_Kind {
	return pb.Type_LONG
}

func (lc *LongColumn) Rows() int {
	return len(lc.Vector)
}

func (lc *LongColumn) reset() {
	lc.Vector = lc.Vector[:0]
	lc.col.reset()
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
}*/

// hive 0.13 support 38 digits
type Decimal64 struct {
	Precision int64
	Scale     uint16
}

func (d Decimal64) String() string {
	return fmt.Sprintf("precision %d, scale %d", d.Precision, d.Scale)
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

// todo: timezone
type Timestamp struct {
	Seconds int64
	Nanos   uint32
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
