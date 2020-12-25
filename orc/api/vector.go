package api

import (
	"fmt"
	"time"

	"github.com/patrickhuang888/goorc/pb/pb"
)

type ColumnVector struct {
	Id   uint32
	Kind pb.Type_Kind

	Vector []Value

	Children []ColumnVector
}

type Value struct {
	Null bool
	V    interface{}
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
	return fmt.Sprintf("%f", d.Float64())
}

func (d Decimal64) Float64() float64 {
	if d.Scale >0 {
		return float64(d.Precision) * float64(10*d.Scale)
	}else {
		return float64(d.Precision)/float64(10*d.Scale)
	}
}

// enhance: UTC
type Date time.Time

func NewDate(year int, month time.Month, day int) Date {
	return Date(time.Date(year, month, day, 0, 0, 0, 0, time.UTC))
}

func (d *Date) String() string {
	return time.Time(*d).Format("2006-01-02")
}

func FromDays(days int32) Date {
	d := time.Duration(int64(days) * 24 * int64(time.Hour))
	t := Date(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).Add(d))
	return t
}

// days from 1970, Jan, 1 UTC
func ToDays(d Date) int32 {
	// time.Time(d).UTC() ??
	s := time.Time(d).Sub(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC))
	return int32(s.Hours() / 24)
}

type Timestamp struct {
	Loc *time.Location
	Seconds int64
	Nanos   uint32
}

func (t Timestamp) Time() time.Time {
	loc:= t.Loc
	if loc == nil {
		loc = time.UTC
	}
	base := time.Date(2015, time.January, 1, 0, 0, 0, 0, loc).Unix()
	return time.Unix(base+t.Seconds, int64(t.Nanos)).In(loc)
}

func (t Timestamp) GetMilliSeconds() int64 {
	loc:= t.Loc
	if loc==nil {
		loc= time.UTC
	}
	var ms int64
	baseSec := time.Date(2015, time.January, 1, 0, 0, 0, 0, loc).Unix()
	ms = (t.Seconds - baseSec) * 1_000
	ms += int64(t.Nanos / 1_000)
	return ms
}

func (t Timestamp) GetMilliSecondsUtc() int64 {
	base := time.Date(2015, time.January, 1, 0, 0, 0, 0, time.UTC).Unix()
	return base+t.Time().Unix()+int64(t.Nanos/1_000)
}

func GetTimestamp(t time.Time) Timestamp {
	base := time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	return Timestamp{t.Location(), t.Unix() - base, uint32(t.Nanosecond())}
}
