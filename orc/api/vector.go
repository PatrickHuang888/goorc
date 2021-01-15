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

func (cv *ColumnVector) Clear() {
	cv.Vector = cv.Vector[:0]
	for i:=0; i<len(cv.Children); i++ {
		cv.Children[i].Clear()
	}
}

func (cv ColumnVector) Len() int {
	// todo: other kind
	if cv.Kind == pb.Type_STRUCT {
		if len(cv.Vector) != 0 {
			// nulls
			return len(cv.Vector)
		}
		for _, c := range cv.Children {
			if len(c.Vector) != 0 {
				return len(c.Vector)
			}
			return c.Len()
		}
	}
	return len(cv.Vector)
}

func (cv ColumnVector) Cap() int {
	return cap(cv.Vector)
}

type Value struct {
	Null bool
	V    interface{}
}

// hive 0.13 support 38 digits
type Decimal64 struct {
	Precision int64
	Scale     int
}

func (d Decimal64) String() string {
	return fmt.Sprintf("precision %d, scale %d", d.Precision, d.Scale)
}

func (d Decimal64) Float64() float64 {
	if d.Scale==0 {
		return float64(d.Precision)
	}
	if d.Scale > 0 {
		return float64(d.Precision) * float64(10*d.Scale)
	}
	if d.Scale < 0 {
		return float64(d.Precision) / float64(10*d.Scale)
	}
	return 0
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
	Loc     *time.Location
	Seconds int64
	Nanos   uint64
}

func (t Timestamp) Time() time.Time {
	loc := t.Loc
	if loc == nil {
		loc = time.UTC
	}
	base := time.Date(2015, time.January, 1, 0, 0, 0, 0, loc).Unix()
	return time.Unix(base+t.Seconds, int64(t.Nanos)).In(loc)
}

func (t Timestamp) GetMilliSeconds() int64 {
	loc := t.Loc
	if loc == nil {
		loc = time.UTC
	}
	var ms int64
	baseSec := time.Date(2015, time.January, 1, 0, 0, 0, 0, loc).Unix()
	ms = (t.Seconds - baseSec) * 1_000
	ms += int64(t.Nanos / 1_000)
	return ms
}

func (t Timestamp) GetMilliSecondsUtc() int64 {
	base := time.Date(2015, time.January, 1, 0, 0, 0, 0, time.UTC).Unix()
	return base + t.Time().Unix() + int64(t.Nanos/1_000)
}

func GetTimestamp(t time.Time) Timestamp {
	base := time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	return Timestamp{t.Location(), t.Unix() - base, uint64(t.Nanosecond())}
}

func EncodingTimestampNanos(nanos uint64) (encoded uint64) {
	if nanos == 0 {
		return 0
	} else if nanos%100 != 0 {
		return uint64(nanos) << 3 // no encoding if less 2 zeros
	} else {
		nanos /= 100
		trailingZeros := 1
		for nanos%10 == 0 && trailingZeros < 7 { // 3 bits
			nanos /= 10
			trailingZeros++
		}
		return nanos<<3 | uint64(trailingZeros)
	}
}

func DecodingTimestampNanos(encoded uint64) (nano uint64) {
	zeros := 0x07 & encoded
	nano = encoded >> 3
	if zeros != 0 {
		for i := 0; i <= int(zeros); i++ {
			nano *= 10
		}
	}
	return
}


