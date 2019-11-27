package orc

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestTimestampEncoding(test *testing.T) {
	layout := "2006-01-01 00:00:00.999999999"
	t1, _ := time.Parse(layout, "2037-01-01 00:00:00.000999")
	ts := GetTimestamp(t1)
	t := GetTime(ts)
	fmt.Println(t)
	ts1 := GetTimestamp(t)
	assert.Equal(test, ts, ts1)

	t2, _ := time.Parse(layout, "2003-01-01 00:00:00.000000222")
	ts = GetTimestamp(t2)
	t = GetTime(ts)
	fmt.Println(t)
	ts1 = GetTimestamp(t)
	assert.Equal(test, ts, ts1)

	t3, _ := time.Parse(layout, "1995-01-01 00:00:00.688888888")
	ts = GetTimestamp(t3)
	t = GetTime(ts)
	fmt.Println(t)
	ts1 = GetTimestamp(t)
	assert.Equal(test, ts, ts1)
}
