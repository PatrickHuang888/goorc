package api

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimestampEncoding(test *testing.T) {
	layout := "2006-01-02 15:04:05.999999999"
	t1, _ := time.Parse(layout, "2037-01-01 00:00:00.000999")
	ts := GetTimestamp(t1)
	t := ts.Time()
	fmt.Println(t)
	ts1 := GetTimestamp(t)
	assert.Equal(test, ts, ts1)

	t2, _ := time.Parse(layout, "2003-01-01 00:00:00.000000222")
	ts = GetTimestamp(t2)
	t = ts.Time()
	fmt.Println(t)
	ts1 = GetTimestamp(t)
	assert.Equal(test, ts, ts1)

	t3, _ := time.Parse(layout, "1995-01-01 00:00:00.688888888")
	ts = GetTimestamp(t3)
	t = ts.Time()
	fmt.Println(t)
	ts1 = GetTimestamp(t)
	assert.Equal(test, ts, ts1)
}
