package testing

import (
	"github.com/patrickhuang888/goorc/orc"
	"github.com/patrickhuang888/goorc/orc/api"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func init() {
	log.SetLevel(log.TraceLevel)
}

func TestBasicNoCompression(t *testing.T) {
	opts := orc.DefaultReaderOptions()
	reader, err := orc.NewFileReader("basicLongNoCompression.orc", opts)
	if err != nil {
		t.Errorf("create reader error: %+v", err)
	}

	schema := reader.GetSchema()
	batch := schema.CreateReaderBatch(opts)

	err = reader.Next(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	assert.Equal(t, 90, batch.ReadRows)

	values := batch.Vector.([]int64)

	min := values[0]
	max := values[0]
	for _, v := range values {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	assert.Equal(t, 1, int(min))
	assert.Equal(t, 2000, int(max))

	if err = reader.Close(); err != nil {
		t.Fatalf("%+v", err)
	}
}

func TestPatchBaseNegativeMinNoCmpression(t *testing.T) {
	values := []int64{
		20, 2, 3, 2, 1,
		3, 17, 71, 35, 2,
		1, 139, 2, 2, 3,
		1783, 475, 2, 1, 1,
		3, 1, 3, 2, 32,
		1, 2, 3, 1, 8,
		30, 1, 3, 414, 1,
		1, 135, 3, 3, 1,
		414, 2, 1, 2, 2,
		594, 2, 5, 6, 4,

		11, 1, 2, 2, 1,
		1, 52, 4, 1, 2,
		7, 1, 17, 334, 1,
		2, 1, 2, 2, 6,
		1, 266, 1, 2, 217,
		2, 6, 2, 13, 2,
		2, 1, 2, 3, 5,
		1, 2, 1, 7244, 11813,
		1, 33, 2, -13, 1,
		2, 3, 13, 1, 92,

		3, 13, 5, 14, 9,
		141, 12, 6, 15, 25,
		1, 1, 1, 46, 2,
		1, 1, 141, 3, 1,
		1, 1, 1, 2, 1,
		4, 34, 5, 78, 8,
		1, 2, 2, 1, 9,
		10, 2, 1, 4, 13,
		1, 5, 4, 4, 19,
		5, 1, 1, 1, 68,

		33, 399, 1, 1885, 25,
		5, 2, 4, 1, 1,
		2, 16, 1, 2966, 3,
		1, 1, 25501, 1, 1,
		1, 66, 1, 3, 8,
		131, 14, 5, 1, 2,
		2, 1, 1, 8, 1,
		1, 2, 1, 5, 9,
		2, 3, 112, 13, 2,
		2, 1, 5, 10, 3,

		1, 1, 13, 2, 3,
		4, 1, 3, 1, 1,
		2, 1, 1, 2, 4,
		2, 207, 1, 1, 2,
		4, 3, 3, 2, 2,
		16}

	opts := orc.DefaultReaderOptions()
	reader, err := orc.NewFileReader("patchBaseNegativeMin.orc", opts)
	if err != nil {
		t.Errorf("create reader error: %+v", err)
	}
	schema := reader.GetSchema()

	batch := schema.CreateReaderBatch(opts)

	if err := reader.Next(batch);err != nil {
		t.Fatalf("%+v", err)
	}

	reader.Close()

	assert.Equal(t, values, batch.Vector)
}

func TestPatchBaseNegativeMin2NoCmppression(t *testing.T) {
	values := []int64{
		20, 2, 3, 2, 1, 3, 17, 71, 35, 2, 1, 139, 2, 2,
		3, 1783, 475, 2, 1, 1, 3, 1, 3, 2, 32, 1, 2, 3, 1, 8, 30, 1, 3, 414, 1,
		1, 135, 3, 3, 1, 414, 2, 1, 2, 2, 594, 2, 5, 6, 4, 11, 1, 2, 2, 1, 1,
		52, 4, 1, 2, 7, 1, 17, 334, 1, 2, 1, 2, 2, 6, 1, 266, 1, 2, 217, 2, 6,
		2, 13, 2, 2, 1, 2, 3, 5, 1, 2, 1, 7244, 11813, 1, 33, 2, -1, 1, 2, 3,
		13, 1, 92, 3, 13, 5, 14, 9, 141, 12, 6, 15, 25, 1, 1, 1, 46, 2, 1, 1,
		141, 3, 1, 1, 1, 1, 2, 1, 4, 34, 5, 78, 8, 1, 2, 2, 1, 9, 10, 2, 1, 4,
		13, 1, 5, 4, 4, 19, 5, 1, 1, 1, 68, 33, 399, 1, 1885, 25, 5, 2, 4, 1,
		1, 2, 16, 1, 2966, 3, 1, 1, 25501, 1, 1, 1, 66, 1, 3, 8, 131, 14, 5, 1,
		2, 2, 1, 1, 8, 1, 1, 2, 1, 5, 9, 2, 3, 112, 13, 2, 2, 1, 5, 10, 3, 1,
		1, 13, 2, 3, 4, 1, 3, 1, 1, 2, 1, 1, 2, 4, 2, 207, 1, 1, 2, 4, 3, 3, 2,
		2, 16}

	opts := orc.DefaultReaderOptions()
	reader, err := orc.NewFileReader("patchBaseNegativeMin2.orc", opts)
	if err != nil {
		t.Errorf("create reader error: %+v", err)
	}
	schema := reader.GetSchema()

	batch := schema.CreateReaderBatch(opts)

	if err = reader.Next(batch);err != nil {
		t.Fatalf("%+v", err)
	}

	reader.Close()

	assert.Equal(t, values, batch.Vector)
}

func TestPatchBaseNegativeMin3NoCompression(t *testing.T) {
	values := []int64{
		20, 2, 3, 2, 1, 3, 17, 71, 35, 2, 1, 139, 2, 2,
		3, 1783, 475, 2, 1, 1, 3, 1, 3, 2, 32, 1, 2, 3, 1, 8, 30, 1, 3, 414, 1,
		1, 135, 3, 3, 1, 414, 2, 1, 2, 2, 594, 2, 5, 6, 4, 11, 1, 2, 2, 1, 1,
		52, 4, 1, 2, 7, 1, 17, 334, 1, 2, 1, 2, 2, 6, 1, 266, 1, 2, 217, 2, 6,
		2, 13, 2, 2, 1, 2, 3, 5, 1, 2, 1, 7244, 11813, 1, 33, 2, 0, 1, 2, 3,
		13, 1, 92, 3, 13, 5, 14, 9, 141, 12, 6, 15, 25, 1, 1, 1, 46, 2, 1, 1,
		141, 3, 1, 1, 1, 1, 2, 1, 4, 34, 5, 78, 8, 1, 2, 2, 1, 9, 10, 2, 1, 4,
		13, 1, 5, 4, 4, 19, 5, 1, 1, 1, 68, 33, 399, 1, 1885, 25, 5, 2, 4, 1,
		1, 2, 16, 1, 2966, 3, 1, 1, 25501, 1, 1, 1, 66, 1, 3, 8, 131, 14, 5, 1,
		2, 2, 1, 1, 8, 1, 1, 2, 1, 5, 9, 2, 3, 112, 13, 2, 2, 1, 5, 10, 3, 1,
		1, 13, 2, 3, 4, 1, 3, 1, 1, 2, 1, 1, 2, 4, 2, 207, 1, 1, 2, 4, 3, 3, 2,
		2, 16}

	opts := orc.DefaultReaderOptions()
	reader, err := orc.NewFileReader("patchBaseNegativeMin3.orc", opts)
	if err != nil {
		t.Errorf("create reader error: %+v", err)
	}
	schema := reader.GetSchema()
	batch := schema.CreateReaderBatch(opts)

	if err = reader.Next(batch);err != nil {
		t.Fatalf("%+v", err)
	}

	reader.Close()

	assert.Equal(t, values, batch.Vector)
}

func TestStructs(t *testing.T) {
	opts := orc.DefaultReaderOptions()

	reader, err := orc.NewFileReader("testStructs.0.12.orc", opts)
	if err != nil {
		t.Errorf("create reader error: %+v", err)
	}

	schema := reader.GetSchema()
	log.Debugf("schema: %s", schema.String())

	batch := schema.CreateReaderBatch(opts)

	if err := reader.Next(batch); err != nil {
		t.Fatalf("%+v", err)
	}

	assert.Equal(t, 1024, batch.ReadRows)

	if err := reader.Close(); err != nil {
		t.Fatalf("%+v", err)
	}

	cl1 := batch.Vector.([]*api.ColumnVector)[0]
	for i := 0; i < 1024; i++ {
		if i < 200 || (i >= 400 && i < 600) || i >= 800 {
			assert.Equal(t, false, cl1.Presents[i])
		} else {
			assert.Equal(t, true, cl1.Presents[i])
			cl2 := cl1.Vector.([]*api.ColumnVector)[0]
			vv := cl2.Vector.([]int64)
			assert.Equal(t, i, int(vv[i]))
		}
	}

}

func TestTimestamp(t *testing.T) {
	opts := orc.DefaultReaderOptions()

	reader, err := orc.NewFileReader("testTimestamp.0.12.orc", opts)
	if err != nil {
		t.Errorf("create reader error: %+v", err)
	}

	schema := reader.GetSchema()
	log.Debugf("schema: %s", schema.String())

	batch := schema.CreateReaderBatch(opts)

	if err := reader.Next(batch); err != nil {
		t.Fatalf("%+v", err)
	}

	var values []string
	t1 := "2037-01-01 00:00:00.000999"
	values = append(values, t1)
	t2 := "2003-01-01 00:00:00.000000222"
	values = append(values, t2)
	t3 := "1999-01-01 00:00:00.999999999"
	values = append(values, t3)
	t4 := "1995-01-01 00:00:00.688888888"
	values = append(values, t4)
	t5 := "2002-01-01 00:00:00.1"
	values = append(values, t5)
	t6 := "2010-03-02 00:00:00.000009001"
	values = append(values, t6)
	t7 := "2005-01-01 00:00:00.000002229"
	values = append(values, t7)
	t8 := "2006-01-01 00:00:00.900203003"
	values = append(values, t8)
	t9 := "2003-01-01 00:00:00.800000007"
	values = append(values, t9)
	t10 := "1996-08-02 00:00:00.723100809"
	values = append(values, t10)
	t11 := "1998-11-02 00:00:00.857340643"
	values = append(values, t11)
	t12 := "2008-10-02 00:00:00"
	values = append(values, t12)

	assert.Equal(t, len(values), batch.ReadRows)

	layout := "2006-01-02 15:04:05.999999999"
	loc, _ := time.LoadLocation("US/Pacific")  //data write with us/pacific locale
	assert.Equal(t, t1, batch.Vector.([]api.Timestamp)[0].Time(loc).Format(layout))
	assert.Equal(t, t2, batch.Vector.([]api.Timestamp)[1].Time(loc).Format(layout))
	assert.Equal(t, t3, batch.Vector.([]api.Timestamp)[2].Time(loc).Format(layout))
	assert.Equal(t, t4, batch.Vector.([]api.Timestamp)[3].Time(loc).Format(layout))
	assert.Equal(t, t5, batch.Vector.([]api.Timestamp)[4].Time(loc).Format(layout))
	assert.Equal(t, t6, batch.Vector.([]api.Timestamp)[5].Time(loc).Format(layout))
	assert.Equal(t, t7, batch.Vector.([]api.Timestamp)[6].Time(loc).Format(layout))
	assert.Equal(t, t8, batch.Vector.([]api.Timestamp)[7].Time(loc).Format(layout))
	assert.Equal(t, t9, batch.Vector.([]api.Timestamp)[8].Time(loc).Format(layout))

	// data written has daylight saving
	v10 := batch.Vector.([]api.Timestamp)[9].Time(loc).Format(layout)
	assert.Equal(t, t10, v10)

	assert.Equal(t, t11, batch.Vector.([]api.Timestamp)[10].Time(loc).Format(layout))
	assert.Equal(t, t12, batch.Vector.([]api.Timestamp)[11].Time(loc).Format(layout))

	if err := reader.Close(); err != nil {
		t.Fatalf("%+v", err)
	}
}

func TestStringAndBinaryStatistics(t *testing.T) {
	opts := orc.DefaultReaderOptions()

	reader, err := orc.NewFileReader("testStringAndBinaryStatistics.0.12.orc", opts)
	if err != nil {
		t.Errorf("create reader error: %+v", err)
	}

	schema := reader.GetSchema()
	log.Debugf("schema: %s", schema.String())

	// check the stats
	stats:= reader.GetStatistics()
	assert.Equal(t, 4, int(stats[0].GetNumberOfValues()))
	assert.Equal(t, 15, int(stats[1].GetBinaryStatistics().GetSum()))

	batch := schema.CreateReaderBatch(opts)

	if err := reader.Next(batch); err != nil {
		t.Fatalf("%+v", err)
	}

	assert.Equal(t, 4, batch.ReadRows)

	if err=reader.Close();err!=nil {
		t.Fatalf("%+v", err)
	}
}

/*func BenchmarkReader(b *testing.B) {
	path := "/u01/apache/orc/java/bench/data/generated/taxi/orc.gz"

	ropts := DefaultReaderOptions()
	reader, err := NewReader(path, ropts)
	if err != nil {
		b.Fatalf("create reader error %+v", err)
	}

	schema := reader.GetSchema()

	stripes, err := reader.Stripes()
	if err != nil {
		b.Fatalf("%+v", err)
	}

	ropts.RowSize = 100000
	column, err := schema.CreateReaderBatch(ropts)
	if err != nil {
		b.Fatalf("create row column error %+v", err)
	}

	var rows int

	i := 0
	stripeR := stripes[0]

	for next := true; next; {
		next, err = stripeR.NextBatch(column)
		if err != nil {
			b.Fatalf("%+v", err)
		}
		rows += column.Rows()
		fmt.Printf("current stripeR %d, rows now: %d\n", i, rows)
	}

	reader.Close()
}
*/
