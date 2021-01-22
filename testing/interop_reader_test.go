package testing

import (
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"

	"github.com/patrickhuang888/goorc/orc"
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
)

func init() {
	orc.SetLogLevel(log.DebugLevel)
	//stream.SetLogLevel(log.TraceLevel)
}

func TestBasicNoCompression(t *testing.T) {
	opts := config.DefaultReaderOptions()
	reader, err := orc.NewOSFileReader("basicLongNoCompression.orc", opts)
	if err != nil {
		t.Errorf("create reader error: %+v", err)
	}

	schema := reader.GetSchema()
	batch, err := api.CreateReaderBatch(schema, opts)
	assert.Nil(t, err)

	var vector []api.Value
	for ; ; {
		if err = reader.Next(&batch); err != nil {
			t.Fatalf("%+v", err)
		}
		if batch.Len() == 0 {
			break
		}
		vector = append(vector, batch.Vector...)
	}

	assert.Equal(t, 90, len(vector))

	min := vector[0].V.(int64)
	max := vector[0].V.(int64)
	for _, vec := range vector {
		v := vec.V.(int64)
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	assert.Equal(t, 1, int(min))
	assert.Equal(t, 2000, int(max))

	reader.Close()
}

func TestPatchBaseNegativeMinNoCompression(t *testing.T) {
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

	opts := config.DefaultReaderOptions()
	reader, err := orc.NewOSFileReader("patchBaseNegativeMin.orc", opts)
	if err != nil {
		t.Errorf("create reader error: %+v", err)
	}
	schema := reader.GetSchema()
	batch, err := api.CreateReaderBatch(schema, opts)
	assert.Nil(t, err)

	var vector []int64
	for ; ; {
		if err := reader.Next(&batch); err != nil {
			t.Fatalf("%+v", err)
		}
		if len(batch.Vector) == 0 {
			break
		}
		for _, v := range batch.Vector {
			vector = append(vector, v.V.(int64))
		}
	}

	reader.Close()

	assert.Equal(t, values, vector)
}

func TestPatchBaseNegativeMin2NoCompression(t *testing.T) {
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

	opts := config.DefaultReaderOptions()
	reader, err := orc.NewOSFileReader("patchBaseNegativeMin2.orc", opts)
	if err != nil {
		t.Errorf("create reader error: %+v", err)
	}
	schema := reader.GetSchema()

	batch, err := api.CreateReaderBatch(schema, opts)
	assert.Nil(t, err)

	var vector []int64
	for ; ; {
		if err = reader.Next(&batch); err != nil {
			t.Fatalf("%+v", err)
		}
		if batch.Len() == 0 {
			break
		}
		for _, v := range batch.Vector {
			vector = append(vector, v.V.(int64))
		}
	}

	reader.Close()

	assert.Equal(t, values, vector)
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

	opts := config.DefaultReaderOptions()
	reader, err := orc.NewOSFileReader("patchBaseNegativeMin3.orc", opts)
	if err != nil {
		t.Errorf("create reader error: %+v", err)
	}
	schema := reader.GetSchema()
	batch, err := api.CreateReaderBatch(schema, opts)
	assert.Nil(t, err)

	var vector []int64
	for ; ; {
		if err = reader.Next(&batch); err != nil {
			t.Fatalf("%+v", err)
		}
		if batch.Len() == 0 {
			break
		}
		for _, v := range batch.Vector {
			vector = append(vector, v.V.(int64))
		}
	}

	reader.Close()

	assert.Equal(t, values, vector)
}

func TestStructs(t *testing.T) {
	opts := config.DefaultReaderOptions()
	reader, err := orc.NewOSFileReader("testStructs.0.12.orc", opts)
	if err != nil {
		t.Fatalf("create reader error: %+v", err)
	}

	schema := reader.GetSchema()
	batch, err := api.CreateReaderBatch(schema, opts)
	assert.Nil(t, err)

	if err := reader.Next(&batch); err != nil {
		t.Fatalf("%+v", err)
	}

	assert.Equal(t, 1024, batch.Len())

	reader.Close()

	vector := batch.Children[0].Children[0].Vector // struct/struct/long
	for i := 0; i < 1024; i++ {
		if i < 200 || (i >= 400 && i < 600) || i >= 800 {
			assert.Equal(t, true, vector[i].Null)
		} else {
			v := vector[i].V.(int64)
			assert.Equal(t, i, int(v))
		}
	}

}

func TestTimestamp(t *testing.T) {
	opts := config.DefaultReaderOptions()

	reader, err := orc.NewOSFileReader("testTimestamp.0.12.orc", opts)
	if err != nil {
		t.Errorf("create reader error: %+v", err)
	}

	schema := reader.GetSchema()
	log.Debugf("schema: %s", schema.String())

	batch, err := api.CreateReaderBatch(schema, opts)
	assert.Nil(t, err)

	if err := reader.Next(&batch); err != nil {
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

	assert.Equal(t, len(values), batch.Len())

	layout := "2006-01-02 15:04:05.999999999"
	loc, _ := time.LoadLocation("US/Pacific") //data write with us/pacific locale

	v1 := batch.Vector[0].V.(api.Timestamp)
	v1.Loc = loc
	assert.Equal(t, t1, v1.Time().Format(layout))
	v2 := batch.Vector[1].V.(api.Timestamp)
	v2.Loc = loc
	assert.Equal(t, t2, v2.Time().Format(layout))
	v3 := batch.Vector[2].V.(api.Timestamp)
	v3.Loc = loc
	assert.Equal(t, t3, v3.Time().Format(layout))
	v4 := batch.Vector[3].V.(api.Timestamp)
	v4.Loc = loc
	assert.Equal(t, t4, v4.Time().Format(layout))
	v5 := batch.Vector[4].V.(api.Timestamp)
	v5.Loc = loc
	assert.Equal(t, t5, v5.Time().Format(layout))

	/*
		assert.Equal(t, t6, batch.Vector.([]api.Timestamp)[5].Time(loc).Format(layout))
		assert.Equal(t, t7, batch.Vector.([]api.Timestamp)[6].Time(loc).Format(layout))
		assert.Equal(t, t8, batch.Vector.([]api.Timestamp)[7].Time(loc).Format(layout))
		assert.Equal(t, t9, batch.Vector.([]api.Timestamp)[8].Time(loc).Format(layout))*/

	// data written has daylight saving
	// todo: daylight saving
	//v10 := batch.Vector[9].V.(api.Timestamp)
	//assert.Equal(t, t10, v10.Time().Format(layout))

	/*assert.Equal(t, t11, batch.Vector.([]api.Timestamp)[10].Time(loc).Format(layout))
	assert.Equal(t, t12, batch.Vector.([]api.Timestamp)[11].Time(loc).Format(layout))*/

	reader.Close()
}

/*func TestStringAndBinaryStatistics(t *testing.T) {
	opts := orc.DefaultReaderOptions()

	reader, err := orc.NewFileReader("testStringAndBinaryStatistics.0.12.orc", opts)
	if err != nil {
		t.Errorf("create reader error: %+v", err)
	}

	schema := reader.GetSchema()
	log.Debugf("schema: %s", schema.String())

	// check the stats
	stats := reader.GetStatistics()
	assert.Equal(t, 4, int(stats[0].GetNumberOfValues()))
	assert.Equal(t, 15, int(stats[1].GetBinaryStatistics().GetSum()))

	batch := schema.CreateReaderBatch(opts)

	if err := reader.Next(batch); err != nil {
		t.Fatalf("%+v", err)
	}

	assert.Equal(t, 4, batch.ReadRows)

	if err = reader.Close(); err != nil {
		t.Fatalf("%+v", err)
	}
}*/

/*func TestSeek(t *testing.T) {
	opts := config.DefaultReaderOptions()

	reader, err := orc.NewOSFileReader("testSeek.0.12.orc", opts)
	defer reader.Close()
	if err != nil {
		t.Errorf("create reader error: %+v", err)
	}

	nor := reader.NumberOfRows()
	log.Infof("number of rows %d", nor)

	schema := reader.GetSchema()
	batch, err := api.CreateReaderBatch(schema, opts)
	assert.Nil(t, err)

	count:=0

	for {
		if err := reader.Next(&batch); err != nil {
			t.Fatalf("%+v", err)
		}
		if batch.Len()==0 {
			break
		}else {
			count += batch.Len()
		}
	}

	fmt.Printf("read rows %d\n", count)
	assert.Equal(t, int(nor), count)

	if err:=reader.Seek(100);err!=nil {
		t.Fatalf("%+v", err)
	}

	//if err:=reader.Seek(1500);err!=nil {
	//	t.Fatalf("%+v", err)
	//}
}*/

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
