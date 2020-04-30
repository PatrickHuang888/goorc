package testing

import (
	"github.com/patrickhuang888/goorc/orc"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
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

	min:= values[0]
	max:= values[0]
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
	stripes, err := reader.Stripes()
	if err != nil {
		t.Fatalf("%+v", err)
	}
	batch := schema.CreateReaderBatch(opts)

	err = stripes[0].Next(batch)
	if err != nil {
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
	stripes, err := reader.Stripes()
	if err != nil {
		t.Fatalf("%+v", err)
	}

	batch := schema.CreateReaderBatch(opts)
	if err != nil {
		t.Errorf("create row column error %+v", err)
	}

	err = stripes[0].Next(batch)
	if err != nil {
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
	stripes, err := reader.Stripes()
	if err != nil {
		t.Fatalf("%+v", err)
	}

	batch := schema.CreateReaderBatch(opts)

	err = stripes[0].Next(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	reader.Close()

	assert.Equal(t, values, batch.Vector)
}

func TestStructs(t *testing.T)  {
	opts := orc.DefaultReaderOptions()

	reader, err := orc.NewFileReader("testStructs.0.12.orc", opts)
	if err != nil {
		t.Errorf("create reader error: %+v", err)
	}

	schema := reader.GetSchema()
	log.Debugf("schema: %s", schema.String())

	batch:=schema.CreateReaderBatch(opts)

	if err:= reader.Next(batch);err!=nil {
		t.Fatalf("%+v", err)
	}

	assert.Equal(t, 1024, batch.ReadRows)

	if err:=reader.Close();err!=nil {
		t.Fatalf("%+v", err)
	}

	cl1:= batch.Vector.([]*orc.ColumnVector)[0]
	for i:=0; i<1024; i++ {
		if i<200 || (i>= 400 && i <600) || i >=800 {
			assert.Equal(t, false, cl1.Presents[i])
		}else {
			assert.Equal(t, true, cl1.Presents[i])
			cl2:=  cl1.Vector.([]*orc.ColumnVector)[0]
			vv:= cl2.Vector.([]int64)
			assert.Equal(t, i, int(vv[i]))
		}
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
