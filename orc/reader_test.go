package orc

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
)

func init() {
	logrus.SetLevel(logrus.InfoLevel)
}

func TestNoCompression(t *testing.T) {
	opts := DefaultReaderOptions()
	reader, err := NewReader("../testing/basicLongNoCompression.orc", opts)
	if err != nil {
		t.Errorf("create reader error: %+v", err)
	}

	schema := reader.GetSchema()
	stripes, err := reader.Stripes()
	if err != nil {
		t.Fatalf("%+v", err)
	}

	for _, stripe := range stripes {
		batch, err := schema.CreateReaderBatch(opts)
		if err != nil {
			t.Errorf("create row batch error %+v", err)
		}

		for next := true; next; {
			next, err = stripe.NextBatch(batch)
			if err != nil {
				t.Fatalf("%+v", err)
			}
		}
	}
}

func TestPatchBaseNegativeMin(t *testing.T) {
	inp := []int64{
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

	opts := DefaultReaderOptions()
	reader, err := NewReader("../testing/patchBaseNegativeMin.orc", opts)
	if err != nil {
		t.Errorf("create reader error: %+v", err)
	}
	schema := reader.GetSchema()
	stripes, err := reader.Stripes()
	if err != nil {
		t.Fatalf("%+v", err)
	}
	stripe := stripes[0]
	batch, err := schema.CreateReaderBatch(opts)
	if err != nil {
		t.Errorf("create row batch error %+v", err)
	}

	_, err = stripe.NextBatch(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	assert.Equal(t, inp, batch.(*LongColumn).Vector)

	reader.Close()
}

func TestPatchBaseNegativeMin2(t *testing.T) {
	inp := []int64{
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

	opts := DefaultReaderOptions()
	reader, err := NewReader("../testing/patchBaseNegativeMin2.orc", opts)
	if err != nil {
		t.Errorf("create reader error: %+v", err)
	}
	schema := reader.GetSchema()
	stripes, err := reader.Stripes()
	if err != nil {
		t.Fatalf("%+v", err)
	}
	stripe := stripes[0]
	batch, err := schema.CreateReaderBatch(opts)
	if err != nil {
		t.Errorf("create row batch error %+v", err)
	}

	_, err = stripe.NextBatch(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	assert.Equal(t, inp, batch.(*LongColumn).Vector)

	reader.Close()
}

func TestPatchBaseNegativeMin3(t *testing.T) {
	inp := []int64{
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

	opts := DefaultReaderOptions()
	reader, err := NewReader("../testing/patchBaseNegativeMin3.orc", opts)
	if err != nil {
		t.Errorf("create reader error: %+v", err)
	}
	schema := reader.GetSchema()
	stripes, err := reader.Stripes()
	if err != nil {
		t.Fatalf("%+v", err)
	}
	stripe := stripes[0]
	batch, err := schema.CreateReaderBatch(opts)
	if err != nil {
		t.Errorf("create row batch error %+v", err)
	}

	_, err = stripe.NextBatch(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	assert.Equal(t, inp, batch.(*LongColumn).Vector)

	reader.Close()
}

func BenchmarkReader(b *testing.B) {
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
	batch, err := schema.CreateReaderBatch(ropts)
	if err != nil {
		b.Fatalf("create row batch error %+v", err)
	}

	var rows int

	i := 0
	stripe := stripes[0]

	for next := true; next; {
		next, err = stripe.NextBatch(batch)
		if err != nil {
			b.Fatalf("%+v", err)
		}
		rows += batch.Rows()
		fmt.Printf("current stripe %d, rows now: %d\n", i, rows)
	}

	reader.Close()
}