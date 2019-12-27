package orc

import (
	"github.com/PatrickHuang888/goorc/pb/pb"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

var workDir = os.TempDir() + string(os.PathSeparator)

func TestDecimalWriter(t *testing.T) {
	path := workDir + "TestOrcDecimal.orc"

	schema := &TypeDescription{Kind: pb.Type_STRUCT}
	x := &TypeDescription{Kind: pb.Type_DECIMAL}
	schema.ChildrenNames = []string{"x"}
	schema.Children = []*TypeDescription{x}
	wopts := DefaultWriterOptions()
	writer, err := NewWriter(path, schema, wopts)
	if err != nil {
		t.Fatalf("create writer error %+v", err)
	}

	vector := make([]Decimal64, 19)
	vector[0] = Decimal64{1, 3}
	for i := 1; i < 18; i++ {
		vector[i] = Decimal64{int64(i-1) * 10, 3}
	}
	vector[18] = Decimal64{-2000, 3}

	batch := schema.CreateWriterBatch(wopts)
	batch.Vector = vector

	if err := writer.Write(batch); err != nil {
		t.Fatalf("%+v", err)
	}

	writer.Close()

	ropts := DefaultReaderOptions()
	reader, err := NewReader(path, ropts)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	schema = reader.GetSchema()
	stripes, err := reader.Stripes()
	if err != nil {
		t.Fatalf("%+v", err)
	}

	for _, stripe := range stripes {
		batch := schema.CreateReaderBatch(ropts)

		err = stripe.NextBatch(batch)
		if err != nil {
			t.Fatalf("%+v", err)
		}

		values:= batch.Vector.([]*ColumnVector)[0].([]Decimal64)
		assert.Equal(t, 19, len(values))
		assert.Equal(t, 3, int(values[0].Scale))
		assert.Equal(t, 1, int(values[0].Precision), "row 0")
		for i := 1; i < 18; i++ {
			assert.Equal(t, 10*int64(i-1), x.Vector[i].Precision)
		}

		assert.Equal(t, -2000, int(x.Vector[18].Precision))
	}

	reader.Close()
}

func TestTimestamp(test *testing.T) {
	schema := &TypeDescription{Kind: pb.Type_TIMESTAMP}
	wopts := DefaultWriterOptions()
	writer, err := NewWriter(workDir+"testTimestamp.orc", schema, wopts)
	if err != nil {
		test.Fatalf("%+v", err)
	}
	wbatch, err := schema.CreateWriterBatch(wopts)
	if err != nil {
		test.Fatalf("%+v", err)
	}
	var vector []Timestamp
	layout := "2006-01-01 00:00:00.999999999"
	t1, _ := time.Parse(layout, "2037-01-01 00:00:00.000999")
	vector = append(vector, GetTimestamp(t1))
	t2, _ := time.Parse(layout, "2003-01-01 00:00:00.000000222")
	vector = append(vector, GetTimestamp(t2))
	t3, _ := time.Parse(layout, "1999-01-01 00:00:00.999999999")
	vector = append(vector, GetTimestamp(t3))
	t4, _ := time.Parse(layout, "1995-01-01 00:00:00.688888888")
	vector = append(vector, GetTimestamp(t4))
	t5, _ := time.Parse(layout, "2002-01-01 00:00:00.1")
	vector = append(vector, GetTimestamp(t5))
	t6, _ := time.Parse(layout, "2010-03-02 00:00:00.000009001")
	vector = append(vector, GetTimestamp(t6))
	t7, _ := time.Parse(layout, "2005-01-01 00:00:00.000002229")
	vector = append(vector, GetTimestamp(t7))
	t8, _ := time.Parse(layout, "2006-01-01 00:00:00.900203003")
	vector = append(vector, GetTimestamp(t8))
	t9, _ := time.Parse(layout, "2003-01-01 00:00:00.800000007")
	vector = append(vector, GetTimestamp(t9))
	t10, _ := time.Parse(layout, "1996-08-02 00:00:00.723100809")
	vector = append(vector, GetTimestamp(t10))
	t11, _ := time.Parse(layout, "1998-11-02 00:00:00.857340643")
	vector = append(vector, GetTimestamp(t11))
	t12, _ := time.Parse(layout, "2008-10-02 00:00:00")
	vector = append(vector, GetTimestamp(t12))

	wbatch.(*TimestampColumn).Vector = vector
	if err := writer.Write(wbatch); err != nil {
		test.Fatalf("%+v", err)
	}
	writer.Close()

	ropts := DefaultReaderOptions()
	reader, err := NewReader(workDir+"testTimestamp.orc", ropts)
	if err != nil {
		test.Fatalf("%+v", err)
	}

	schema = reader.GetSchema()
	stripes, err := reader.Stripes()
	if err != nil {
		test.Fatalf("%+v", err)
	}

	rbatch, err := schema.CreateReaderBatch(ropts)
	if err != nil {
		test.Fatalf("fail create batch %+v", err)
	}

	_, err = stripes[0].NextBatch(rbatch)
	if err != nil {
		test.Fatalf("%+v", err)
	}

	assert.Equal(test, 12, rbatch.Rows())
	assert.Equal(test, GetTimestamp(t1).Seconds, rbatch.(*TimestampColumn).Vector[0].Seconds)
	assert.Equal(test, GetTimestamp(t1).Nanos, rbatch.(*TimestampColumn).Vector[0].Nanos)
	assert.Equal(test, GetTimestamp(t2).Seconds, rbatch.(*TimestampColumn).Vector[1].Seconds)
	assert.Equal(test, GetTimestamp(t2).Nanos, rbatch.(*TimestampColumn).Vector[1].Nanos)
	assert.Equal(test, GetTimestamp(t3).Seconds, rbatch.(*TimestampColumn).Vector[2].Seconds)
	assert.Equal(test, GetTimestamp(t3).Nanos, rbatch.(*TimestampColumn).Vector[2].Nanos)
	assert.Equal(test, GetTimestamp(t4).Seconds, rbatch.(*TimestampColumn).Vector[3].Seconds)
	assert.Equal(test, GetTimestamp(t4).Nanos, rbatch.(*TimestampColumn).Vector[3].Nanos)
	assert.Equal(test, GetTimestamp(t5).Seconds, rbatch.(*TimestampColumn).Vector[4].Seconds)
	assert.Equal(test, GetTimestamp(t5).Nanos, rbatch.(*TimestampColumn).Vector[4].Nanos)
	assert.Equal(test, GetTimestamp(t6).Seconds, rbatch.(*TimestampColumn).Vector[5].Seconds)
	assert.Equal(test, GetTimestamp(t6).Nanos, rbatch.(*TimestampColumn).Vector[5].Nanos)
	assert.Equal(test, GetTimestamp(t7).Seconds, rbatch.(*TimestampColumn).Vector[6].Seconds)
	assert.Equal(test, GetTimestamp(t7).Nanos, rbatch.(*TimestampColumn).Vector[6].Nanos)
	assert.Equal(test, GetTimestamp(t8).Seconds, rbatch.(*TimestampColumn).Vector[7].Seconds)
	assert.Equal(test, GetTimestamp(t8).Nanos, rbatch.(*TimestampColumn).Vector[7].Nanos)
	assert.Equal(test, GetTimestamp(t9).Seconds, rbatch.(*TimestampColumn).Vector[8].Seconds)
	assert.Equal(test, GetTimestamp(t9).Nanos, rbatch.(*TimestampColumn).Vector[8].Nanos)
	assert.Equal(test, GetTimestamp(t10).Seconds, rbatch.(*TimestampColumn).Vector[9].Seconds)
	assert.Equal(test, GetTimestamp(t10).Nanos, rbatch.(*TimestampColumn).Vector[9].Nanos)
	assert.Equal(test, GetTimestamp(t11).Seconds, rbatch.(*TimestampColumn).Vector[10].Seconds)
	assert.Equal(test, GetTimestamp(t11).Nanos, rbatch.(*TimestampColumn).Vector[10].Nanos)
	assert.Equal(test, GetTimestamp(t12).Seconds, rbatch.(*TimestampColumn).Vector[11].Seconds)
	assert.Equal(test, GetTimestamp(t12).Nanos, rbatch.(*TimestampColumn).Vector[11].Nanos)
	reader.Close()

}
