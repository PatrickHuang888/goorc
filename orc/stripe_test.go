package orc

import (
	"bytes"
	"testing"
	"time"

	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/stretchr/testify/assert"
)

var dummyOut = &bufSeeker{&bytes.Buffer{}}

func TestStripeStructBasic(t *testing.T) {

	schema := &TypeDescription{Kind: pb.Type_STRUCT}
	x := &TypeDescription{Kind: pb.Type_DECIMAL, Encoding: pb.ColumnEncoding_DIRECT_V2}
	schema.ChildrenNames = []string{"x"}
	schema.Children = []*TypeDescription{x}
	schemas := schema.normalize()

	wopts := DefaultWriterOptions()
	writer, err := newStripe(0, schemas, wopts)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	vector := make([]Decimal64, 19)
	vector[0] = Decimal64{1, 3}
	for i := 1; i < 18; i++ {
		vector[i] = Decimal64{int64(i-1) * 10, 3}
	}
	vector[18] = Decimal64{-2000, 3}

	batch := schema.CreateWriterBatch(wopts)
	batch.Vector.([]*ColumnVector)[0].Vector = vector

	if err := writer.writeColumn(batch); err != nil {
		t.Fatalf("%+v", err)
	}
	dummyOut.Reset()
	if err := writer.writeout(dummyOut); err != nil {
		t.Fatalf("%+v", err)
	}
	footer, err := writer.writeFooter(dummyOut)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	ropts := DefaultReaderOptions()
	sr := &stripeReader{in: dummyOut, opts: ropts, footer: footer, schemas: schemas, info: writer.info, idx: 0}
	if err := sr.prepare(); err != nil {
		t.Fatalf("%+v", err)
	}

	rbatch := schema.CreateReaderBatch(ropts)

	if err := sr.Next(rbatch); err != nil {
		t.Fatalf("%+v", err)
	}

	values := batch.Vector.([]*ColumnVector)[0].Vector.([]Decimal64)
	assert.Equal(t, 19, len(values))
	assert.Equal(t, 3, int(values[0].Scale))
	assert.Equal(t, 1, int(values[0].Precision), "row 0")
	for i := 1; i < 18; i++ {
		assert.Equal(t, 10*int64(i-1), values[i].Precision)
	}
	assert.Equal(t, -2000, int(values[18].Precision))
}

func TestStripeBasic(t *testing.T) {
	schema := &TypeDescription{Kind: pb.Type_TIMESTAMP, Encoding: pb.ColumnEncoding_DIRECT_V2}
	schemas := schema.normalize()
	wopts := DefaultWriterOptions()
	writer, err := newStripe(0, schemas, wopts)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	batch := schema.CreateWriterBatch(wopts)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	var vector []Timestamp
	layout := "2006-01-01 00:00:00.999999999"
	v1, _ := time.Parse(layout, "2037-01-01 00:00:00.000999")
	vector = append(vector, GetTimestamp(v1))
	v2, _ := time.Parse(layout, "2003-01-01 00:00:00.000000222")
	vector = append(vector, GetTimestamp(v2))
	v3, _ := time.Parse(layout, "1999-01-01 00:00:00.999999999")
	vector = append(vector, GetTimestamp(v3))
	v4, _ := time.Parse(layout, "1995-01-01 00:00:00.688888888")
	vector = append(vector, GetTimestamp(v4))
	v5, _ := time.Parse(layout, "2002-01-01 00:00:00.1")
	vector = append(vector, GetTimestamp(v5))
	v6, _ := time.Parse(layout, "2010-03-02 00:00:00.000009001")
	vector = append(vector, GetTimestamp(v6))
	t7, _ := time.Parse(layout, "2005-01-01 00:00:00.000002229")
	vector = append(vector, GetTimestamp(t7))
	v8, _ := time.Parse(layout, "2006-01-01 00:00:00.900203003")
	vector = append(vector, GetTimestamp(v8))
	v9, _ := time.Parse(layout, "2003-01-01 00:00:00.800000007")
	vector = append(vector, GetTimestamp(v9))
	v10, _ := time.Parse(layout, "1996-08-02 00:00:00.723100809")
	vector = append(vector, GetTimestamp(v10))
	v11, _ := time.Parse(layout, "1998-11-02 00:00:00.857340643")
	vector = append(vector, GetTimestamp(v11))
	v12, _ := time.Parse(layout, "2008-10-02 00:00:00")
	vector = append(vector, GetTimestamp(v12))

	batch.Vector = vector

	if err := writer.writeColumn(batch); err != nil {
		t.Fatalf("%+v", err)
	}

	dummyOut.Reset()
	if err := writer.writeout(dummyOut); err != nil {
		t.Fatalf("%+v", err)
	}
	footer, err := writer.writeFooter(dummyOut)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	ropts := DefaultReaderOptions()

	sr := &stripeReader{in: dummyOut, opts: ropts, footer: footer, schemas: schemas, info: writer.info, idx: 0}
	if err := sr.prepare(); err != nil {
		t.Fatalf("%+v", err)
	}

	rbatch := schema.CreateReaderBatch(ropts)

	if err := sr.Next(rbatch); err != nil {
		t.Fatalf("%+v", err)
	}

	values := rbatch.Vector.([]Timestamp)
	assert.Equal(t, 12, len(values))
	assert.Equal(t, vector, values)
}
