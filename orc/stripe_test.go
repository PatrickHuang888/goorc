package orc

import (
	"fmt"
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/pb/pb"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
)

func init() {
	log.SetLevel(log.TraceLevel)
}

/*func TestStripeStructBasic(t *testing.T) {

	schema := &api.TypeDescription{Kind: pb.Type_STRUCT}
	x := &api.TypeDescription{Kind: pb.Type_DECIMAL, Encoding: pb.ColumnEncoding_DIRECT_V2}
	schema.ChildrenNames = []string{"x"}
	schema.Children = []*api.TypeDescription{x}

	wopts := config.DefaultWriterOptions()
	writer, err := newStripeWriter(0, schema, wopts)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	vector := make([]api.Decimal64, 19)
	vector[0] = api.Decimal64{1, 3}
	for i := 1; i < 18; i++ {
		vector[i] = api.Decimal64{int64(i-1) * 10, 3}
	}
	vector[18] = api.Decimal64{-2000, 3}

	batch := schema.CreateWriterBatch(wopts)
	batch.Vector.([]*api.ColumnVector)[0].Vector = vector

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
	sr, err := newStripeReader(dummyOut, schema.normalize(), ropts, 0, writer.info, footer)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	rbatch := schema.CreateReaderBatch(ropts)

	if err := sr.next(rbatch); err != nil {
		t.Fatalf("%+v", err)
	}

	values := batch.Vector.([]*api.ColumnVector)[0].Vector.([]api.Decimal64)
	assert.Equal(t, 19, len(values))
	assert.Equal(t, 3, int(values[0].Scale))
	assert.Equal(t, 1, int(values[0].Precision), "row 0")
	for i := 1; i < 18; i++ {
		assert.Equal(t, 10*int64(i-1), values[i].Precision)
	}
	assert.Equal(t, -2000, int(values[18].Precision))
}*/

func TestStripeBasic(t *testing.T) {
	//schema := api.TypeDescription{Kind: pb.Type_TIMESTAMP, Encoding: pb.ColumnEncoding_DIRECT_V2}
	schema := api.TypeDescription{Kind: pb.Type_BYTE, Encoding: pb.ColumnEncoding_DIRECT}
	schemas, err := schema.Normalize()
	assert.Nil(t, err)

	buf := make([]byte, 500)
	f := orcio.NewMockFile(buf)

	wopts := config.DefaultWriterOptions()
	batch := api.CreateWriterBatch(schema, wopts)

	rows := 104

	values := make([]api.Value, rows)
	for i := 0; i < rows; i++ {
		values[i].V = byte(i)
	}
	/*layout := "2006-01-01 00:00:00.999999999"
	v1, _ := time.Parse(layout, "2037-01-01 00:00:00.000999")
	vector = append(vector, api.Value{V:api.GetTimestamp(v1)})
	v2, _ := time.Parse(layout, "2003-01-01 00:00:00.000000222")
	vector = append(vector, api.Value{V:api.GetTimestamp(v2)})
	v3, _ := time.Parse(layout, "1999-01-01 00:00:00.999999999")
	vector = append(vector, api.Value{V:api.GetTimestamp(v3)})
	v4, _ := time.Parse(layout, "1995-01-01 00:00:00.688888888")
	vector = append(vector, api.Value{V:api.GetTimestamp(v4)})
	v5, _ := time.Parse(layout, "2002-01-01 00:00:00.1")
	vector = append(vector, api.Value{V:api.GetTimestamp(v5)})
	v6, _ := time.Parse(layout, "2010-03-02 00:00:00.000009001")
	vector = append(vector, api.Value{V:api.GetTimestamp(v6)})
	v7, _ := time.Parse(layout, "2005-01-01 00:00:00.000002229")
	vector = append(vector, api.Value{V:api.GetTimestamp(v7)})
	v8, _ := time.Parse(layout, "2006-01-01 00:00:00.900203003")
	vector = append(vector, api.Value{V:api.GetTimestamp(v8)})
	v9, _ := time.Parse(layout, "2003-01-01 00:00:00.800000007")
	vector = append(vector, api.Value{V:api.GetTimestamp(v9)})
	v10, _ := time.Parse(layout, "1996-08-02 00:00:00.723100809")
	vector = append(vector, api.Value{V:api.GetTimestamp(v10)})
	v11, _ := time.Parse(layout, "1998-11-02 00:00:00.857340643")
	vector = append(vector, api.Value{V:api.GetTimestamp(v11)})
	v12, _ := time.Parse(layout, "2008-10-02 00:00:00")
	vector = append(vector, api.Value{V:api.GetTimestamp(v12)})*/

	batch.Vector = values

	writer, err := newStripeWriter(f, 0, schemas, &wopts)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if err := writer.write(&batch); err != nil {
		t.Fatalf("%+v", err)
	}
	if err := writer.flushOut(); err != nil {
		t.Fatalf("%+v", err)
	}

	ropts := config.DefaultReaderOptions()
	reader, err := newStripeReader(f, schemas, &ropts, 0, writer.info)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	readBatch := api.CreateReaderBatch(schema, ropts)

	if err := reader.Next(&readBatch); err != nil {
		t.Fatalf("%+v", err)
	}

	assert.Equal(t, values, readBatch.Vector)
}

func TestMultipleStripes(t *testing.T) {
	schema := api.TypeDescription{Id: 0, Kind: pb.Type_STRING, Encoding: pb.ColumnEncoding_DIRECT_V2, HasNulls: false}
	schemas, err := schema.Normalize()
	assert.Nil(t, err)

	wopts := config.DefaultWriterOptions()
	wopts.CompressionKind = pb.CompressionKind_ZLIB
	wopts.StripeSize = 50_000
	batch := api.CreateWriterBatch(schema, wopts)

	rows := 600
	values := make([]api.Value, rows)
	for i := 0; i < rows; i++ {
		values[i].V = fmt.Sprintf("string %d Because the number of nanoseconds often has a large number of trailing zeros", i)
	}
	batch.Vector = values

	buf := make([]byte, 2_000_000)
	f := orcio.NewMockFile(buf)

	writer, err := newStripeWriter(f, 0, schemas, &wopts)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if err := writer.write(&batch); err != nil {
		t.Fatalf("%+v", err)
	}
	if err := writer.flushOut(); err != nil {
		t.Fatalf("%+v", err)
	}


}