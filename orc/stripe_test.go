package orc

import (
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/stretchr/testify/assert"
	"testing"
)

/*func init() {
	log.SetLevel(log.TraceLevel)
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

	if _, err := reader.next(&readBatch); err != nil {
		t.Fatalf("%+v", err)
	}

	assert.Equal(t, values, readBatch.Vector)
}

func TestStructWithPresents (t *testing.T) {
	schema := api.TypeDescription{Id: 0, Kind: pb.Type_STRUCT, HasNulls: true}
	schema.Encoding = pb.ColumnEncoding_DIRECT
	child1 := api.TypeDescription{Id: 0, Kind: pb.Type_INT}
	child1.Encoding = pb.ColumnEncoding_DIRECT_V2
	schema.Children= []*api.TypeDescription{&child1}
	schemas, _:= schema.Normalize()

	wopts := config.DefaultWriterOptions()
	wbatch := api.CreateWriterBatch(schema, wopts)

	rows := 100

	values := make([]api.Value, rows)
	values[0].Null = true
	values[45].Null = true
	values[99].Null = true
	wbatch.Vector= values

	childValues := make([]api.Value, rows)
	for i:=0; i<rows;i++ {
		childValues[i].V= int64(i)
	}
	childValues[0].Null= true
	childValues[0].V= nil
	childValues[45].Null=true
	childValues[45].V= nil
	childValues[99].Null=true
	childValues[99].V= nil
	wbatch.Children[0].Vector=childValues

	buf := make([]byte, 5000)
	f := orcio.NewMockFile(buf)

	writer, err := newStripeWriter(f, 0, schemas, &wopts)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if err := writer.write(&wbatch); err != nil {
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

	rBatch := api.CreateReaderBatch(schema, ropts)

	if _, err := reader.next(&rBatch); err != nil {
		t.Fatalf("%+v", err)
	}

	assert.Equal(t, values, rBatch.Vector)
	assert.Equal(t, childValues, rBatch.Children[0].Vector)
}