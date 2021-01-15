package orc

import (
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStripeBasic(t *testing.T) {
	schema := &api.TypeDescription{Kind: pb.Type_BYTE, Encoding: pb.ColumnEncoding_DIRECT}

	buf := make([]byte, 500)
	f := orcio.NewMockFile(buf)

	wopts := config.DefaultWriterOptions()
	wopts.CreateVector= false
	batch, err := api.CreateWriterBatch(schema, wopts)

	rows := 104

	values := make([]api.Value, rows)
	for i := 0; i < rows; i++ {
		values[i].V = byte(i)
	}
	batch.Vector = values

	schemas, err:= schema.Flat()
	assert.Nil(t, err)
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

	readBatch, err := api.CreateReaderBatch(schema, ropts)
	assert.Nil(t, err)

	if _, err := reader.next(&readBatch); err != nil {
		t.Fatalf("%+v", err)
	}

	assert.Equal(t, values, readBatch.Vector)
}

func TestStructWithPresents (t *testing.T) {
	schema := &api.TypeDescription{Id: 0, Kind: pb.Type_STRUCT, HasNulls: true}
	schema.Encoding = pb.ColumnEncoding_DIRECT
	child1 := api.TypeDescription{Id: 0, Kind: pb.Type_INT}
	child1.Encoding = pb.ColumnEncoding_DIRECT_V2
	schema.Children= []*api.TypeDescription{&child1}
	err:=api.NormalizeSchema(schema)
	assert.Nil(t, err)

	wopts := config.DefaultWriterOptions()
	wopts.CreateVector= false
	wbatch, err := api.CreateWriterBatch(schema, wopts)
	assert.Nil(t, err)

	rows := 100

	values := make([]api.Value, rows)
	values[0].Null = true
	values[45].Null = true
	values[99].Null = true
	wbatch.Vector= values

	childValues := make([]api.Value, rows)
	for i:=0; i<rows;i++ {
		childValues[i].V= int32(i)
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

	schemas, err:= schema.Flat()
	assert.Nil(t, err)
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

	rBatch, err := api.CreateReaderBatch(schema, ropts)
	assert.Nil(t, err)

	if _, err := reader.next(&rBatch); err != nil {
		t.Fatalf("%+v", err)
	}

	assert.Equal(t, values, rBatch.Vector)
	assert.Equal(t, childValues, rBatch.Children[0].Vector)
}