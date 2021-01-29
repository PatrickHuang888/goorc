package orc

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/pb/pb"
)

func init() {
	logger.SetLevel(log.DebugLevel)
	//stream.SetLogLevel(log.DebugLevel)
	//encoding.SetLogLevel(log.TraceLevel)
	//orcio.SetLogLevel(log.TraceLevel)
}

func TestStruct(t *testing.T) {
	schema := &api.TypeDescription{Kind: pb.Type_STRUCT}
	x := &api.TypeDescription{Kind: pb.Type_INT}
	x.Encoding = pb.ColumnEncoding_DIRECT_V2
	y := &api.TypeDescription{Kind: pb.Type_STRING}
	y.Encoding = pb.ColumnEncoding_DIRECT_V2
	schema.ChildrenNames = []string{"x", "y"}
	schema.Children = []*api.TypeDescription{x, y}

	wopts := config.DefaultWriterOptions()
	wopts.RowSize = 150

	buf := make([]byte, 200_000)
	f := orcio.NewMockFile(buf)

	writer, err := newWriter(schema, &wopts, f)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	wbatch, err := api.CreateWriterBatch(schema, wopts)
	if err != nil {
		fmt.Printf("got error when create row batch %v+", err)
		os.Exit(1)
	}

	for i := 0; i < wopts.RowSize; i++ {
		wbatch.Children[0].Vector[i].V = int32(i)
		wbatch.Children[1].Vector[i].V = fmt.Sprintf("string-%s", strconv.Itoa(i))
	}

	if err := writer.Write(&wbatch); err != nil {
		fmt.Printf("write error %+v\n", err)
		os.Exit(1)
	}

	if err := writer.Close(); err != nil {
		fmt.Printf("close error %+v\n", err)
	}

	reader, err := newReader(f)
	defer reader.Close()
	assert.Nil(t, err)

	bopt := &api.BatchOption{RowSize: 150}
	br, err := reader.CreateBatchReader(bopt)
	defer br.Close()
	assert.Nil(t, err)
	batch, err := schema.CreateBatch(bopt)
	assert.Nil(t, err)
	err = br.Next(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	assert.Equal(t, wbatch.Children[0].Vector, batch.Children[0].Vector)
}

func TestString(t *testing.T) {
	schema := &api.TypeDescription{Id: 0, Kind: pb.Type_STRING, Encoding: pb.ColumnEncoding_DIRECT_V2}
	err := api.NormalizeSchema(schema)
	assert.Nil(t, err)

	wopts := config.DefaultWriterOptions()
	wopts.CompressionKind = pb.CompressionKind_ZLIB
	wopts.StripeSize = 10_000
	wopts.ChunkSize = 5_000
	wopts.CreateVector = false
	wbatch, err := api.CreateWriterBatch(schema, wopts)

	rows := 100
	values := make([]api.Value, rows)
	for i := 0; i < rows; i++ {
		values[i].V = fmt.Sprintf("string %d Because the number of nanoseconds often has a large number of trailing zeros", i)
	}
	wbatch.Vector = values

	buf := make([]byte, 200_000)
	f := orcio.NewMockFile(buf)

	writer, err := newWriter(schema, &wopts, f)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if err := writer.Write(&wbatch); err != nil {
		t.Fatalf("%+v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("%+v", err)
	}

	reader, err := newReader(f)
	defer reader.Close()
	assert.Nil(t, err)

	bopt := &api.BatchOption{RowSize: api.DefaultRowSize}
	br, err := reader.CreateBatchReader(bopt)
	defer br.Close()
	assert.Nil(t, err)

	rbatch, err := schema.CreateBatch(bopt)
	assert.Nil(t, err)

	var vector []api.Value
	for {
		if err = br.Next(rbatch); err != nil {
			t.Fatalf("%+v", err)
		}
		vector = append(vector, rbatch.Vector...)
		if rbatch.Len() == 0 {
			break
		}
	}
	assert.Equal(t, values, vector)
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

	bopt:= &api.BatchOption{RowSize: api.DefaultRowSize}
	batch, err := schema.CreateBatch(bopt)
	assert.Nil(t, err)

	if _, err := reader.next(batch); err != nil {
		t.Fatalf("%+v", err)
	}

	assert.Equal(t, values, batch.Vector)
	assert.Equal(t, childValues, batch.Children[0].Vector)
}
