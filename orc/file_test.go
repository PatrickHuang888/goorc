package orc

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/pb/pb"
)

func init() {
	//logger.SetLevel(log.DebugLevel)
	//stream.SetLogLevel(log.DebugLevel)
	//encoding.SetLogLevel(log.TraceLevel)
	//orcio.SetLogLevel(log.TraceLevel)
}

func TestStruct(t *testing.T) {
	schema := &api.TypeDescription{Id: 0, Kind: pb.Type_STRUCT}
	schema.Encoding = pb.ColumnEncoding_DIRECT
	x := &api.TypeDescription{Id: 1, Kind: pb.Type_INT}
	x.Encoding = pb.ColumnEncoding_DIRECT_V2
	y := &api.TypeDescription{Id: 2, Kind: pb.Type_STRING}
	y.Encoding = pb.ColumnEncoding_DIRECT_V2
	schema.ChildrenNames = []string{"x", "y"}
	schema.Children = []*api.TypeDescription{x, y}

	wopts := config.DefaultWriterOptions()

	buf := make([]byte, 200_000)
	f := orcio.NewMockFile(buf)

	writer, err := newWriter(schema, &wopts, f)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	bopt := &api.BatchOption{RowSize: 150}
	wvec, err := schema.CreateVector(bopt)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < bopt.RowSize; i++ {
		wvec.Children[0].Vector[i].V = int32(i)
		wvec.Children[1].Vector[i].V = fmt.Sprintf("string-%s", strconv.Itoa(i))
	}

	if err := writer.Write(wvec); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Error(err)
	}

	reader, err := newReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	br, err := reader.CreateBatchReader(bopt)
	if err != nil {
		t.Fatal(err)
	}
	defer br.Close()

	rvec, err := schema.CreateVector(bopt)
	if err != nil {
		t.Fatal(err)
	}

	if _, err = br.Next(rvec); err != nil {
		t.Fatalf("%+v", err)
	}

	assert.Equal(t, wvec.Children[0].Vector, rvec.Children[0].Vector)
}

func TestString(t *testing.T) {
	schema := &api.TypeDescription{Id: 0, Kind: pb.Type_STRING, Encoding: pb.ColumnEncoding_DIRECT_V2}
	err := api.NormalizeSchema(schema)
	assert.Nil(t, err)

	wopts := config.DefaultWriterOptions()
	wopts.CompressionKind = pb.CompressionKind_ZLIB
	wopts.StripeSize = 10_000
	wopts.ChunkSize = 5_000
	bopt := &api.BatchOption{RowSize: 100}
	wbatch, err := schema.CreateVector(bopt)
	assert.Nil(t, err)
	for i := 0; i < bopt.RowSize; i++ {
		wbatch.Vector[i].V = fmt.Sprintf("string %d Because the number of nanoseconds often has a large number of trailing zeros", i)
	}

	buf := make([]byte, 200_000)
	f := orcio.NewMockFile(buf)

	writer, err := newWriter(schema, &wopts, f)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if err := writer.Write(wbatch); err != nil {
		t.Fatalf("%+v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("%+v", err)
	}

	reader, err := newReader(f)
	defer reader.Close()
	assert.Nil(t, err)

	br, err := reader.CreateBatchReader(bopt)
	defer br.Close()
	assert.Nil(t, err)

	rbatch, err := schema.CreateVector(bopt)
	assert.Nil(t, err)

	var vector []api.Value
	var end bool
	for !end {
		if end, err = br.Next(rbatch); err != nil {
			t.Fatalf("%+v", err)
		}
		vector = append(vector, rbatch.Vector...)
	}
	assert.Equal(t, wbatch.Vector, vector)
}

func TestStructWithPresents(t *testing.T) {
	schema := &api.TypeDescription{Id: 0, Kind: pb.Type_STRUCT, HasNulls: true}
	schema.Encoding = pb.ColumnEncoding_DIRECT
	// children should should no has nulls
	child1 := &api.TypeDescription{Id: 0, Kind: pb.Type_INT}
	child1.Encoding = pb.ColumnEncoding_DIRECT_V2
	schema.Children = []*api.TypeDescription{child1}
	// todo: verify children names
	schema.ChildrenNames = []string{"child1"}
	if err := api.NormalizeSchema(schema); err != nil {
		t.Fatal(err)
	}

	wopts := config.DefaultWriterOptions()
	bopt := &api.BatchOption{RowSize: 100, NotCreateVector: true}
	wvec, err := schema.CreateVector(bopt)
	if err != nil {
		t.Fatal(err)
	}

	values := make([]api.Value, bopt.RowSize)
	values[0].Null = true
	values[45].Null = true
	values[99].Null = true
	wvec.Vector = values

	childValues := make([]api.Value, bopt.RowSize)
	for i := 0; i < bopt.RowSize; i++ {
		childValues[i].V = int32(i)
	}
	childValues[0].Null = true
	childValues[0].V = nil
	childValues[45].Null = true
	childValues[45].V = nil
	childValues[99].Null = true
	childValues[99].V = nil
	wvec.Children[0].Vector = childValues

	buf := make([]byte, 5000)
	f := orcio.NewMockFile(buf)

	assert.Nil(t, err)
	writer, err := newWriter(schema, &wopts, f)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if err := writer.Write(wvec); err != nil {
		t.Fatalf("%+v", err)
	}
	if err = writer.Close(); err != nil {
		t.Fatalf("%+v", err)
	}

	reader, err := newReader(f)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	defer reader.Close()

	bopt.NotCreateVector = false
	rvec, err := schema.CreateVector(bopt)
	if err != nil {
		t.Fatal(err)
	}
	br, err := reader.CreateBatchReader(bopt)
	if err != nil {
		t.Fatal(err)
	}
	defer br.Close()

	if _, err := br.Next(rvec); err != nil {
		t.Fatalf("%+v", err)
	}

	assert.Equal(t, values, rvec.Vector)
	assert.Equal(t, childValues, rvec.Children[0].Vector)
}
