package orc

import (
	"fmt"
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
	bopt := &api.BatchOption{RowSize: 104, NotCreateVector: true}
	batch, err := schema.CreateVector(bopt)
	assert.Nil(t, err)

	values := make([]api.Value, bopt.RowSize)
	for i := 0; i < bopt.RowSize; i++ {
		values[i].V = byte(i)
	}
	batch.Vector = values

	schemas, err := schema.Flat()
	assert.Nil(t, err)
	writer, err := newStripeWriter(f, 0, schemas, &wopts)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if err := writer.write(batch); err != nil {
		t.Fatalf("%+v", err)
	}
	if err := writer.flushOut(); err != nil {
		t.Fatalf("%+v", err)
	}

	bopt.NotCreateVector = false
	ropts := config.DefaultReaderOptions()
	reader, err := newStripeReader(f, schemas, &ropts, bopt, 0, writer.info)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	vec, err := schema.CreateVector(bopt)
	assert.Nil(t, err)

	vec.Vector = vec.Vector[:0]
	if _, err := reader.next(vec); err != nil {
		t.Fatalf("%+v", err)
	}

	assert.Equal(t, values, vec.Vector)
}

func TestMultipleStripes(t *testing.T) {
	schema := &api.TypeDescription{Id: 0, Kind: pb.Type_STRING, Encoding: pb.ColumnEncoding_DIRECT_V2, HasNulls: false}

	wopts := config.DefaultWriterOptions()
	wopts.CompressionKind = pb.CompressionKind_ZLIB
	wopts.StripeSize = 10_000
	wopts.ChunkSize = 8_000
	bopt := &api.BatchOption{RowSize: 1_000, NotCreateVector: true}
	wbatch, err := schema.CreateVector(bopt)
	assert.Nil(t, err)

	values := make([]api.Value, bopt.RowSize)
	for i := 0; i < bopt.RowSize; i++ {
		values[i].V = fmt.Sprintf("string %d Because the number of nanoseconds often has a large number of trailing zeros", i)
	}
	wbatch.Vector = values

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
	assert.Nil(t, err)
	defer reader.Close()

	bopt.NotCreateVector = false
	br, err := reader.CreateBatchReader(bopt)
	assert.Nil(t, err)
	defer br.Close()

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
	reader.Close()
	assert.Equal(t, values, vector)
}
