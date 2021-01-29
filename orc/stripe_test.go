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

	bopt:= &api.BatchOption{RowSize: rows}
	vec, err := schema.CreateBatch(bopt)
	assert.Nil(t, err)

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
	wopts.CreateVector = false
	wbatch, err := api.CreateWriterBatch(schema, wopts)
	assert.Nil(t, err)

	rows := 1_000
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
	assert.Nil(t, err)
	defer reader.Close()

	bopt:= &api.BatchOption{RowSize: api.DefaultRowSize}
	br, err := reader.CreateBatchReader(bopt)
	assert.Nil(t, err)
	defer br.Close()

	rbatch, err:= schema.CreateBatch(bopt)
	assert.Nil(t, err)

	var vector []api.Value
	for {
		if err = br.Next(rbatch);err != nil {
			t.Fatalf("%+v", err)
		}
		vector = append(vector, rbatch.Vector...)
		if rbatch.Len() == 0 {
			break
		}
	}
	reader.Close()
	assert.Equal(t, values, vector)
}