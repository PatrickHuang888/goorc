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

	ropts := config.DefaultReaderOptions()
	reader, err := newReader(&ropts, f)
	assert.Nil(t, err)
	rbatch, err := api.CreateReaderBatch(reader.GetSchema(), ropts)
	assert.Nil(t, err)

	err = reader.Next(&rbatch)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	reader.Close()
	//assert.Equal(t, values, vector)
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

	ropts := config.DefaultReaderOptions()
	reader, err := newReader(&ropts, f)
	assert.Nil(t, err)
	rbatch, err := api.CreateReaderBatch(reader.GetSchema(), ropts)
	assert.Nil(t, err)

	var vector []api.Value
	for {
		err = reader.Next(&rbatch)
		if err != nil {
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

	ropts := config.DefaultReaderOptions()
	ropts.RowSize = 2000
	reader, err := newReader(&ropts, f)
	assert.Nil(t, err)
	rbatch, err := api.CreateReaderBatch(reader.GetSchema(), ropts)
	assert.Nil(t, err)

	var vector []api.Value
	for {
		err = reader.Next(&rbatch)
		if err != nil {
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
