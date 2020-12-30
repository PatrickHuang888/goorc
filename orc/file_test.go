package orc

import (
	"fmt"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/orc/stream"
	"github.com/patrickhuang888/goorc/pb/pb"
)

func init() {
	logger.SetLevel(log.TraceLevel)
	stream.SetLogLevel(log.DebugLevel)
	//encoding.SetLogLevel(log.TraceLevel)
	//orcio.SetLogLevel(log.TraceLevel)
}

func TestString(t *testing.T) {
	schema := api.TypeDescription{Id: 0, Kind: pb.Type_STRING, Encoding: pb.ColumnEncoding_DIRECT_V2}
	err := api.NormalizeSchema(&schema)
	assert.Nil(t, err)

	wopts := config.DefaultWriterOptions()
	wopts.CompressionKind = pb.CompressionKind_ZLIB
	wopts.StripeSize = 10_000
	wopts.ChunkSize = 5_000
	wbatch, err := api.CreateWriterBatch(schema, wopts, false)

	rows := 100
	values := make([]api.Value, rows)
	for i := 0; i < rows; i++ {
		values[i].V = fmt.Sprintf("string %d Because the number of nanoseconds often has a large number of trailing zeros", i)
	}
	wbatch.Vector = values

	buf := make([]byte, 200_000)
	f := orcio.NewMockFile(buf)

	writer, err := newWriter(&schema, &wopts, f)
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
	rbatch := api.CreateReaderBatch(*reader.GetSchema(), ropts)

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
	schema := api.TypeDescription{Id: 0, Kind: pb.Type_STRING, Encoding: pb.ColumnEncoding_DIRECT_V2, HasNulls: false}

	wopts := config.DefaultWriterOptions()
	wopts.CompressionKind = pb.CompressionKind_ZLIB
	wopts.StripeSize = 10_000
	wopts.ChunkSize = 8_000
	wbatch, err := api.CreateWriterBatch(schema, wopts, false)
	assert.Nil(t, err)

	rows := 1_000
	values := make([]api.Value, rows)
	for i := 0; i < rows; i++ {
		values[i].V = fmt.Sprintf("string %d Because the number of nanoseconds often has a large number of trailing zeros", i)
	}
	wbatch.Vector = values

	buf := make([]byte, 200_000)
	f := orcio.NewMockFile(buf)

	writer, err := newWriter(&schema, &wopts, f)
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
	rbatch := api.CreateReaderBatch(*reader.GetSchema(), ropts)

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
