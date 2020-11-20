package orc

import (
	"fmt"
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/orc/stream"
	"github.com/patrickhuang888/goorc/pb/pb"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
)

func init()  {
	logger.SetLevel(log.TraceLevel)
	stream.SetLogLevel(log.DebugLevel)
	//encoding.SetLogLevel(log.TraceLevel)
	//orcio.SetLogLevel(log.TraceLevel)
}


/*type dummyFile struct {
	array              []byte
	start, offset, end int64
}

func (dummyFile) Close() error {
	return nil
}

func (df dummyFile) Size() (int64, error) {
	return df.end - df.start, nil
}

func (df *dummyFile) Seek(offset int64, whence int) (int64, error) {
	log.Tracef("dummy file set offset %d, end %d", offset, df.end)
	if offset > df.end {
		return 0, errors.New("offset beyond end")
	}
	df.offset = offset
	return df.offset, nil
}

func (df *dummyFile) Read(p []byte) (n int, err error) {
	log.Tracef("dummy file read, offset %d, end %d", df.offset, df.end)
	n = copy(p, df.array[df.offset:df.end])
	df.offset += int64(n)
	if n == 0 {
		return 0, errors.WithStack(io.EOF)
	}
	return
}

func (df *dummyFile) Write(p []byte) (n int, err error) {
	log.Tracef("dummy file write, offset %d, end %d", df.offset, df.end)
	n = len(p)
	if cap(df.array) < int(df.end)+n {
		return 0, errors.New("capacity not enough")
	}
	copy(df.array[df.end:], p)
	df.end += int64(n)
	return
}

func (df *dummyFile) Reset() {
	df.start = 0
	df.offset = 0
	df.end = 0
}

var df = &dummyFile{array: make([]byte, 2_000_000)}*/

/*func TestBasicNoCompression(t *testing.T) {
	schema := &api.TypeDescription{Id: 0, Kind: pb.Type_STRING, Encoding: pb.ColumnEncoding_DIRECT_V2}
	wopts := config.DefaultWriterOptions()
	wopts.CompressionKind = pb.CompressionKind_NONE
	batch := CreateWriterBatch(schema, wopts)

	rows := 10_000
	vector := make([]string, rows)
	for i := 0; i < rows; i++ {
		vector[i] = fmt.Sprintf("string %d Because the number of nanoseconds often has a large number of trailing zeros", i)
	}
	batch.Vector = vector

	df.Reset()
	writer, err := newWriter(schema, wopts, df)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if err := writer.Write(batch); err != nil {
		t.Fatalf("%+v", err)
	}
	if err := writer.close(); err != nil {
		t.Fatalf("%+v", err)
	}

	ropts := DefaultReaderOptions()
	rbatch := schema.CreateReaderBatch(ropts)

	reader, err := newReader(ropts, df)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	if err := reader.Next(rbatch); err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, rows, rbatch.ReadRows)
	assert.Equal(t, vector, rbatch.Vector)
}*/

/*func TestBasicZlibCompression(t *testing.T) {
	schema := &api.TypeDescription{Id: 0, Kind: pb.Type_STRING, Encoding: pb.ColumnEncoding_DIRECT_V2}
	wopts := DefaultWriterOptions()
	wopts.CompressionKind = pb.CompressionKind_ZLIB
	batch := schema.CreateWriterBatch(wopts)

	rows := 10_000
	vector := make([]string, rows)
	for i := 0; i < rows; i++ {
		vector[i] = fmt.Sprintf("string %d Because the number of nanoseconds often has a large number of trailing zeros", i)
	}
	batch.Vector = vector

	df.Reset()
	writer, err := newWriter(schema, wopts, df)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	if err := writer.Write(batch); err != nil {
		t.Fatalf("%+v", err)
	}

	if err := writer.Write(batch); err != nil {
		t.Fatalf("%+v", err)
	}

	if err := writer.close(); err != nil {
		t.Fatalf("%+v", err)
	}

	ropts := DefaultReaderOptions()
	rbatch := schema.CreateReaderBatch(ropts)

	reader, err := newReader(ropts, df)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	var totalRows int
	var readResults []string
	for {
		if err := reader.Next(rbatch); err != nil {
			t.Fatalf("%+v", err)
		}
		if rbatch.ReadRows== 0 {
			break
		}
		totalRows+=rbatch.ReadRows
		readResults= append(readResults, rbatch.Vector.([]string)...)
	}
	assert.Equal(t, 20000, totalRows)
	assert.Equal(t, vector, readResults[:10_000])
	assert.Equal(t, vector, readResults[10_000:20_000])
}*/

func TestString(t *testing.T) {
	schema := api.TypeDescription{Id: 0, Kind: pb.Type_STRING, Encoding: pb.ColumnEncoding_DIRECT_V2, HasNulls: false}
	//schemas, err := schema.Normalize()
	//assert.Nil(t, err)

	wopts := config.DefaultWriterOptions()
	wopts.CompressionKind = pb.CompressionKind_ZLIB
	wopts.StripeSize = 10_000
	// todo: check chunkSize should not larger than stripe size
	wopts.ChunkSize= 5_000
	wbatch := api.CreateWriterBatch(schema, wopts)

	rows := 100
	values := make([]api.Value, rows)
	for i := 0; i < rows; i++ {
		values[i].V = fmt.Sprintf("string %d Because the number of nanoseconds often has a large number of trailing zeros", i)
	}
	wbatch.Vector = values

	buf := make([]byte, 200_000)
	f := orcio.NewMockFile(buf)

	//writer, err := newStripeWriter(f, 0, schemas, &wopts)
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

	ropts:= config.DefaultReaderOptions()
	reader, err:=newReader(&ropts, f)
	assert.Nil(t, err)
	rbatch:= api.CreateReaderBatch(*reader.GetSchema(), ropts )

	var vector []api.Value
	for {
		err = reader.Next(&rbatch)
		if err!=nil {
			t.Fatalf("%+v", err)
		}
		vector= append(vector, rbatch.Vector...)
		if rbatch.Len()==0 {
			break
		}
	}
	reader.Close()
	assert.Equal(t, values, vector)
}


func TestMultipleStripes(t *testing.T) {
	schema := api.TypeDescription{Id: 0, Kind: pb.Type_STRING, Encoding: pb.ColumnEncoding_DIRECT_V2, HasNulls: false}
	//schemas, err := schema.Normalize()
	//assert.Nil(t, err)

	wopts := config.DefaultWriterOptions()
	wopts.CompressionKind = pb.CompressionKind_ZLIB
	wopts.StripeSize = 10_000
	// todo: check chunkSize should not larger than stripe size
	wopts.ChunkSize= 8_000
	wbatch := api.CreateWriterBatch(schema, wopts)

	rows := 1_000
	values := make([]api.Value, rows)
	for i := 0; i < rows; i++ {
		values[i].V = fmt.Sprintf("string %d Because the number of nanoseconds often has a large number of trailing zeros", i)
	}
	wbatch.Vector = values

	buf := make([]byte, 200_000)
	f := orcio.NewMockFile(buf)

	//writer, err := newStripeWriter(f, 0, schemas, &wopts)
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

	ropts:= config.DefaultReaderOptions()
	ropts.RowSize= 2000
	reader, err:=newReader(&ropts, f)
	assert.Nil(t, err)
	rbatch:= api.CreateReaderBatch(*reader.GetSchema(), ropts )

	var vector []api.Value
	for {
		err = reader.Next(&rbatch)
		if err!=nil {
			t.Fatalf("%+v", err)
		}
		vector= append(vector, rbatch.Vector...)
		if rbatch.Len()==0 {
			break
		}
	}
	reader.Close()
	assert.Equal(t, values, vector)
}
