package orc

import (
	"fmt"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
)

type dummyFile struct {
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

var df = &dummyFile{array: make([]byte, 2_000_000)}

func TestBasicNoCompression(t *testing.T) {
	schema := &TypeDescription{Id: 0, Kind: pb.Type_STRING, Encoding: pb.ColumnEncoding_DIRECT_V2}
	wopts := DefaultWriterOptions()
	wopts.CompressionKind = pb.CompressionKind_NONE
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
}

func TestBasicZlibCompression(t *testing.T) {
	schema := &TypeDescription{Id: 0, Kind: pb.Type_STRING, Encoding: pb.ColumnEncoding_DIRECT_V2}
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
}

func TestBasicMultipleStripes(t *testing.T) {
	schema := &TypeDescription{Id: 0, Kind: pb.Type_STRING, Encoding: pb.ColumnEncoding_DIRECT_V2}
	wopts := DefaultWriterOptions()
	wopts.CompressionKind = pb.CompressionKind_ZLIB
	wopts.StripeSize = 100_000
	wopts.ChunkSize = 50_000
	batch := schema.CreateWriterBatch(wopts)

	rows := 30_000
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

	batch.Vector = vector[:1000]

	if err := writer.Write(batch); err != nil {
		t.Fatalf("%+v", err)
	}

	if err := writer.close(); err != nil {
		t.Fatalf("%+v", err)
	}

	ropts := DefaultReaderOptions()
	ropts.RowSize=100_000
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

		if rbatch.ReadRows==0 {
			break
		}

		totalRows+= int(rbatch.ReadRows)
		readResults= append(readResults, rbatch.Vector.([]string)...)
	}

	assert.Equal(t, 61000, totalRows)
	assert.Equal(t, vector, readResults[:30_000])
	assert.Equal(t, vector, readResults[30_000:60_000])
	assert.Equal(t, vector[:1000], readResults[60_000:61_000])
}
