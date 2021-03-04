package column

import (
	"fmt"
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/common"
	"github.com/patrickhuang888/goorc/orc/config"
	"github.com/patrickhuang888/goorc/orc/encoding"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/orc/stream"
	"github.com/patrickhuang888/goorc/pb/pb"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func init() {
	logger.SetLevel(log.TraceLevel)
	encoding.SetLogLevel(log.TraceLevel)
	stream.SetLogLevel(log.TraceLevel)
	common.SetLogLevel(log.TraceLevel)
}

func TestIntV2(t *testing.T) {
	var err error
	schema := &api.TypeDescription{Id: 0, Kind: pb.Type_LONG}
	schema.Encoding = pb.ColumnEncoding_DIRECT_V2

	rows := 1000
	values := make([]api.Value, rows)
	for i := 0; i < rows; i++ {
		values[i].V = int64(i)
	}

	wopts := config.DefaultWriterOptions()
	wopts.WriteIndex = true
	wopts.IndexStride = 200
	writer := newIntV2Writer(schema, &wopts, BitsBigInt).(*intWriter)
	if err = writer.Writes(values); err != nil {
		t.Fatalf("%+v", err)
	}

	if err = writer.Flush(); err != nil {
		t.Fatalf("%+v", err)
	}

	bb := make([]byte, 1024)
	f := orcio.NewMockFile(bb)

	if _, err := writer.WriteOut(f); err != nil {
		t.Fatalf("%+v", err)
	}

	ropts := config.DefaultReaderOptions()
	ropts.IndexStride = 200
	r, err := NewReader(schema, &ropts, f)
	if err != nil {
		t.Fatal(err)
	}
	reader := r.(*IntV2Reader)
	reader.reader.index = writer.index
	err = reader.InitStream(writer.data.Info(), 0)
	if err != nil {
		t.Fatal(err)
	}

	bopt := &api.BatchOption{RowSize: rows}
	vec, err := schema.CreateVector(bopt)
	if err != nil {
		t.Fatal(err)
	}
	if err = reader.NextBatch(vec); err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, values, vec.Vector)

	if err = reader.SeekStride(1); err != nil { // less than 512
		t.Fatalf("%+v", err)
	}
	if err = reader.Skip(100); err != nil { // seek 300
		t.Fatal(err)
	}
	vec.Vector = vec.Vector[:1]
	if err = reader.NextBatch(vec); err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, int64(300), vec.Vector[0].V)

	if err = reader.SeekStride(3); err != nil { // seek 600
		t.Fatalf("%+v", err)
	}
	vec.Vector = vec.Vector[:1]
	if err = reader.NextBatch(vec); err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, int64(600), vec.Vector[0].V)

	if err = reader.SeekStride(4); err != nil {
		t.Fatalf("%+v", err)
	}
	if err = reader.Skip(50); err != nil { // seek 850
		t.Fatal(err)
	}
	vec.Vector = vec.Vector[:1]
	if err = reader.NextBatch(vec); err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, int64(850), vec.Vector[0].V)
}

func TestIntV2WithPresents(t *testing.T) {
	schema := &api.TypeDescription{Id: 0, Kind: pb.Type_LONG, HasNulls: true}
	schema.HasNulls = true
	schema.Encoding = pb.ColumnEncoding_DIRECT_V2

	wopts := config.DefaultWriterOptions()

	//bool don't know how many values, so 13*8
	rows := 104

	values := make([]api.Value, rows)
	for i := 0; i < rows; i++ {
		values[i].V = int64(i)
	}
	values[0].Null = true
	values[0].V = nil
	values[45].Null = true
	values[45].V = nil

	values[102].Null = true
	values[102].V = nil

	writer := newIntV2Writer(schema, &wopts, BitsBigInt).(*intWriter)
	if err := writer.Writes(values); err != nil {
		t.Fatalf("%+v", err)
	}

	if err := writer.Flush(); err != nil {
		t.Fatalf("%+v", err)
	}

	bb := make([]byte, 1024)
	f := orcio.NewMockFile(bb)

	if _, err := writer.WriteOut(f); err != nil {
		t.Fatalf("%+v", err)
	}

	ropts := config.DefaultReaderOptions()
	r, err := NewReader(schema, &ropts, f)
	assert.Nil(t, err)
	reader := r.(*IntV2Reader)
	err = reader.InitStream(writer.present.Info(), 0)
	assert.Nil(t, err)
	err = reader.InitStream(writer.data.Info(), writer.present.Info().GetLength())
	assert.Nil(t, err)

	bopt := &api.BatchOption{RowSize: rows}
	vec, err := schema.CreateVector(bopt)
	if err != nil {
		t.Fatal(err)
	}
	if err = reader.NextBatch(vec); err != nil {
		t.Fatalf("%+v", err)
	}

	assert.Equal(t, values, vec.Vector)
}

func TestBool(t *testing.T) {
	schema := api.TypeDescription{Id: 0, Kind: pb.Type_BOOLEAN}
	wopts := config.DefaultWriterOptions()
	wopts.IndexStride = 130
	wopts.WriteIndex = true

	rows := encoding.MaxByteRunLength*encoding.MaxBoolRunLength + 10
	values := make([]api.Value, rows)
	for i := 0; i < rows; i++ {
		values[i].V = true
	}
	values[0].V = false
	values[45].V = false
	values[encoding.MaxByteRunLength*encoding.MaxBoolRunLength+9].V = false

	writer := newBoolWriter(&schema, &wopts).(*boolWriter)
	for _, v := range values {
		if err := writer.Write(v); err != nil {
			t.Fatalf("%+v", err)
		}
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("%+v", err)
	}

	f := orcio.NewMockFile(make([]byte, 1024))

	if _, err := writer.WriteOut(f); err != nil {
		t.Fatalf("%+v", err)
	}

	ropts := config.DefaultReaderOptions()
	ropts.IndexStride = 130

	reader := NewBoolReader(&schema, &ropts, f).(*BoolReader)
	reader.index = writer.index
	err := reader.InitStream(writer.data.Info(), 0)
	assert.Nil(t, err)

	bopt := &api.BatchOption{RowSize: rows}
	vec, err := schema.CreateVector(bopt)
	if err != nil {
		t.Fatal(err)
	}
	if err = reader.NextBatch(vec); err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, values, vec.Vector)

	counts := encoding.MaxBoolRunLength*encoding.MaxByteRunLength + 8
	stride := counts / ropts.IndexStride
	offset := uint64(counts % ropts.IndexStride)
	if err = reader.SeekStride(stride); err != nil {
		t.Fatalf("%+v", err)
	}
	if err = reader.Skip(offset); err != nil {
		t.Fatal(err)
	}
	vec.Vector = vec.Vector[:2]
	if err = reader.NextBatch(vec); err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, true, vec.Vector[0].V)
	assert.Equal(t, false, vec.Vector[1].V)
}

func TestFloat(t *testing.T) {
	schema := api.TypeDescription{Id: 0, Kind: pb.Type_FLOAT}
	wopts := config.DefaultWriterOptions()

	rows := 100

	values := make([]api.Value, rows)
	for i := 0; i < rows; i++ {
		values[i].V = float32(i)
	}
	writer := newFloatWriter(&schema, &wopts, false).(*floatWriter)
	for _, v := range values {
		if err := writer.Write(v); err != nil {
			t.Fatalf("%+v", err)
		}
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("%+v", err)
	}

	f := orcio.NewMockFile(make([]byte, 1024))
	if _, err := writer.WriteOut(f); err != nil {
		t.Fatalf("%+v", err)
	}

	ropts := config.DefaultReaderOptions()
	r, err := NewReader(&schema, &ropts, f)
	assert.Nil(t, err)
	reader := r.(*FloatReader)
	err = reader.InitStream(writer.data.Info(), 0)
	assert.Nil(t, err)

	bopt := &api.BatchOption{RowSize: rows}
	vec, err := schema.CreateVector(bopt)
	if err != nil {
		t.Fatal(err)
	}
	if err = reader.NextBatch(vec); err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, values, vec.Vector)
}

func TestStringDirectV2(t *testing.T) {
	rows := 100
	schema := api.TypeDescription{Id: 0, Kind: pb.Type_STRING, HasNulls: false}

	wopts := config.DefaultWriterOptions()
	schema.Encoding = pb.ColumnEncoding_DIRECT_V2
	writer := newStringDirectV2Writer(&schema, &wopts).(*stringDirectV2Writer)

	var values []api.Value
	var value api.Value
	for i := 0; i < rows; i++ {
		value.V = fmt.Sprintf("string %d", i)
		if err := writer.Write(value); err != nil {
			t.Fatalf("%+v", err)
		}
		values = append(values, value)
	}

	if err := writer.Flush(); err != nil {
		t.Fatalf("%+v", err)
	}

	bb := make([]byte, 10240)
	f := orcio.NewMockFile(bb)

	if _, err := writer.WriteOut(f); err != nil {
		t.Fatalf("%+v", err)
	}

	ropts := config.DefaultReaderOptions()
	reader := NewStringDirectV2Reader(&ropts, &schema, f)
	err := reader.InitStream(writer.data.Info(), 0)
	assert.Nil(t, err)
	err = reader.InitStream(writer.length.Info(), writer.data.Info().GetLength())
	assert.Nil(t, err)

	bopt := &api.BatchOption{RowSize: rows}
	vec, err := schema.CreateVector(bopt)
	if err != nil {
		t.Fatal(err)
	}
	if err = reader.NextBatch(vec); err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, values, vec.Vector)
}

func TestStringDirectV2Index(t *testing.T) {
	rows := 1000
	schema := api.TypeDescription{Id: 0, Kind: pb.Type_STRING, HasNulls: false}

	wopts := config.DefaultWriterOptions()
	wopts.WriteIndex = true
	wopts.IndexStride = 200
	schema.Encoding = pb.ColumnEncoding_DIRECT_V2
	writer := newStringDirectV2Writer(&schema, &wopts).(*stringDirectV2Writer)

	var values []api.Value
	var value api.Value
	for i := 0; i < rows; i++ {
		value.V = fmt.Sprintf("string %d", i)
		if err := writer.Write(value); err != nil {
			t.Fatalf("%+v", err)
		}
		values = append(values, value)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("%+v", err)
	}
	bb := make([]byte, 10240)
	f := orcio.NewMockFile(bb)
	if _, err := writer.WriteOut(f); err != nil {
		t.Fatalf("%+v", err)
	}

	ropts := config.DefaultReaderOptions()
	ropts.IndexStride = 200
	reader := NewStringDirectV2Reader(&ropts, &schema, f).(*StringDirectV2Reader)
	reader.index = writer.index
	err := reader.InitStream(writer.data.Info(), 0)
	assert.Nil(t, err)
	err = reader.InitStream(writer.length.Info(), writer.data.Info().GetLength())
	assert.Nil(t, err)

	if err = reader.SeekStride(1); err != nil {
		t.Fatal(err)
	}
	if err = reader.Skip(100); err != nil {
		t.Fatal(err)
	}

	bopt := &api.BatchOption{RowSize: 1}
	vec, err := schema.CreateVector(bopt)
	if err != nil {
		t.Fatal(err)
	}
	if err = reader.NextBatch(vec); err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, "string 300", vec.Vector[0].V)
}

func TestByteWithPresents(t *testing.T) {
	rows := 150
	schema := &api.TypeDescription{Id: 0, Kind: pb.Type_BYTE, HasNulls: true}
	schema.Encoding = pb.ColumnEncoding_DIRECT

	values := make([]api.Value, rows)
	for i := 0; i < rows; i++ {
		values[i].V = byte(i)
	}
	values[0].Null = true
	values[45].Null = true
	values[98].Null = true
	wopts := config.DefaultWriterOptions()

	writer := newByteWriter(schema, &wopts).(*byteWriter)
	for _, v := range values {
		if err := writer.Write(v); err != nil {
			t.Fatalf("%+v", err)
		}
	}

	if err := writer.Flush(); err != nil {
		t.Fatalf("%+v", err)
	}

	bb := make([]byte, 500)
	f := orcio.NewMockFile(bb)

	if _, err := writer.WriteOut(f); err != nil {
		t.Fatalf("%+v", err)
	}

	ropts := config.DefaultReaderOptions()
	reader := NewByteReader(schema, &ropts, f)

	if err := reader.InitStream(writer.present.Info(), 0); err != nil {
		t.Fatalf("%+v", err)
	}
	if err := reader.InitStream(writer.data.Info(), writer.present.Info().GetLength()); err != nil {
		t.Fatalf("%+v", err)
	}

	bopt := &api.BatchOption{RowSize: rows}
	vec, err := schema.CreateVector(bopt)
	if err != nil {
		t.Fatal(err)
	}
	if err = reader.NextBatch(vec); err != nil {
		t.Fatalf("%+v", err)
	}

	assert.Equal(t, true, vec.Vector[0].Null)
	assert.Equal(t, true, vec.Vector[45].Null)
	assert.Equal(t, true, vec.Vector[98].Null)

	for i, v := range vec.Vector {
		if !v.Null {
			assert.Equal(t, values[i], vec.Vector[i])
		}
	}

	// todo: stats test
	// stats verification at file test?
}

func TestByteOnIndex(t *testing.T) {
	rows := 150
	indexStride := 130

	schema := &api.TypeDescription{Id: 0, Kind: pb.Type_BYTE}
	schema.Encoding = pb.ColumnEncoding_DIRECT

	values := make([]api.Value, rows)
	for i := 0; i < rows; i++ { // start from 0
		values[i].V = byte(i)
	}

	wopts := config.DefaultWriterOptions()
	wopts.WriteIndex = true
	wopts.IndexStride = indexStride // index stride should > 128 (max byte encoding block)

	writer := newByteWriter(schema, &wopts).(*byteWriter)
	for _, v := range values {
		if err := writer.Write(v); err != nil {
			t.Fatalf("%+v", err)
		}
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("%+v", err)
	}
	bb := make([]byte, 500)
	f := orcio.NewMockFile(bb)
	if _, err := writer.WriteOut(f); err != nil {
		t.Fatalf("%+v", err)
	}

	ropts := config.DefaultReaderOptions()
	ropts.IndexStride = indexStride
	reader := NewByteReader(schema, &ropts, f).(*ByteReader)
	reader.index = writer.index
	if err := reader.InitStream(writer.data.Info(), 0); err != nil {
		t.Fatalf("%+v", err)
	}

	bopt := &api.BatchOption{RowSize: rows}
	vec, err := schema.CreateVector(bopt)
	if err != nil {
		t.Fatal(err)
	}
	if err = reader.NextBatch(vec); err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, values, vec.Vector)

	if err := reader.SeekStride(1); err != nil {
		t.Fatalf("%+v", err)
	}
	vec.Vector = vec.Vector[:1]
	if err = reader.NextBatch(vec); err != nil { // advance 1
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, byte(130), vec.Vector[0].V)

	if err := reader.Skip(9); err != nil {
		t.Fatalf("%+v", err)
	}
	vec.Vector = vec.Vector[:1]
	if err = reader.NextBatch(vec); err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, byte(140), vec.Vector[0].V)
}

func TestByteOnIndexWithPresents(t *testing.T) {
	rows := 150
	indexStride := 130

	schema := &api.TypeDescription{Id: 0, Kind: pb.Type_BYTE, HasNulls: true}
	schema.Encoding = pb.ColumnEncoding_DIRECT

	values := make([]api.Value, rows)
	for i := 0; i < rows; i++ {
		values[i].V = byte(i)
	}
	values[0].Null = true
	values[129].Null = true
	wopts := config.DefaultWriterOptions()
	wopts.WriteIndex = true
	wopts.IndexStride = indexStride

	writer := newByteWriter(schema, &wopts).(*byteWriter)
	for _, v := range values {
		if err := writer.Write(v); err != nil {
			t.Fatalf("%+v", err)
		}
	}

	if err := writer.Flush(); err != nil {
		t.Fatalf("%+v", err)
	}

	bb := make([]byte, 500)
	f := orcio.NewMockFile(bb)

	if _, err := writer.WriteOut(f); err != nil {
		t.Fatalf("%+v", err)
	}

	ropts := config.DefaultReaderOptions()
	ropts.IndexStride = indexStride

	reader := NewByteReader(schema, &ropts, f).(*ByteReader)
	reader.index = writer.index
	if err := reader.InitStream(writer.present.Info(), 0); err != nil {
		t.Fatalf("%+v", err)
	}
	if err := reader.InitStream(writer.data.Info(), writer.present.Info().GetLength()); err != nil {
		t.Fatalf("%+v", err)
	}

	if err := reader.SeekStride(1); err != nil {
		t.Fatalf("%+v", err)
	}
	if err := reader.Skip(10); err != nil {
		t.Fatalf("%+v", err)
	}
	bopt := &api.BatchOption{RowSize: 1}
	vec, err := schema.CreateVector(bopt)
	if err != nil {
		t.Fatal(err)
	}
	if err = reader.NextBatch(vec); err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, byte(140), vec.Vector[0].V)
}

func TestColumnDecimal64(t *testing.T) {
	schema := &api.TypeDescription{Id: 0, Kind: pb.Type_DECIMAL}
	wopts := config.DefaultWriterOptions()

	rows := 100
	values := make([]api.Value, rows)
	for i := 0; i < rows; i++ {
		values[i].V = api.Decimal64{Precision: int64(i), Scale: 2}
	}

	writer := NewDecimal64V2Writer(schema, &wopts).(*decimalV2Writer)
	for _, v := range values {
		if err := writer.Write(v); err != nil {
			t.Fatalf("%+v", err)
		}
	}
	err := writer.Flush()
	assert.Nil(t, err)

	bb := make([]byte, 500)
	f := orcio.NewMockFile(bb)

	if _, err := writer.WriteOut(f); err != nil {
		t.Fatalf("%+v", err)
	}

	ropts := config.DefaultReaderOptions()
	reader := NewDecimal64V2Reader(schema, &ropts, f)
	err = reader.InitStream(writer.data.Info(), 0)
	assert.Nil(t, err)
	err = reader.InitStream(writer.secondary.Info(), writer.data.Info().GetLength())
	assert.Nil(t, err)

	bopt := &api.BatchOption{RowSize: rows}
	vec, err := schema.CreateVector(bopt)
	if err != nil {
		t.Fatal(err)
	}
	if err = reader.NextBatch(vec); err != nil {
		t.Fatalf("%+v", err)
	}

	assert.Equal(t, values, vec.Vector)
}

func TestColumnTimestampWithPresents(t *testing.T) {
	schema := &api.TypeDescription{Id: 0, Kind: pb.Type_TIMESTAMP, HasNulls: true}
	wopts := config.DefaultWriterOptions()

	rows := 100

	values := make([]api.Value, rows)
	for i := 0; i < rows; i++ {
		values[i].V = api.GetTimestamp(time.Now())
	}
	values[0].Null = true
	values[0].V = nil
	values[45].Null = true
	values[45].V = nil
	values[99].Null = true
	values[99].V = nil

	writer := NewTimestampV2Writer(schema, &wopts).(*timestampWriter)
	for _, v := range values {
		if err := writer.Write(v); err != nil {
			t.Fatalf("%+v", err)
		}
	}
	err := writer.Flush()
	if err != nil {
		t.Fatal(err)
	}

	bb := make([]byte, 500)
	f := orcio.NewMockFile(bb)

	if _, err = writer.WriteOut(f); err != nil {
		t.Fatalf("%+v", err)
	}

	ropts := config.DefaultReaderOptions()
	reader := NewTimestampV2Reader(schema, &ropts, f, nil)

	err = reader.InitStream(writer.present.Info(), 0)
	assert.Nil(t, err)
	err = reader.InitStream(writer.data.Info(), writer.present.Info().GetLength())
	assert.Nil(t, err)
	err = reader.InitStream(writer.secondary.Info(), writer.present.Info().GetLength()+writer.data.Info().GetLength())
	assert.Nil(t, err)

	bopt := &api.BatchOption{RowSize: rows}
	vec, err := schema.CreateVector(bopt)
	if err != nil {
		t.Fatal(err)
	}
	if err = reader.NextBatch(vec); err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, values, vec.Vector)
}
