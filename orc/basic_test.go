package orc

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/patrickhuang888/goorc/orc/encoding"
	"github.com/patrickhuang888/goorc/pb/pb"
)

func init() {
	log.SetLevel(log.TraceLevel)
}

type bufSeeker struct {
	*bytes.Buffer
}

func (bs *bufSeeker) Seek(offset int64, whence int) (int64, error) {
	return offset, nil
}

func TestStreamReadWrite(t *testing.T) {
	data := &bytes.Buffer{}
	for i := 0; i < 100; i++ {
		data.WriteByte(byte(1))
	}
	for i := 0; i < 100; i++ {
		data.WriteByte(byte(i))
	}
	bs := data.Bytes()

	// expand to several chunks
	opts := &WriterOptions{ChunkSize: 60, CompressionKind: pb.CompressionKind_ZLIB}
	k := pb.Stream_DATA
	id := uint32(0)
	l := uint64(0)
	info := &pb.Stream{Kind: &k, Column: &id, Length: &l}
	sw := &streamWriter{info: info, buf: &bytes.Buffer{}, opts: opts}
	_, err := sw.write(data)
	if err != nil {
		t.Fatal(err)
	}

	in := &bufSeeker{&bytes.Buffer{}}
	if _, err := sw.flush(in); err != nil {
		t.Fatal(err)
	}

	vs := make([]byte, 500)
	ropts := &ReaderOptions{ChunkSize: 60, CompressionKind: pb.CompressionKind_ZLIB}
	sr := &streamReader{info: info, opts: ropts, buf: &bytes.Buffer{}, in: in}

	n, err := sr.Read(vs)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, 200, n)
	assert.Equal(t, bs, vs[:n])
}

func TestLongColumnRWwithNoPresents(t *testing.T) {
	schema := &TypeDescription{Id: 0, Kind: pb.Type_LONG}
	wopts := DefaultWriterOptions()
	batch := schema.CreateWriterBatch(wopts)

	var values []int64
	for i := 0; i < 100; i++ {
		values = append(values, int64(i))
	}
	batch.Vector = values

	writer := newLongV2Writer(schema, wopts)
	rows, err := writer.write(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, uint64(100), rows)

	ropts := DefaultReaderOptions()
	rbatch := schema.CreateReaderBatch(ropts)

	bs := &bufSeeker{writer.data.buf}
	kind_ := pb.Stream_DATA
	length_ := uint64(writer.data.buf.Len())
	info := &pb.Stream{Column: &schema.Id, Kind: &kind_, Length: &length_}

	cr := &crBase{schema: schema}
	dataStream := &streamReader{opts: ropts, info: info, buf: &bytes.Buffer{}, in: bs}
	data := &longV2StreamReader{decoder: &encoding.IntRleV2{Signed: true}, stream: dataStream}
	reader := &longV2Reader{crBase: cr, data: data}
	err = reader.next(rbatch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, values, rbatch.Vector)
}

func TestLongColumnRWwithPresents(t *testing.T) {
	schema := &TypeDescription{Id: 0, Kind: pb.Type_LONG, HasNulls: true}
	wopts := DefaultWriterOptions()
	batch := schema.CreateWriterBatch(wopts)

	//bool don't know how many values, so 13*8
	rows := 104

	values := make([]int64, rows)
	for i := 0; i < rows; i++ {
		values[i] = int64(i)
	}
	presents := make([]bool, rows)
	for i := 0; i < rows; i++ {
		presents[i] = true
	}
	presents[0] = false
	values[0] = 0
	presents[45] = false
	values[45] = 0

	// is it meaningfal last present is false?
	//presents[103]=false
	//values[103]= 0

	presents[102] = false
	values[102] = 0

	batch.Presents = presents
	batch.Vector = values

	writer := newLongV2Writer(schema, wopts)
	n, err := writer.write(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, uint64(rows), n)

	ropts := DefaultReaderOptions()
	batch = schema.CreateReaderBatch(ropts)
	presentBs := &bufSeeker{writer.present.buf}
	pKind := pb.Stream_PRESENT
	pLength_ := uint64(writer.present.buf.Len())
	pInfo := &pb.Stream{Column: &schema.Id, Kind: &pKind, Length: &pLength_}
	present := newBoolStreamReader(ropts, pInfo, 0, presentBs)

	dataBs := &bufSeeker{writer.data.buf}
	dKind := pb.Stream_DATA
	dLength := uint64(writer.data.buf.Len())
	dInfo := &pb.Stream{Column: &schema.Id, Kind: &dKind, Length: &dLength}
	data := newLongV2StreamReader(ropts, dInfo, 0, dataBs, true)

	cr := &crBase{schema: schema, present: present}
	reader := &longV2Reader{crBase: cr, data: data}
	err = reader.next(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, presents, batch.Presents)
	assert.Equal(t, values, batch.Vector)
}

func TestBoolColumnRWwithoutPresents(t *testing.T) {
	schema := &TypeDescription{Id: 0, Kind: pb.Type_BOOLEAN}
	wopts := DefaultWriterOptions()
	batch := schema.CreateWriterBatch(wopts)

	rows := 104
	values := make([]bool, rows)
	for i := 0; i < rows; i++ {
		values[i] = true
	}
	values[0] = false
	values[45] = false
	values[103] = false
	batch.Vector = values

	writer := newBoolWriter(schema, wopts)
	n, err := writer.write(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, uint64(rows), n)

	ropts := DefaultReaderOptions()
	rbatch := schema.CreateReaderBatch(ropts)

	bs := &bufSeeker{writer.data.buf}
	kind_ := pb.Stream_DATA
	length_ := uint64(writer.data.buf.Len())
	info := &pb.Stream{Column: &schema.Id, Kind: &kind_, Length: &length_}

	cr := &crBase{schema: schema}
	dataStream := &streamReader{opts: ropts, info: info, buf: &bytes.Buffer{}, in: bs}
	data := &boolStreamReader{decoder: &encoding.BoolRunLength{&encoding.ByteRunLength{}}, stream: dataStream}
	reader := &boolReader{crBase: cr, data: data, numberOfRows: uint64(rows)}
	err = reader.next(rbatch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, values, rbatch.Vector)
}

func TestFloatColumnWithPresents(t *testing.T) {
	schema := &TypeDescription{Id: 0, Kind: pb.Type_FLOAT, HasNulls: true}
	wopts := DefaultWriterOptions()
	batch := schema.CreateWriterBatch(wopts)

	rows := 100
	values := make([]float32, rows)
	for i := 0; i < rows; i++ {
		values[i] = float32(i)
	}
	presents := make([]bool, rows)
	for i := 0; i < rows; i++ {
		presents[i] = true
	}
	presents[0] = false
	values[0] = 0
	presents[45] = false
	values[45] = 0
	presents[98] = false
	values[98] = 0

	batch.Presents = presents
	batch.Vector = values

	writer := newFloatWriter(schema, wopts)
	n, err := writer.write(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, uint64(rows), n)

	ropts := DefaultReaderOptions()
	batch = schema.CreateReaderBatch(ropts)
	presentBs := &bufSeeker{writer.present.buf}
	pKind := pb.Stream_PRESENT
	pLength_ := uint64(writer.present.buf.Len())
	pInfo := &pb.Stream{Column: &schema.Id, Kind: &pKind, Length: &pLength_}
	present := newBoolStreamReader(ropts, pInfo, 0, presentBs)

	dataBs := &bufSeeker{writer.data.buf}
	dKind := pb.Stream_DATA
	dLength := uint64(writer.data.buf.Len())
	dInfo := &pb.Stream{Column: &schema.Id, Kind: &dKind, Length: &dLength}
	data := newFloatStreamReader(ropts, dInfo, 0, dataBs)

	cr := &crBase{schema: schema, present: present}
	reader := &floatReader{crBase: cr, data: data}
	err = reader.next(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, presents, batch.Presents[:100])
	assert.Equal(t, values, batch.Vector)
}

func TestDoubleColumnWithPresents(t *testing.T) {
	schema := &TypeDescription{Id: 0, Kind: pb.Type_DOUBLE, HasNulls: true}
	wopts := DefaultWriterOptions()
	batch := schema.CreateWriterBatch(wopts)

	rows := 100
	values := make([]float64, rows)
	for i := 0; i < rows; i++ {
		values[i] = float64(i)
	}
	presents := make([]bool, rows)
	for i := 0; i < rows; i++ {
		presents[i] = true
	}
	presents[0] = false
	values[0] = 0
	presents[45] = false
	values[45] = 0
	presents[98] = false
	values[98] = 0

	batch.Presents = presents
	batch.Vector = values

	writer := newDoubleWriter(schema, wopts)
	n, err := writer.write(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, uint64(rows), n)

	ropts := DefaultReaderOptions()
	batch = schema.CreateReaderBatch(ropts)
	presentBs := &bufSeeker{writer.present.buf}
	pKind := pb.Stream_PRESENT
	pLength_ := uint64(writer.present.buf.Len())
	pInfo := &pb.Stream{Column: &schema.Id, Kind: &pKind, Length: &pLength_}
	present := newBoolStreamReader(ropts, pInfo, 0, presentBs)

	dataBs := &bufSeeker{writer.data.buf}
	dKind := pb.Stream_DATA
	dLength := uint64(writer.data.buf.Len())
	dInfo := &pb.Stream{Column: &schema.Id, Kind: &dKind, Length: &dLength}
	data := newDoubleStreamReader(ropts, dInfo, 0, dataBs)

	cr := &crBase{schema: schema, present: present}
	reader := &doubleReader{crBase: cr, data: data}
	err = reader.next(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, presents, batch.Presents[:100])
	assert.Equal(t, values, batch.Vector)
}

func TestColumnStringDirectV2WithPresents(t *testing.T) {
	schema := &TypeDescription{Id: 0, Kind: pb.Type_STRING, HasNulls: true}
	wopts := DefaultWriterOptions()
	batch := schema.CreateWriterBatch(wopts)

	rows := 100
	values := make([]string, rows)
	for i := 0; i < rows; i++ {
		values[i] = fmt.Sprintf("string %d", i)
	}
	presents := make([]bool, rows)
	for i := 0; i < rows; i++ {
		presents[i] = true
	}
	presents[0] = false
	values[0] = ""
	presents[45] = false
	values[45] = ""
	presents[98] = false
	values[98] = ""

	batch.Presents = presents
	batch.Vector = values

	writer := newStringDirectV2Writer(schema, wopts)
	n, err := writer.write(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, uint64(rows), n)

	ropts := DefaultReaderOptions()
	batch = schema.CreateReaderBatch(ropts)
	presentBs := &bufSeeker{writer.present.buf}
	pKind := pb.Stream_PRESENT
	pLength_ := uint64(writer.present.buf.Len())
	pInfo := &pb.Stream{Column: &schema.Id, Kind: &pKind, Length: &pLength_}
	present := newBoolStreamReader(ropts, pInfo, 0, presentBs)

	dataBs := &bufSeeker{writer.data.buf}
	dKind := pb.Stream_DATA
	dLength := uint64(writer.data.buf.Len())
	dInfo := &pb.Stream{Column: &schema.Id, Kind: &dKind, Length: &dLength}
	data := newStringContentsStreamReader(ropts, dInfo, 0, dataBs)

	lengthBs := &bufSeeker{writer.length.buf}
	lKind := pb.Stream_LENGTH
	lLength := uint64(writer.length.buf.Len())
	lInfo := &pb.Stream{Column: &schema.Id, Kind: &lKind, Length: &lLength}
	length := newLongV2StreamReader(ropts, lInfo, 0, lengthBs, false)

	cr := &crBase{schema: schema, present: present}
	reader := &stringDirectV2Reader{crBase: cr, data: data, length: length}
	err = reader.next(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, presents, batch.Presents[:100])
	assert.Equal(t, values, batch.Vector)
}

func TestColumnTinyIntWithPresents(t *testing.T) {
	schema := &TypeDescription{Id: 0, Kind: pb.Type_BYTE, HasNulls: true}
	wopts := DefaultWriterOptions()
	batch := schema.CreateWriterBatch(wopts)

	rows := 100
	values := make([]byte, rows)
	for i := 0; i < rows; i++ {
		values[i] = byte(i)
	}
	presents := make([]bool, rows)
	for i := 0; i < rows; i++ {
		presents[i] = true
	}
	presents[0] = false
	values[0] = 0
	presents[45] = false
	values[45] = 0
	presents[98] = false
	values[98] = 0

	batch.Presents = presents
	batch.Vector = values

	writer := newByteWriter(schema, wopts)
	n, err := writer.write(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, uint64(rows), n)

	ropts := DefaultReaderOptions()
	batch = schema.CreateReaderBatch(ropts)
	presentBs := &bufSeeker{writer.present.buf}
	pKind := pb.Stream_PRESENT
	pLength_ := uint64(writer.present.buf.Len())
	pInfo := &pb.Stream{Column: &schema.Id, Kind: &pKind, Length: &pLength_}
	present := newBoolStreamReader(ropts, pInfo, 0, presentBs)

	dataBs := &bufSeeker{writer.data.buf}
	dKind := pb.Stream_DATA
	dLength := uint64(writer.data.buf.Len())
	dInfo := &pb.Stream{Column: &schema.Id, Kind: &dKind, Length: &dLength}
	data := newByteStreamReader(ropts, dInfo, 0, dataBs)

	cr := &crBase{schema: schema, present: present}
	reader := &byteReader{crBase: cr, data: data}
	err = reader.next(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, presents, batch.Presents[:100])
	assert.Equal(t, values, batch.Vector)
}

func TestColumnBinaryV2WithPresents(t *testing.T) {
	schema := &TypeDescription{Id: 0, Kind: pb.Type_BINARY, HasNulls: true}
	wopts := DefaultWriterOptions()
	batch := schema.CreateWriterBatch(wopts)

	rows := 100
	values := make([][]byte, rows)
	for i := 0; i < rows; i++ {
		values[i] = []byte{0b1101, byte(i)}
	}
	presents := make([]bool, rows)
	for i := 0; i < rows; i++ {
		presents[i] = true
	}
	presents[0] = false
	values[0] = nil
	presents[45] = false
	values[45] = nil
	presents[98] = false
	values[98] = nil

	batch.Presents = presents
	batch.Vector = values

	writer := newBinaryDirectV2Writer(schema, wopts)
	n, err := writer.write(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, uint64(rows), n)

	ropts := DefaultReaderOptions()
	batch = schema.CreateReaderBatch(ropts)
	presentBs := &bufSeeker{writer.present.buf}
	pKind := pb.Stream_PRESENT
	pLength_ := uint64(writer.present.buf.Len())
	pInfo := &pb.Stream{Column: &schema.Id, Kind: &pKind, Length: &pLength_}
	present := newBoolStreamReader(ropts, pInfo, 0, presentBs)

	dataBs := &bufSeeker{writer.data.buf}
	dKind := pb.Stream_DATA
	dLength := uint64(writer.data.buf.Len())
	dInfo := &pb.Stream{Column: &schema.Id, Kind: &dKind, Length: &dLength}
	data := newStringContentsStreamReader(ropts, dInfo, 0, dataBs)

	lengthBs := &bufSeeker{writer.length.buf}
	lKind := pb.Stream_LENGTH
	lLength := uint64(writer.length.buf.Len())
	lInfo := &pb.Stream{Column: &schema.Id, Kind: &lKind, Length: &lLength}
	length := newLongV2StreamReader(ropts, lInfo, 0, lengthBs, false)

	cr := &crBase{schema: schema, present: present}
	reader := &binaryV2Reader{crBase: cr, data: data, length: length}
	err = reader.next(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, presents, batch.Presents[:100])
	assert.Equal(t, values, batch.Vector)
}

func TestColumnDecimal64WithPresents(t *testing.T) {
	schema := &TypeDescription{Id: 0, Kind: pb.Type_DECIMAL, HasNulls:true}
	wopts:= DefaultWriterOptions()
	batch := schema.CreateWriterBatch(wopts)

	rows:= 100
	values:= make([]Decimal64, rows)
	for i := 0; i < rows; i++ {
		values[i]= Decimal64{Precision:int64(i), Scale:10}
	}
	presents:= make([]bool, rows)
	for i:=0; i<rows; i++ {
		presents[i]= true
	}
	presents[0]=false
	values[0]= Decimal64{}
	presents[45]=false
	values[45]= Decimal64{}
	presents[98]= false
	values[98]= Decimal64{}

	batch.Presents= presents
	batch.Vector= values

	writer:= newDecimal64DirectV2Writer(schema, wopts)
	n, err:=writer.write(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, uint64(rows), n)

	ropts := DefaultReaderOptions()
	batch = schema.CreateReaderBatch(ropts)
	presentBs := &bufSeeker{writer.present.buf}
	pKind := pb.Stream_PRESENT
	pLength_ := uint64(writer.present.buf.Len())
	pInfo := &pb.Stream{Column: &schema.Id, Kind: &pKind, Length: &pLength_}
	present := newBoolStreamReader(ropts, pInfo, 0, presentBs)

	dataBs := &bufSeeker{writer.data.buf}
	dKind := pb.Stream_DATA
	dLength := uint64(writer.data.buf.Len())
	dInfo := &pb.Stream{Column: &schema.Id, Kind: &dKind, Length: &dLength}
	data := newVarIntStreamReader(ropts, dInfo, 0, dataBs)

	secondaryBs := &bufSeeker{writer.secondary.buf}
	sKind := pb.Stream_SECONDARY
	sLength := uint64(writer.secondary.buf.Len())
	sInfo := &pb.Stream{Column: &schema.Id, Kind: &sKind, Length: &sLength}
	secondary := newLongV2StreamReader(ropts, sInfo, 0, secondaryBs, false)

	cr := &crBase{schema: schema, present: present}
	reader := &decimal64DirectV2Reader{crBase: cr, data: data, secondary: secondary}
	err = reader.next(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, presents, batch.Presents[:100])
	assert.Equal(t, values, batch.Vector)
}

func TestColumnDateWithPresents(t *testing.T) {
	schema := &TypeDescription{Id: 0, Kind: pb.Type_DATE, HasNulls: true}
	wopts := DefaultWriterOptions()
	batch := schema.CreateWriterBatch(wopts)

	rows := 100
	values := make([]Date, rows)
	for i := 0; i < rows; i++ {
		values[i] = NewDate(2020, time.February, i%30)
	}
	presents := make([]bool, rows)
	for i := 0; i < rows; i++ {
		presents[i] = true
	}
	presents[0] = false
	values[0] = Date{}
	presents[45] = false
	values[45] = Date{}
	presents[98] = false
	values[98] = Date{}

	batch.Presents = presents
	batch.Vector = values

	writer := newDateDirectV2Writer(schema, wopts)
	n, err := writer.write(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, uint64(rows), n)

	ropts := DefaultReaderOptions()
	batch = schema.CreateReaderBatch(ropts)
	presentBs := &bufSeeker{writer.present.buf}
	pKind := pb.Stream_PRESENT
	pLength_ := uint64(writer.present.buf.Len())
	pInfo := &pb.Stream{Column: &schema.Id, Kind: &pKind, Length: &pLength_}
	present := newBoolStreamReader(ropts, pInfo, 0, presentBs)

	dataBs := &bufSeeker{writer.data.buf}
	dKind := pb.Stream_DATA
	dLength := uint64(writer.data.buf.Len())
	dInfo := &pb.Stream{Column: &schema.Id, Kind: &dKind, Length: &dLength}
	data := newLongV2StreamReader(ropts, dInfo, 0, dataBs, true)

	cr := &crBase{schema: schema, present: present}
	reader := &dateV2Reader{crBase: cr, data: data}
	err = reader.next(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, presents, batch.Presents[:100])
	assert.Equal(t, values, batch.Vector)
}

func TestColumnTimestampWithPresents(t *testing.T) {
	schema := &TypeDescription{Id: 0, Kind: pb.Type_TIMESTAMP, HasNulls: true}
	wopts := DefaultWriterOptions()
	batch := schema.CreateWriterBatch(wopts)

	rows := 100
	values := make([]Timestamp, rows)
	for i := 0; i < rows; i++ {
		values[i] = GetTimestamp(time.Now())
	}
	presents := make([]bool, rows)
	for i := 0; i < rows; i++ {
		presents[i] = true
	}
	presents[0] = false
	presents[45] = false
	presents[98] = false
	batch.Presents = presents
	batch.Vector = values

	writer := newTimestampDirectV2Writer(schema, wopts)
	n, err := writer.write(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, uint64(rows), n)

	ropts := DefaultReaderOptions()
	batch = schema.CreateReaderBatch(ropts)
	presentBs := &bufSeeker{writer.present.buf}
	pKind := pb.Stream_PRESENT
	pLength_ := uint64(writer.present.buf.Len())
	pInfo := &pb.Stream{Column: &schema.Id, Kind: &pKind, Length: &pLength_}
	present := newBoolStreamReader(ropts, pInfo, 0, presentBs)

	dataBs := &bufSeeker{writer.data.buf}
	dKind := pb.Stream_DATA
	dLength := uint64(writer.data.buf.Len())
	dInfo := &pb.Stream{Column: &schema.Id, Kind: &dKind, Length: &dLength}
	data := newLongV2StreamReader(ropts, dInfo, 0, dataBs, true)

	secondaryBs := &bufSeeker{writer.secondary.buf}
	sKind := pb.Stream_SECONDARY
	sLength := uint64(writer.secondary.buf.Len())
	sInfo := &pb.Stream{Column: &schema.Id, Kind: &sKind, Length: &sLength}
	secondary := newLongV2StreamReader(ropts, sInfo, 0, secondaryBs, false)

	cr := &crBase{schema: schema, present: present}
	reader := &timestampV2Reader{crBase: cr, data: data, secondary: secondary}
	err = reader.next(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, presents, batch.Presents[:100])
	assert.Equal(t, values, batch.Vector)
}

func TestColumnStructWithPresents(t *testing.T) {
	schema := &TypeDescription{Id: 0, Kind: pb.Type_STRUCT, HasNulls: true}
	schema.Encoding = pb.ColumnEncoding_DIRECT
	child1 := &TypeDescription{Id: 0, Kind: pb.Type_INT}
	child1.Encoding = pb.ColumnEncoding_DIRECT_V2
	schema.ChildrenNames = []string{"child1"}
	schema.Children = []*TypeDescription{child1}
	if err := normalizeSchema(schema); err != nil {
		t.Fatalf("%+v", err)
	}

	wopts := DefaultWriterOptions()
	batch := schema.CreateWriterBatch(wopts)

	rows := 100

	presents := make([]bool, rows)
	for i := 0; i < rows; i++ {
		presents[i] = true
	}
	presents[0] = false
	presents[45] = false
	presents[98] = false
	batch.Presents = presents

	//rethink: just write not null values
	rows -=3
	values := make([]int64, rows)
	for i := 0; i < rows; i++ {
		values[i] = int64(i)
	}
	batch.Vector.([]*ColumnVector)[0].Vector = values

	writer, err := createColumnWriter(schema, wopts)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	n, err := writer.write(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, uint64(rows), n)

	ropts := DefaultReaderOptions()
	batch = schema.CreateReaderBatch(ropts)
	w:= writer.(*structWriter)
	presentBs := &bufSeeker{w.present.buf}
	pKind := pb.Stream_PRESENT
	pLength_ := uint64(w.present.buf.Len())
	pInfo := &pb.Stream{Column: &schema.Id, Kind: &pKind, Length: &pLength_}
	present := newBoolStreamReader(ropts, pInfo, 0, presentBs)

	intWriter:= w.childrenWriters[0].(*longV2Writer)
	dataBs := &bufSeeker{intWriter.data.buf}
	dKind := pb.Stream_DATA
	dLength := uint64(intWriter.data.buf.Len())
	dInfo := &pb.Stream{Column: &schema.Id, Kind: &dKind, Length: &dLength}
	data := newLongV2StreamReader(ropts, dInfo, 0, dataBs, true)

	intCr:= &crBase{schema:schema}
	intReader:= &longV2Reader{crBase:intCr, data:data}
	cr := &crBase{schema: schema, present: present}
	reader := &structReader{crBase: cr, children:[]columnReader{intReader}}
	err = reader.next(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, presents, batch.Presents[:100])
	// rethink: how struct present impl?
	assert.Equal(t, values, batch.Vector.([]*ColumnVector)[0].Vector)
}

func TestColumnStringUsingDictWithPresents(t *testing.T) {
	schema := &TypeDescription{Id: 0, Kind: pb.Type_STRING, HasNulls: true}
	wopts := DefaultWriterOptions()
	batch := schema.CreateWriterBatch(wopts)

	rows := 100
	values := make([]string, rows)
	for i := 0; i < rows; i++ {
		values[i] = fmt.Sprintf("string %d", i%4)
	}
	presents := make([]bool, rows)
	for i := 0; i < rows; i++ {
		presents[i] = true
	}
	presents[0] = false
	values[0] = ""
	presents[45] = false
	values[45] = ""
	presents[98] = false
	values[98] = ""

	batch.Presents = presents
	batch.Vector = values

	writer := newStringDictV2Writer(schema, wopts)
	n, err := writer.write(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, uint64(rows), n)


	ropts := DefaultReaderOptions()
	batch = schema.CreateReaderBatch(ropts)

	presentBs := &bufSeeker{writer.present.buf}
	pKind := pb.Stream_PRESENT
	pLength_ := uint64(writer.present.buf.Len())
	pInfo := &pb.Stream{Column: &schema.Id, Kind: &pKind, Length: &pLength_}
	present := newBoolStreamReader(ropts, pInfo, 0, presentBs)

	dataBs := &bufSeeker{writer.data.buf}
	dKind := pb.Stream_DATA
	dLength := uint64(writer.data.buf.Len())
	dInfo := &pb.Stream{Column: &schema.Id, Kind: &dKind, Length: &dLength}
	data := newLongV2StreamReader(ropts, dInfo, 0, dataBs, false)

	dDataBs := &bufSeeker{writer.dictData.buf}
	dDataKind := pb.Stream_DICTIONARY_DATA
	dDataLength := uint64(writer.dictData.buf.Len())
	dDataInfo := &pb.Stream{Column: &schema.Id, Kind: &dDataKind, Length: &dDataLength}
	dictData := newStringContentsStreamReader(ropts, dDataInfo, 0, dDataBs)

	dictLengthBs := &bufSeeker{writer.length.buf}
	dlKind := pb.Stream_LENGTH
	dlLength := uint64(writer.length.buf.Len())
	dlInfo := &pb.Stream{Column: &schema.Id, Kind: &dlKind, Length: &dlLength}
	dictLength := newLongV2StreamReader(ropts, dlInfo, 0, dictLengthBs, false)

	cr := &crBase{schema: schema, present: present}
	reader := &stringDictV2Reader{crBase: cr, data: data, dictData:dictData, dictLength:dictLength}
	err = reader.next(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, presents, batch.Presents[:100])
	assert.Equal(t, values, batch.Vector)
}
