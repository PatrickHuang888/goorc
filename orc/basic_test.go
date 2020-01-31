package orc

import (
	"bytes"
	"github.com/patrickhuang888/goorc/orc/encoding"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

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

func TestLongColumnReadWrite(t *testing.T) {
	schema := &TypeDescription{Id: 0, Kind: pb.Type_LONG}
	wopts := DefaultWriterOptions()
	batch := schema.CreateWriterBatch(wopts)

	var values []int64
	for i := 0; i < 100; i++ {
		values = append(values, int64(i))
	}
	batch.Vector = values

	writer:= newLongV2Writer(schema, wopts)
	rows, err := writer.write(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, uint64(100), rows)

	ropts := DefaultReaderOptions()
	rbatch := schema.CreateReaderBatch(ropts)

	bs:= &bufSeeker{writer.data.buf}
	kind_:= pb.Stream_DATA
	length_:= uint64(writer.data.buf.Len())
	info:= &pb.Stream{Column:&schema.Id, Kind:&kind_, Length:&length_}

	cr:= &crBase{schema: schema}
	dataStream:= &streamReader{opts:ropts, info:info, buf:&bytes.Buffer{}, in:bs}
	data:= &longV2StreamReader{decoder:&encoding.IntRleV2{Signed:true}, stream:dataStream}
	reader:= &longV2Reader{crBase: cr, data:data}
	err = reader.next(rbatch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, values, rbatch.Vector)
}

func TestLongReadWriteWithPresents(t *testing.T)  {
	schema := &TypeDescription{Id: 0, Kind: pb.Type_LONG, HasNulls:true}
	wopts:= DefaultWriterOptions()
	batch := schema.CreateWriterBatch(wopts)

	//bool don't know how many values, so 13*8
	rows:= 104

	values:= make([]int64, rows)
	for i := 0; i < rows; i++ {
		values[i]= int64(i)
	}
	presents:= make([]bool, rows)
	for i:=0; i<rows; i++ {
		presents[i]= true
	}
	presents[0]=false
	values[0]= 0
	presents[45]=false
	values[45]= 0
	presents[103]=false
	values[103]= 0

	batch.Presents= presents
	batch.Vector= values

	writer:= newLongV2Writer(schema, wopts)
	n, err:=writer.write(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, uint64(rows), n)

	ropts:=DefaultReaderOptions()
	batch= schema.CreateReaderBatch(ropts)
	presentBs:= &bufSeeker{writer.present.buf}
	pKind:= pb.Stream_PRESENT
	pLength_:= uint64(writer.present.buf.Len())
	pInfo:= &pb.Stream{Column:&schema.Id, Kind:&pKind, Length:&pLength_}
	present:= newBoolStreamReader(ropts, pInfo,0, presentBs)

	dataBs:= &bufSeeker{writer.data.buf}
	dKind:= pb.Stream_DATA
	dLength:= uint64(writer.data.buf.Len())
	dInfo:= &pb.Stream{Column:&schema.Id, Kind:&dKind, Length:&dLength}
	data:= newLongV2StreamReader(ropts, dInfo, 0, dataBs, true)

	cr:= &crBase{schema:schema, present:present}
	reader:= &longV2Reader{crBase: cr, data:data}
	err = reader.next(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, presents, batch.Presents)
	assert.Equal(t, values, batch.Vector)
}

func TestBoolColumnReadWrite(t *testing.T) {
	schema := &TypeDescription{Id: 0, Kind: pb.Type_BOOLEAN}
	wopts := DefaultWriterOptions()
	batch := schema.CreateWriterBatch(wopts)

	rows:=104
	values := make([]bool, rows)
	for i := 0; i < rows; i++ {
		values[i]= true
	}
	values[0]= false
	values[45]= false
	values[103]=false
	batch.Vector = values

	writer:= newBoolWriter(schema, wopts)
	n, err := writer.write(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, uint64(rows), n)

	ropts := DefaultReaderOptions()
	rbatch := schema.CreateReaderBatch(ropts)

	bs:= &bufSeeker{writer.data.buf}
	kind_:= pb.Stream_DATA
	length_:= uint64(writer.data.buf.Len())
	info:= &pb.Stream{Column:&schema.Id, Kind:&kind_, Length:&length_}

	cr:= &crBase{schema: schema}
	dataStream:= &streamReader{opts:ropts, info:info, buf:&bytes.Buffer{}, in:bs}
	data:= &boolStreamReader{decoder:&encoding.BoolRunLength{&encoding.ByteRunLength{}}, stream:dataStream}
	reader:= &boolReader{crBase: cr, data:data, numberOfRows:uint64(rows)}
	err = reader.next(rbatch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, values, rbatch.Vector)
}