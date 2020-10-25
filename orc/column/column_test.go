package column

import (
	"bytes"
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	"github.com/patrickhuang888/goorc/orc/encoding"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/patrickhuang888/goorc/pb/pb"
)

func init() {
	log.SetLevel(log.TraceLevel)
}

/*func TestLongWithNoPresents(t *testing.T) {
	schema := &api.TypeDescription{Id: 0, Kind: pb.Type_LONG}
	wopts := config.DefaultWriterOptions()

	var values []int64
	for i := 0; i < 100; i++ {
		values = append(values, int64(i))
	}

	writer := NewLongV2Writer(schema, wopts)
	rows, err := writer.write(&orc.batchInternal{ColumnVector: batch})
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, 100, rows)

	ropts := orc.DefaultReaderOptions()
	rbatch := schema.CreateReaderBatch(ropts)

	bs := &bufSeeker{writer.data.buf}
	kind_ := pb.Stream_DATA
	length_ := uint64(writer.data.buf.Len())
	info := &pb.Stream{Column: &schema.Id, Kind: &kind_, Length: &length_}

	cr := &orc.treeReader{schema: schema, numberOfRows: uint64(rows)}
	dataStream := &orc.streamReader{opts: ropts, info: info, buf: &bytes.Buffer{}, in: bs}
	data := &orc.longV2StreamReader{decoder: &encoding.IntRL2{Signed: true}, stream: dataStream}
	reader := &orc.longV2Reader{treeReader: cr, data: data}
	err = reader.next(rbatch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, 100, rbatch.ReadRows)
	assert.Equal(t, values, rbatch.Vector)
}

func TestLongColumnRWwithPresents(t *testing.T) {
	schema := &orc.TypeDescription{Id: 0, Kind: pb.Type_LONG, HasNulls: true}
	wopts := orc.DefaultWriterOptions()
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
	n, err := writer.write(&orc.batchInternal{ColumnVector: batch})
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, rows, n)

	ropts := orc.DefaultReaderOptions()
	batch = schema.CreateReaderBatch(ropts)
	presentBs := &bufSeeker{writer.present.buf}
	pKind := pb.Stream_PRESENT
	pLength_ := uint64(writer.present.buf.Len())
	pInfo := &pb.Stream{Column: &schema.Id, Kind: &pKind, Length: &pLength_}
	present := orc.newBoolStreamReader(ropts, pInfo, 0, presentBs)

	dataBs := &bufSeeker{writer.data.buf}
	dKind := pb.Stream_DATA
	dLength := uint64(writer.data.buf.Len())
	dInfo := &pb.Stream{Column: &schema.Id, Kind: &dKind, Length: &dLength}
	data := orc.newLongV2StreamReader(ropts, dInfo, 0, dataBs, true)

	base := &orc.treeReader{schema: schema, present: present, numberOfRows: uint64(rows)}
	reader := &orc.longV2Reader{treeReader: base, data: data}
	err = reader.next(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, presents, batch.Presents)
	assert.Equal(t, values, batch.Vector)
}*/

/*func TestBoolColumnRWwithoutPresents(t *testing.T) {
	schema := &orc.TypeDescription{Id: 0, Kind: pb.Type_BOOLEAN}
	wopts := orc.DefaultWriterOptions()
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
	n, err := writer.write(&orc.batchInternal{ColumnVector: batch})
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, rows, n)

	ropts := orc.DefaultReaderOptions()
	rbatch := schema.CreateReaderBatch(ropts)

	bs := &bufSeeker{writer.data.buf}
	kind_ := pb.Stream_DATA
	length_ := uint64(writer.data.buf.Len())
	info := &pb.Stream{Column: &schema.Id, Kind: &kind_, Length: &length_}

	cr := &orc.treeReader{schema: schema, numberOfRows: uint64(rows)}
	dataStream := &orc.streamReader{opts: ropts, info: info, buf: &bytes.Buffer{}, in: bs}
	data := &orc.boolStreamReader{decoder: &encoding.BoolRunLength{&encoding.ByteRunLength{}}, stream: dataStream}
	reader := &orc.boolReader{treeReader: cr, data: data}
	err = reader.next(rbatch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, values, rbatch.Vector)
}

func TestFloatColumnWithPresents(t *testing.T) {
	schema := &orc.TypeDescription{Id: 0, Kind: pb.Type_FLOAT, HasNulls: true}
	wopts := orc.DefaultWriterOptions()
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
	n, err := writer.write(&orc.batchInternal{ColumnVector: batch})
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, rows, n)

	ropts := orc.DefaultReaderOptions()
	batch = schema.CreateReaderBatch(ropts)
	presentBs := &bufSeeker{writer.present.buf}
	pKind := pb.Stream_PRESENT
	pLength_ := uint64(writer.present.buf.Len())
	pInfo := &pb.Stream{Column: &schema.Id, Kind: &pKind, Length: &pLength_}
	present := orc.newBoolStreamReader(ropts, pInfo, 0, presentBs)

	dataBs := &bufSeeker{writer.data.buf}
	dKind := pb.Stream_DATA
	dLength := uint64(writer.data.buf.Len())
	dInfo := &pb.Stream{Column: &schema.Id, Kind: &dKind, Length: &dLength}
	data := orc.newFloatStreamReader(ropts, dInfo, 0, dataBs)

	cr := &orc.treeReader{schema: schema, present: present, numberOfRows: uint64(rows)}
	reader := &orc.floatReader{treeReader: cr, data: data}
	err = reader.next(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, presents, batch.Presents[:100])
	assert.Equal(t, values, batch.Vector)
}

func TestDoubleColumnWithPresents(t *testing.T) {
	schema := &orc.TypeDescription{Id: 0, Kind: pb.Type_DOUBLE, HasNulls: true}
	wopts := orc.DefaultWriterOptions()
	batch := schema.CreateWriterBatch(wopts)

	rows := 100
	values := make([]float64, rows)
	for i := 0; i < rows; i++ {
		values[i] = float64(i)+0.11
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
	n, err := writer.write(&orc.batchInternal{ColumnVector: batch})
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, rows, n)

	ropts := orc.DefaultReaderOptions()
	batch = schema.CreateReaderBatch(ropts)
	presentBs := &bufSeeker{writer.present.buf}
	pKind := pb.Stream_PRESENT
	pLength_ := uint64(writer.present.buf.Len())
	pInfo := &pb.Stream{Column: &schema.Id, Kind: &pKind, Length: &pLength_}
	present := orc.newBoolStreamReader(ropts, pInfo, 0, presentBs)

	dataBs := &bufSeeker{writer.data.buf}
	dKind := pb.Stream_DATA
	dLength := uint64(writer.data.buf.Len())
	dInfo := &pb.Stream{Column: &schema.Id, Kind: &dKind, Length: &dLength}
	data := orc.newDoubleStreamReader(ropts, dInfo, 0, dataBs)

	cr := &orc.treeReader{schema: schema, present: present, numberOfRows: uint64(rows)}
	reader := &orc.doubleReader{treeReader: cr, data: data}
	err = reader.next(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, presents, batch.Presents)
	assert.Equal(t, values, batch.Vector)
}

func TestColumnStringDirectV2WithPresents(t *testing.T) {
	schema := &orc.TypeDescription{Id: 0, Kind: pb.Type_STRING, HasNulls: true}
	wopts := orc.DefaultWriterOptions()
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
	n, err := writer.write(&orc.batchInternal{ColumnVector: batch})
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, rows, n)

	ropts := orc.DefaultReaderOptions()
	batch = schema.CreateReaderBatch(ropts)
	presentBs := &bufSeeker{writer.present.buf}
	pKind := pb.Stream_PRESENT
	pLength_ := uint64(writer.present.buf.Len())
	pInfo := &pb.Stream{Column: &schema.Id, Kind: &pKind, Length: &pLength_}
	present := orc.newBoolStreamReader(ropts, pInfo, 0, presentBs)

	dataBs := &bufSeeker{writer.data.buf}
	dKind := pb.Stream_DATA
	dLength := uint64(writer.data.buf.Len())
	dInfo := &pb.Stream{Column: &schema.Id, Kind: &dKind, Length: &dLength}
	data := orc.newStringContentsStreamReader(ropts, dInfo, 0, dataBs)

	lengthBs := &bufSeeker{writer.length.buf}
	lKind := pb.Stream_LENGTH
	lLength := uint64(writer.length.buf.Len())
	lInfo := &pb.Stream{Column: &schema.Id, Kind: &lKind, Length: &lLength}
	length := orc.newLongV2StreamReader(ropts, lInfo, 0, lengthBs, false)

	cr := &orc.treeReader{schema: schema, present: present, numberOfRows: uint64(rows)}
	reader := &orc.stringDirectV2Reader{treeReader: cr, data: data, length: length}
	err = reader.next(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, presents, batch.Presents[:100])
	assert.Equal(t, values, batch.Vector)
}*/

func TestColumnTinyIntWithPresents(t *testing.T) {
	schema := &api.TypeDescription{Id: 0, Kind: pb.Type_BYTE, HasNulls: true}
	schema.Encoding= pb.ColumnEncoding_DIRECT

	wopts := config.DefaultWriterOptions()

	rows := 100
	values := make([]api.Value, rows)
	for i := 0; i < rows; i++ {
		values[i].V = byte(i)
	}
	values[0].Null = true
	values[45].Null = true
	values[98].Null = true

	writer := newByteWriter(schema, &wopts).(*byteWriter)
	for _, v := range values {
		if err:=writer.Write(v);err!=nil {
			t.Fatalf("%+v", err)
		}
	}

	if err := writer.Flush();err!=nil {
		t.Fatalf("%+v", err)
	}

	bb := make([]byte, 500)
	f := orcio.NewMockFile(bb)

	if _, err:= writer.WriteOut(f);err!=nil {
		t.Fatalf("%+v", err)
	}

	ropts := config.DefaultReaderOptions()
	vector:= schema.CreateReaderBatch(&ropts)
	reader := NewByteReader(schema, &ropts, f, uint64(rows))

	if err := reader.InitStream(writer.present.Info(), schema.Encoding, 0);err != nil {
		t.Fatalf("%+v", err)
	}
	if err:= reader.InitStream(writer.data.Info(), schema.Encoding, writer.present.Info().GetLength());err !=nil {
		t.Fatalf("%+v", err)
	}

	if _, err:= reader.Next(&vector.Presents, false, &vector.Vector);err!=nil {
		t.Fatalf("%+v", err)
	}

	assert.Equal(t, presents, vector.Presents)
	assert.Equal(t, values, vector.Vector)
}

/*func TestColumnBinaryV2WithPresents(t *testing.T) {
	schema := &orc.TypeDescription{Id: 0, Kind: pb.Type_BINARY, HasNulls: true}
	wopts := orc.DefaultWriterOptions()
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
	n, err := writer.write(&orc.batchInternal{ColumnVector: batch})
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, rows, n)

	ropts := orc.DefaultReaderOptions()
	batch = schema.CreateReaderBatch(ropts)
	presentBs := &bufSeeker{writer.present.buf}
	pKind := pb.Stream_PRESENT
	pLength_ := uint64(writer.present.buf.Len())
	pInfo := &pb.Stream{Column: &schema.Id, Kind: &pKind, Length: &pLength_}
	present := orc.newBoolStreamReader(ropts, pInfo, 0, presentBs)

	dataBs := &bufSeeker{writer.data.buf}
	dKind := pb.Stream_DATA
	dLength := uint64(writer.data.buf.Len())
	dInfo := &pb.Stream{Column: &schema.Id, Kind: &dKind, Length: &dLength}
	data := orc.newStringContentsStreamReader(ropts, dInfo, 0, dataBs)

	lengthBs := &bufSeeker{writer.length.buf}
	lKind := pb.Stream_LENGTH
	lLength := uint64(writer.length.buf.Len())
	lInfo := &pb.Stream{Column: &schema.Id, Kind: &lKind, Length: &lLength}
	length := orc.newLongV2StreamReader(ropts, lInfo, 0, lengthBs, false)

	cr := &orc.treeReader{schema: schema, present: present, numberOfRows: uint64(rows)}
	reader := &orc.binaryV2Reader{treeReader: cr, data: data, length: length}
	err = reader.next(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, presents, batch.Presents[:100])
	assert.Equal(t, values, batch.Vector)
}

func TestColumnDecimal64WithPresents(t *testing.T) {
	schema := &orc.TypeDescription{Id: 0, Kind: pb.Type_DECIMAL, HasNulls: true}
	wopts := orc.DefaultWriterOptions()
	batch := schema.CreateWriterBatch(wopts)

	rows := 100
	vector := make([]orc.Decimal64, rows)
	for i := 0; i < rows; i++ {
		vector[i] = orc.Decimal64{Precision: int64(i), Scale: 10}
	}
	presents := make([]bool, rows)
	for i := 0; i < rows; i++ {
		presents[i] = true
	}
	presents[0] = false
	vector[0] = orc.Decimal64{}
	presents[45] = false
	vector[45] = orc.Decimal64{}
	presents[98] = false
	vector[98] = orc.Decimal64{}

	batch.Presents = presents
	batch.Vector = vector

	writer := newDecimal64DirectV2Writer(schema, wopts)
	n, err := writer.write(&orc.batchInternal{ColumnVector: batch})
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, rows, n)

	ropts := orc.DefaultReaderOptions()
	batch = schema.CreateReaderBatch(ropts)
	presentBs := &bufSeeker{writer.present.buf}
	pKind := pb.Stream_PRESENT
	pLength_ := uint64(writer.present.buf.Len())
	pInfo := &pb.Stream{Column: &schema.Id, Kind: &pKind, Length: &pLength_}
	present := orc.newBoolStreamReader(ropts, pInfo, 0, presentBs)

	dataBs := &bufSeeker{writer.data.buf}
	dKind := pb.Stream_DATA
	dLength := uint64(writer.data.buf.Len())
	dInfo := &pb.Stream{Column: &schema.Id, Kind: &dKind, Length: &dLength}
	data := orc.newVarIntStreamReader(ropts, dInfo, 0, dataBs)

	secondaryBs := &bufSeeker{writer.secondary.buf}
	sKind := pb.Stream_SECONDARY
	sLength := uint64(writer.secondary.buf.Len())
	sInfo := &pb.Stream{Column: &schema.Id, Kind: &sKind, Length: &sLength}
	secondary := orc.newLongV2StreamReader(ropts, sInfo, 0, secondaryBs, true)

	cr := &orc.treeReader{schema: schema, present: present, numberOfRows: uint64(rows)}
	reader := &orc.decimal64DirectV2Reader{treeReader: cr, data: data, secondary: secondary}

	err = reader.next(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	assert.Equal(t, presents, batch.Presents)
	assert.Equal(t, vector, batch.Vector)
}

func TestColumnDateWithPresents(t *testing.T) {
	schema := &orc.TypeDescription{Id: 0, Kind: pb.Type_DATE, HasNulls: true}
	wopts := orc.DefaultWriterOptions()
	batch := schema.CreateWriterBatch(wopts)

	rows := 100
	values := make([]orc.Date, rows)
	for i := 0; i < rows; i++ {
		values[i] = orc.NewDate(2020, time.February, i%30)
	}
	presents := make([]bool, rows)
	for i := 0; i < rows; i++ {
		presents[i] = true
	}
	presents[0] = false
	values[0] = orc.Date{}
	presents[45] = false
	values[45] = orc.Date{}
	presents[98] = false
	values[98] = orc.Date{}

	batch.Presents = presents
	batch.Vector = values

	writer := newDateDirectV2Writer(schema, wopts)
	n, err := writer.write(&orc.batchInternal{ColumnVector: batch})
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, rows, n)

	ropts := orc.DefaultReaderOptions()
	batch = schema.CreateReaderBatch(ropts)
	presentBs := &bufSeeker{writer.present.buf}
	pKind := pb.Stream_PRESENT
	pLength_ := uint64(writer.present.buf.Len())
	pInfo := &pb.Stream{Column: &schema.Id, Kind: &pKind, Length: &pLength_}
	present := orc.newBoolStreamReader(ropts, pInfo, 0, presentBs)

	dataBs := &bufSeeker{writer.data.buf}
	dKind := pb.Stream_DATA
	dLength := uint64(writer.data.buf.Len())
	dInfo := &pb.Stream{Column: &schema.Id, Kind: &dKind, Length: &dLength}
	data := orc.newLongV2StreamReader(ropts, dInfo, 0, dataBs, true)

	cr := &orc.treeReader{schema: schema, present: present, numberOfRows: uint64(rows)}
	reader := &orc.dateV2Reader{treeReader: cr, data: data}
	err = reader.next(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, presents, batch.Presents[:100])
	assert.Equal(t, values, batch.Vector)
}

func TestColumnTimestampWithPresents(t *testing.T) {
	schema := &orc.TypeDescription{Id: 0, Kind: pb.Type_TIMESTAMP, HasNulls: true}
	wopts := orc.DefaultWriterOptions()
	batch := schema.CreateWriterBatch(wopts)

	rows := 100

	vector := make([]orc.Timestamp, rows)
	for i := 0; i < rows; i++ {
		vector[i] = orc.GetTimestamp(time.Now())
	}

	presents := make([]bool, rows)
	for i := 0; i < rows; i++ {
		presents[i] = true
	}
	presents[0] = false
	presents[45] = false
	presents[99] = false

	vector[0]= orc.Timestamp{}
	vector[45]= orc.Timestamp{}
	vector[99]= orc.Timestamp{}

	batch.Presents = presents
	batch.Vector = vector

	writer := newTimestampDirectV2Writer(schema, wopts)
	n, err := writer.write(&orc.batchInternal{ColumnVector: batch})
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, rows, n)

	ropts := orc.DefaultReaderOptions()
	batch = schema.CreateReaderBatch(ropts)

	presentBs := &bufSeeker{writer.present.buf}
	pKind := pb.Stream_PRESENT
	pLength_ := uint64(writer.present.buf.Len())
	pInfo := &pb.Stream{Column: &schema.Id, Kind: &pKind, Length: &pLength_}
	present := orc.newBoolStreamReader(ropts, pInfo, 0, presentBs)

	dataBs := &bufSeeker{writer.data.buf}
	dKind := pb.Stream_DATA
	dLength := uint64(writer.data.buf.Len())
	dInfo := &pb.Stream{Column: &schema.Id, Kind: &dKind, Length: &dLength}
	data := orc.newLongV2StreamReader(ropts, dInfo, 0, dataBs, true)

	secondaryBs := &bufSeeker{writer.secondary.buf}
	sKind := pb.Stream_SECONDARY
	sLength := uint64(writer.secondary.buf.Len())
	sInfo := &pb.Stream{Column: &schema.Id, Kind: &sKind, Length: &sLength}
	secondary := orc.newLongV2StreamReader(ropts, sInfo, 0, secondaryBs, false)

	cr := &orc.treeReader{schema: schema, present: present, numberOfRows: uint64(rows)}
	reader := &orc.timestampV2Reader{treeReader: cr, data: data, secondary: secondary}

	err = reader.next(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	assert.Equal(t, presents, batch.Presents)
	assert.Equal(t, vector, batch.Vector)
}

func TestColumnStructWithPresents(t *testing.T) {
	schema := &orc.TypeDescription{Id: 0, Kind: pb.Type_STRUCT, HasNulls: true}
	schema.Encoding = pb.ColumnEncoding_DIRECT
	child1 := &orc.TypeDescription{Id: 0, Kind: pb.Type_INT}
	child1.Encoding = pb.ColumnEncoding_DIRECT_V2
	schema.ChildrenNames = []string{"child1"}
	schema.Children = []*orc.TypeDescription{child1}
	schema.normalize()

	wopts := orc.DefaultWriterOptions()
	batch := schema.CreateWriterBatch(wopts)

	rows := 100

	presents := make([]bool, rows)
	for i := 0; i < rows; i++ {
		presents[i] = true
	}
	presents[0] = false
	presents[45] = false
	presents[99] = false
	batch.Presents = presents

	values := make([]int64, rows)
	for i := 0; i < rows; i++ {
		values[i] = int64(i)
	}
	values[0]= 0
	values[45]=0
	values[99]=0

	batch.Vector.([]*orc.ColumnVector)[0].Vector = values

	writer, err := createColumnWriter(schema, wopts)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	n, err := writer.write(&orc.batchInternal{ColumnVector: batch})
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, rows, n)

	ropts := orc.DefaultReaderOptions()
	rbatch := schema.CreateReaderBatch(ropts)

	w := writer.(*structWriter)

	presentBs := &bufSeeker{w.present.buf}
	pKind := pb.Stream_PRESENT
	pLength_ := uint64(w.present.buf.Len())
	pInfo := &pb.Stream{Column: &schema.Id, Kind: &pKind, Length: &pLength_}
	present := orc.newBoolStreamReader(ropts, pInfo, 0, presentBs)

	intWriter := w.children[0].(*longV2Writer)

	dataBs := &bufSeeker{intWriter.data.buf}
	dKind := pb.Stream_DATA
	dLength := uint64(intWriter.data.buf.Len())
	dInfo := &pb.Stream{Column: &schema.Id, Kind: &dKind, Length: &dLength}
	data := orc.newLongV2StreamReader(ropts, dInfo, 0, dataBs, true)

	intCr := &orc.treeReader{schema: schema, numberOfRows: uint64(rows)}
	intReader := &orc.longV2Reader{treeReader: intCr, data: data}

	cr := &orc.treeReader{schema: schema, present: present, numberOfRows: uint64(rows)}
	reader := &orc.structReader{treeReader: cr, children: []orc.columnReader{intReader}}

	err = reader.next(rbatch)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	assert.Equal(t, presents, rbatch.Presents)
	assert.Equal(t, values, rbatch.Vector.([]*orc.ColumnVector)[0].Vector)
}

func TestColumnStringUsingDictWithPresents(t *testing.T) {
	schema := &orc.TypeDescription{Id: 0, Kind: pb.Type_STRING, HasNulls: true}
	wopts := orc.DefaultWriterOptions()
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
	n, err := writer.write(&orc.batchInternal{ColumnVector: batch})
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, rows, n)

	ropts := orc.DefaultReaderOptions()
	batch = schema.CreateReaderBatch(ropts)

	presentBs := &bufSeeker{writer.present.buf}
	pKind := pb.Stream_PRESENT
	pLength_ := uint64(writer.present.buf.Len())
	pInfo := &pb.Stream{Column: &schema.Id, Kind: &pKind, Length: &pLength_}
	present := orc.newBoolStreamReader(ropts, pInfo, 0, presentBs)

	dataBs := &bufSeeker{writer.data.buf}
	dKind := pb.Stream_DATA
	dLength := uint64(writer.data.buf.Len())
	dInfo := &pb.Stream{Column: &schema.Id, Kind: &dKind, Length: &dLength}
	data := orc.newLongV2StreamReader(ropts, dInfo, 0, dataBs, false)

	dDataBs := &bufSeeker{writer.dictData.buf}
	dDataKind := pb.Stream_DICTIONARY_DATA
	dDataLength := uint64(writer.dictData.buf.Len())
	dDataInfo := &pb.Stream{Column: &schema.Id, Kind: &dDataKind, Length: &dDataLength}
	dictData := orc.newStringContentsStreamReader(ropts, dDataInfo, 0, dDataBs)

	dictLengthBs := &bufSeeker{writer.length.buf}
	dlKind := pb.Stream_LENGTH
	dlLength := uint64(writer.length.buf.Len())
	dlInfo := &pb.Stream{Column: &schema.Id, Kind: &dlKind, Length: &dlLength}
	dictLength := orc.newLongV2StreamReader(ropts, dlInfo, 0, dictLengthBs, false)

	cr := &orc.treeReader{schema: schema, present: present, numberOfRows: uint64(rows)}
	reader := &orc.stringDictV2Reader{treeReader: cr, data: data, dictData: dictData, dictLength: dictLength}

	err = reader.next(batch)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	assert.Equal(t, presents, batch.Presents[:100])
	assert.Equal(t, values, batch.Vector)
}
*/