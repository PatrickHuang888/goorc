package encoding

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

/*func init() {
	log.SetLevel(log.TraceLevel)
}*/

func TestByteRunLength(t *testing.T) {
	var values []byte
	var err error

	buf := bytes.NewBuffer([]byte{0x61, 0x00})
	brl := NewByteEncoder()

	values, err = DecodeByteRL(buf, values)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, 100, len(values))
	assert.Equal(t, byte(0), values[0])
	assert.Equal(t, byte(0), values[99])

	buf.Reset()
	buf.Write([]byte{0xfe, 0x44, 0x45})
	values = values[:0]
	values, err = DecodeByteRL(buf, values)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, 2, len(values))
	assert.Equal(t, byte(0x44), values[0])
	assert.Equal(t, byte(0x45), values[1])

	//
	vs := []byte{0x5, 0x5, 0x5, 0x5}
	buf.Reset()
	for _, v := range vs {
		if err = brl.Encode(v, buf); err != nil {
			t.Fatalf("fail %+v", err)
		}
		/*if i==2 {
			brl.MarkPosition()
		}*/
	}
	if err = brl.Flush(buf); err != nil {
		t.Fatalf("+%v", err)
	}

	values = values[:0]
	values, err = DecodeByteRL(buf, values)
	assert.Equal(t, vs, values)

	/*pos:= brl.GetAndClearPositions()
	assert.Equal(t, uint64(3), pos[0])*/

	vs = []byte{0x1, 0x5, 0x5, 0x5, 0x5}
	buf.Reset()
	for _, v := range vs {
		if err = brl.Encode(v, buf); err != nil {
			t.Fatalf("fail %+v", err)
		}
	}
	if err = brl.Flush(buf); err != nil {
		t.Fatalf("fail %+v", err)
	}

	values = values[:0]
	for buf.Len() != 0 {
		if values, err = DecodeByteRL(buf, values); err != nil {
			t.Fatalf("fail %+v", err)
		}
	}
	assert.Equal(t, vs, values)

	vs = []byte{0x1, 0x5, 0x5, 0x5, 0x5, 0x1}
	buf.Reset()
	for _, v := range vs {
		if err = brl.Encode(v, buf); err != nil {
			t.Fatalf("fail %+v", err)
		}
	}
	if err = brl.Flush(buf); err != nil {
		t.Fatalf("fail %+v", err)
	}

	values = values[:0]
	for buf.Len() != 0 {
		if values, err = DecodeByteRL(buf, values); err != nil {
			t.Fatalf("fail %+v", err)
		}
	}
	assert.Equal(t, vs, values)

	vs = []byte{0x01, 0x02, 0x03, 0x4, 0x05, 0x05, 0x05, 0x05, 0x06, 0x07, 0x08, 0x08, 0x08, 0x09, 0x10}
	buf.Reset()
	for _, v := range vs {
		if err = brl.Encode(v, buf); err != nil {
			t.Fatalf("fail %+v", err)
		}
		/*if i==4 {
			brl.MarkPosition()
		}*/
	}
	if err = brl.Flush(buf); err != nil {
		t.Fatalf("fail %+v", err)
	}

	/*p:= brl.GetAndClearPositions()
	assert.Equal(t, uint64(5), p[0])*/

	values = values[:0]
	for buf.Len() != 0 {
		if values, err = DecodeByteRL(buf, values); err != nil {
			t.Fatalf("fail %+v", err)
		}
	}
	assert.Equal(t, vs, values)

	vs = vs[:0]
	for i := 0; i <= 130; i++ { // run 131
		vs = append(vs, 0x01)
	}
	vs = append(vs, 0x02, 0x03)

	buf.Reset()
	for _, v := range vs {
		if err = brl.Encode(v, buf); err != nil {
			t.Fatalf("fail %+v", err)
		}
	}
	if err = brl.Flush(buf); err != nil {
		t.Fatalf("fail %+v", err)
	}

	values = values[:0]
	for buf.Len() != 0 {
		if values, err = DecodeByteRL(buf, values); err != nil {
			t.Fatalf("fail %+v", err)
		}
	}
	assert.Equal(t, vs, values)
}

func TestFindBytesRepeats(t *testing.T) {
	vs1 := []byte{0x01, 0x02, 0x02, 0x02, 0x03, 0x04}
	repeats := findRepeatsInBytes(vs1, 3)
	assert.Equal(t, 1, repeats[0].start)
	assert.Equal(t, 3, repeats[0].count)
	assert.Equal(t, 1, len(repeats))

	vs2 := []byte{0x01, 0x01, 0x01, 0x01, 0x01}
	repeats = findRepeatsInBytes(vs2, 3)
	assert.Equal(t, 0, repeats[0].start)
	assert.Equal(t, 5, repeats[0].count)

	vs3 := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
	repeats = findRepeatsInBytes(vs3, 3)
	assert.Equal(t, 0, len(repeats))
}

/*func TestDouble(test *testing.T) {
	vs := []float64{0.0001, 125.001, 1343822337.759, 0.8}
	c := &ieee754Double{}
	buf := &bytes.Buffer{}

	if err := c.Encode(buf, vs); err != nil {
		test.Fatalf("fail %+v", err)
	}

	var values []float64
	for ; buf.Len() != 0; {
		value, err := c.Decode(buf)
		if err != nil {
			test.Fatalf("fail %+v", err)
		}
		values = append(values, value)
	}
	assert.Equal(test, vs, values)
}*/

/*func TestIntRunLengthV1(t *testing.T) {
	t1 := bytes.NewBuffer([]byte{0x61, 0x00, 0x07})
	irl := &intRunLengthV1{signed: false}
	if err := irl.readValues(t1); err != nil {
		t.Error(err)
	}
	if irl.numLiterals != 100 {
		t.Fatal("num literals error")
	}
	if irl.uliterals[0] != 7 || irl.uliterals[99] != 7 {
		t.Fatal("literal error")
	}
	irl.reset()
	irl.signed = false
	t2 := bytes.NewBuffer([]byte{0xfb, 0x02, 0x03, 0x04, 0x07, 0xb})
	err := irl.readValues(t2)
	assertx.Nil(t, err)
	assertx.Equal(t, irl.numLiterals, 5)
	if irl.uliterals[0] != 2 {
		t.Fatal("uliteral error")
	}
	if irl.uliterals[4] != 11 {
		t.Fatal("uliteral error")
	}
}*/

func TestIntRunLengthV2_Delta(t *testing.T) {
	var err error
	irl := NewIntRLV2(false)

	uvs := []uint64{2, 3, 5, 7, 11, 13, 17, 19, 23, 29}
	bs := []byte{0xc6, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46}
	buf := bytes.NewBuffer(bs)
	vector, err := irl.Decode(buf)
	if err != nil {
		t.Fatal(err)
	}
	uvalues := vector.([]uint64)
	assert.Equal(t, 10, len(uvalues))
	assert.EqualValues(t, uvs, uvalues)
	irl.Reset()

	buf.Reset()
	for _, v := range uvs {
		if err = irl.Encode(v, buf); err != nil {
			t.Fatalf("%+v", err)
		}
	}
	if err := irl.Flush(buf); err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, bs, buf.Bytes())

	vs := []int64{-2, -3, -5, -7, -11, -13, -17, -19, -23, -29}
	irl.signed = true
	buf.Reset()
	for _, v := range vs {
		err = irl.Encode(v, buf)
		assert.Nil(t, err)
	}
	if err = irl.Flush(buf); err != nil {
		t.Fatalf("%+v", err)
	}
	vector, err = irl.Decode(buf)
	assert.Nil(t, err)
	assert.Equal(t, vs, vector.([]int64))
	irl.Reset()

	// fixed delta 0
	vs = []int64{-2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2}
	irl.signed = true
	buf.Reset()
	for _, v := range vs {
		err = irl.Encode(v, buf)
		assert.Nil(t, err)
	}
	err = irl.Flush(buf)
	assert.Nil(t, err)
	vector, err = irl.Decode(buf)
	assert.Nil(t, err)
	assert.Equal(t, vs, vector.([]int64))
	irl.Reset()

	// over 512 numbers with uint
	uvs = uvs[:0]
	for i := 0; i < 1000; i++ {
		uvs = append(uvs, uint64(i))
	}
	irl.signed = false
	buf.Reset()
	for _, v := range uvs {
		err = irl.Encode(v, buf)
		assert.Nil(t, err)
	}
	err = irl.Flush(buf)
	assert.Nil(t, err)
	uvalues = uvalues[:0]
	for {
		vector, err = irl.Decode(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("%+v", err)
		}
		uvalues = append(uvalues, vector.([]uint64)...)
	}
	irl.Reset()
	assert.Equal(t, uvs, uvalues)

	// number over 512 with int
	vs = vs[:0]
	for i := 0; i < 1500; i++ {
		vs = append(vs, int64(1000-i))
	}
	irl.signed = true
	buf.Reset()
	for _, v := range vs {
		err = irl.Encode(v, buf)
		assert.Nil(t, err)
	}
	err = irl.Flush(buf)
	assert.Nil(t, err)

	var values []int64
	for {
		vector, err = irl.Decode(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
		}
		values = append(values, vector.([]int64)...)
	}
	irl.Reset()
	assert.Equal(t, vs, values)
}

func TestIntRunLengthV2Direct(t *testing.T) {
	irl := NewIntRLV2(false)
	buf:= &bytes.Buffer{}

	//uint
	uvs := []uint64{23713, 57005, 43806, 48879}
	encoded := []byte{0x5e, 0x03, 0x5c, 0xa1, 0xde, 0xad, 0xab, 0x1e, 0xbe, 0xef}
	for _, v := range uvs {
		if err:= irl.Encode(v, buf);err!=nil {
			t.Fatalf("%+v", err)
		}
	}
	if err:= irl.Flush(buf);err!=nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, encoded, buf.Bytes())
	vector, err := irl.Decode(buf)
	assert.Nil(t, err)
	assert.Equal(t, uvs, vector.([]uint64))
	irl.Reset()

	uvs = []uint64{999, 900203003, 688888888, 857340643}
	buf.Reset()
	for _, v := range uvs {
		if err = irl.Encode(v, buf);err!=nil {
			t.Fatalf("%+v", err)
		}
	}
	err= irl.Flush(buf)
	assert.Nil(t, err)
	vector, err = irl.Decode(buf)
	assert.Nil(t, err)
	assert.Equal(t, uvs, vector.([]uint64))
	irl.Reset()

	uvs = []uint64{6, 7, 8} // width 4
	buf.Reset()
	if err := irl.writeDirect(buf, false, uvs); err != nil {
		t.Fatalf("%+v", err)
	}
	irl.Reset()
	vector, err = irl.Decode(buf)
	assert.Nil(t, err)
	assert.Equal(t, uvs, vector.([]uint64))
	irl.Reset()

	// int
	irl.signed = true
	values := []int64{1, 1, 2, 2, 2, 2, 2} // width 2 -> aligned to 4
	uvs = uvs[:0]
	for _, v := range values {
		uvs = append(uvs, Zigzag(v))
	}
	buf.Reset()
	if err := irl.writeDirect(buf, true, uvs); err != nil {
		t.Fatalf("%+v", err)
	}
	irl.Reset()
	vector, err = irl.Decode(buf)
	assert.Nil(t, err)
	assert.Equal(t, values, vector.([]int64))
	irl.Reset()

	values = []int64{6, 7, 8} // width 5 after zigzag ?
	uvs = uvs[:0]
	for _, v := range values {
		uvs = append(uvs, Zigzag(v))
	}
	buf.Reset()
	if err := irl.writeDirect(buf, false, uvs); err != nil {
		t.Fatalf("%+v", err)
	}
	vector, err = irl.Decode(buf)
	assert.Nil(t, err)
	assert.Equal(t, values, vector.([]int64))
	irl.Reset()

	// test width 16
	uvalue:=uint64(0x5ff)
	irl.signed= false
	buf.Reset()
	err=irl.Encode(uvalue, buf)
	assert.Nil(t, err)
	err=irl.Flush(buf)
	assert.Nil(t, err)
	vector, err=irl.Decode(buf)
	assert.Nil(t, err)
	assert.Equal(t, []uint64{uvalue}, vector.([]uint64))
	irl.Reset()

	// test width 11
	uvs= []uint64{0b100_0000_0001, 0b100_0000_0011}
	irl.signed=false
	buf.Reset()
	if err := irl.writeDirect(buf, false, uvs); err != nil {
		t.Fatalf("%+v", err)
	}
	if vector, err = irl.Decode(buf); err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, uvs, vector.([]uint64))
}

func TestIntRunLengthV2Patch(t *testing.T) {
	irl := NewIntRLV2(true)

	values := []int64{2030, 2000, 2020, 1000000, 2040, 2050, 2060, 2070, 2080, 2090, 2100, 2110, 2120, 2130,
		2140, 2150, 2160, 2170, 2180, 2190}
	bs := []byte{0x8e, 0x13, 0x2b, 0x21, 0x07, 0xd0, 0x1e, 0x00, 0x14, 0x70, 0x28, 0x32, 0x3c, 0x46, 0x50, 0x5a,
		0x64, 0x6e, 0x78, 0x82, 0x8c, 0x96, 0xa0, 0xaa, 0xb4, 0xbe, 0xfc, 0xe8}
	buf:= bytes.NewBuffer(bs)

	vector, err := irl.Decode(buf)
	assert.Nil(t, err)
	assert.Equal(t, values, vector.([]int64))
	irl.Reset()

	buf.Reset()
	for _, v :=range values {
		if err := irl.Encode(v, buf); err != nil {
			t.Fatalf("fail %+v", err)
		}
	}
	err=irl.Flush(buf)
	assert.Nil(t, err)
	assert.Equal(t, bs, buf.Bytes())

	values = []int64{-2030, -2000, -2020, 1000000, 2040, -2050, -2060, -2070, -2080, -2090, -2100, -2110, -2120, -2130,
		-2140, -2150, -2160, -2170, -2180, -2190}
	buf.Reset()
	for _, v := range values {
		if err := irl.Encode(v,buf); err != nil {
			t.Fatalf("encoding error %+v", err)
		}
	}
	err=irl.Flush(buf)
	assert.Nil(t, err)
	if vector, err = irl.Decode(buf); err != nil {
		t.Fatalf("decoding error %+v", err)
	}
	assert.Equal(t, values, vector.([]int64))
}

func TestIntRunLengthV2(t *testing.T) {
	irl := NewIntRLV2(false)
	buf := &bytes.Buffer{}

	//short repeat
	bs := []byte{0x0a, 0x27, 0x10}
	vector, err := irl.Decode(bytes.NewBuffer(bs))
	assert.Nil(t, err)
	assert.Equal(t, 5, len(vector.([]uint64)))
	assert.Equal(t, uint64(10000), vector.([]uint64)[0])


	irl.signed = true
	values := make([]int64, 10)
	for i := 0; i < 10; i++ {
		values[i] = -1
	}
	for _, v := range values {
		err = irl.Encode(v, buf)
		assert.Nil(t, err)
	}
	err=irl.Flush(buf)
	assert.Nil(t, err)
	vector, err = irl.Decode(buf)
	assert.Nil(t, err)
	assert.Equal(t, values, vector.([]int64))
	irl.Reset()

	// direct
	uvalues := []uint64{23713, 43806, 57005, 48879}
	bs = []byte{0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xef}
	irl.signed= false
	buf = bytes.NewBuffer(bs)
	vector, err = irl.Decode(buf)
	assert.Nil(t, err)
	assert.Equal(t, uvalues, vector.([]uint64))
}

func TestZigzag(t *testing.T) {
	assert.Equal(t, uint64(1), Zigzag(-1))
	assert.Equal(t, int64(-1), UnZigzag(1))

	var x int64 = 2147483647
	assert.Equal(t, uint64(4294967294), Zigzag(x))
	assert.Equal(t, x, UnZigzag(Zigzag(x)))

	var y int64 = -2147483648
	assert.Equal(t, uint64(4294967295), Zigzag(y))
	assert.Equal(t, y, UnZigzag(Zigzag(y)))

	assert.Equal(t, 1, int(UnZigzag(Zigzag(1))))
}

/*func TestChunkHeader(t *testing.T) {
	l := 100000
	v := []byte{0x40, 0x0d, 0x03}

	h := encChunkHeader(l, false)
	assert.Equal(t, h, v)
	dl, o := decChunkHeader(v)
	assert.Equal(t, l, dl)
	assert.Equal(t, o, false)
}*/

func TestTimestampTrailing(t *testing.T) {
	assert.Equal(t, uint64(0x0a), EncodingNano(1000))
	assert.Equal(t, uint64(0x0c), EncodingNano(100000))
}

func TestNanoEncoding(t *testing.T) {
	assert.Equal(t, uint64(0x0a), EncodingNano(uint64(1000)))
	assert.Equal(t, uint64(0x0c), EncodingNano(uint64(100000)))
}

func TestBoolRunLength (t *testing.T) {
	values := []bool{true, false, false, false, false, false, false, false}
	encoded := []byte{0xff, 0x80}

	var err error

	buf := &bytes.Buffer{}
	brl := NewBoolEncoder()

	for _, v := range values {
		if err = brl.Encode(v, buf); err != nil {
			t.Fatalf("%+v", err)
		}
	}
	if err = brl.Flush(buf); err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, encoded, buf.Bytes())

	var vs []bool
	vs, err = DecodeBools(buf, vs)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, values, vs)

	//
	values = make([]bool, 100)
	values[0] = true

	buf.Reset()
	for _, v := range values {
		if err = brl.Encode(v, buf);err != nil {
			t.Fatalf("%+v", err)
		}
	}
	if err = brl.Flush(buf); err != nil {
		t.Fatalf("%+v", err)
	}

	vs = vs[:0]
	for buf.Len() != 0 {
		vs, err = DecodeBools(buf, vs)
		if err != nil {
			t.Fatalf("%+v", err)
		}
	}
	assert.Equal(t, values, vs[:100])

	//
	values[44] = true
	values[99] = true

	buf.Reset()
	for _, v := range values{
		if err = brl.Encode(v, buf);err != nil {
			t.Fatalf("%+v", err)
		}
		//if i==99 {
		//	brl.MarkPosition()
		//}
	}
	if err = brl.Flush(buf); err != nil {
		t.Fatalf("%+v", err)
	}

	//p:=brl.GetAndClearPositions()
	//assert.Equal(t, uint64(4), p[0])

	vs = vs[:0]
	for buf.Len() != 0 {
		vs, err = DecodeBools(buf, vs)
		if err != nil {
			t.Fatalf("%+v", err)
		}
	}
	assert.Equal(t, values, vs[:100])

	rows := 100
	bb := make([]bool, rows)
	for i := 0; i < rows; i++ {
		bb[i] = true
	}
	bb[0] = false
	bb[45] = false
	bb[98] = false

	buf.Reset()
	for _, v := range bb {
		if err= brl.Encode(v, buf);err!=nil {
			t.Fatalf("%+v", err)
		}
	}
	if err = brl.Flush(buf); err != nil {
		t.Fatalf("%+v", err)
	}

	vs = vs[:0]
	for buf.Len() != 0 {
		vs, err = DecodeBools(buf, vs)
		if err != nil {
			t.Fatalf("%+v", err)
		}
	}
	assert.Equal(t, bb, vs[:100])
}