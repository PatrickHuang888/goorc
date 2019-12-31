package encoding

import (
	"bytes"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func init() {
	logrus.SetLevel(logrus.TraceLevel)
}

func TestByteRunLength(t *testing.T) {
	var values []byte
	var err error

	buf := bytes.NewBuffer([]byte{0x61, 0x00})
	brl := &ByteRunLength{}

	values, err = brl.ReadValues(buf, values)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, 100, len(values))
	assert.Equal(t, byte(0), values[0])
	assert.Equal(t, byte(0), values[99])

	values=values[:0]
	t2 := bytes.NewBuffer([]byte{0xfe, 0x44, 0x45})
	values, err = brl.ReadValues(t2, values)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, 2, len(values))
	assert.Equal(t, byte(0x44), values[0])
	assert.Equal(t, byte(0x45), values[1])


	/*vs := []byte{0x01, 0x02, 0x03, 0x4, 0x05, 0x05, 0x05, 0x05, 0x06, 0x07, 0x08, 0x08, 0x08, 0x09, 0x10}
	buf.Reset()
	if err := brl.WriteValues(vs, buf); err != nil {
		t.Fatalf("fail %+v", err)
	}
	result= result[:0]
	if result, err = brl.ReadValues(buf, result); err != nil {
		t.Fatalf("fail %+v", err)
	}
	assert.Equal(t, vs, result)

	vs = vs[:0]
	for i := 0; i <= 130; i++ { // run 131
		vs = append(vs, 0x01)
	}
	vs = append(vs, 0x02, 0x03)

	buf.Reset()
	if err := brl.WriteValues(vs, buf); err != nil {
		t.Fatalf("fail %+v", err)
	}
	result= result[:0]
	if result, err = brl.ReadValues(buf, result); err != nil {
		t.Fatalf("fail %+v", err)
	}
	assert.Equal(t, vs, result)*/

}

func TestFindBytesRepeats(t *testing.T) {
	vs1 := []byte{0x01,0x02, 0x02, 0x02, 0x03, 0x04}
	repeats:= findRepeatsInBytes(vs1, 3)
	assert.Equal(t, 1, repeats[0].start)
	assert.Equal(t, 3, repeats[0].count)
	assert.Equal(t, 1, len(repeats))

	vs2 := []byte{0x01, 0x01, 0x01, 0x01, 0x01}
	repeats= findRepeatsInBytes(vs2, 3)
	assert.Equal(t,  0, repeats[0].start)
	assert.Equal(t, 5, repeats[0].count)

	vs3 := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
	repeats= findRepeatsInBytes(vs3, 3)
	assert.Equal(t,  0, len(repeats))
}

/*func TestBoolRunLength(t *testing.T) {
	vs := []bool{true, false, false, false, false, false, false, false}
	bs := []byte{0xff, 0x80}

	brl := &BoolRunLength{}
	brl.bools = vs
	buf := &bytes.Buffer{}
	buf.Reset()

	if err := brl.writeValues(buf); err != nil {
		t.Fatalf("fail %+v", err)
	}
	assert.Equal(t, bs, buf.Bytes())

	brl.reset()
	if err := brl.readValues(buf); err != nil {
		t.Fatalf("fail %+v", err)
	}
	assert.Equal(t, vs, brl.bools)
}*/

func TestDouble(test *testing.T) {
	vs := []float64{0.0001, 125.001, 1343822337.759, 0.8}
	c := &Ieee754Double{}
	buf := &bytes.Buffer{}

	if err := c.WriteValues(buf, vs); err != nil {
		test.Fatalf("fail %+v", err)
	}

	var values []float64
	for ; buf.Len()!=0; {
		value, err := c.ReadValue(buf)
		if err != nil {
			test.Fatalf("fail %+v", err)
		}
		values= append(values, value)
	}
	assert.Equal(test, vs, values)
}

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
	irl := &IntRleV2{}
	irl.Signed = false

	uvs := []uint64{2, 3, 5, 7, 11, 13, 17, 19, 23, 29}
	bs := []byte{0xc6, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46}
	var uvalues []uint64
	buf := bytes.NewBuffer(bs)

	uvalues, err = irl.ReadValues(buf, uvalues)
	if err!=nil {
		t.Fatal(err)
	}
	assert.Equal(t, 10, len(uvalues))
	assert.EqualValues(t, uvs, uvalues)

	buf.Reset()
	err = irl.WriteValues(buf, uvs)
	assert.Nil(t, err)
	assert.Equal(t, bs, buf.Bytes())


	vs := []int64{-2, -3, -5, -7, -11, -13, -17, -19, -23, -29}
	irl.Signed = true
	uvs= uvs[:0]
	for _, v := range vs{
		uvs= append(uvs, Zigzag(v))
	}

	buf.Reset()
	err = irl.WriteValues(buf, uvs)
	assert.Nil(t, err)

	var values []int64
	uvs= uvs[:0]
	uvs, err = irl.ReadValues(buf, uvs)
	for _, v :=range uvs {
		values= append(values, UnZigzag(v))
	}
	assert.Nil(t, err)
	assert.Equal(t,  vs, values)

	// fixed delta 0
	vs = []int64{-2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2}
	irl.Signed = true

	uvs= uvs[:0]
	for _, v := range vs {
		uvs= append(uvs, Zigzag(v))
	}
	buf.Reset()
	err = irl.WriteValues(buf, uvs)
	assert.Nil(t, err)

	uvs= uvs[:0]
	uvs, err = irl.ReadValues(buf, uvs)
	assert.Nil(t, err)
	values= values[:0]
	for _, v := range uvs {
		values= append(values, UnZigzag(v))
	}
	assert.Equal(t, vs, values)

	// over 512 numbers with uint
	uvs= uvs[:0]
	for i := 0; i < 1000; i++ {
		uvs = append(uvs, uint64(i))
	}
	irl.Signed = false

	buf.Reset()
	err = irl.WriteValues(buf, uvs)
	assert.Nil(t, err)

	uvs= uvs[:0]
	for buf.Len()!=0 {
		uvs, err = irl.ReadValues(buf,  uvs)
		assert.Nil(t, err)
	}
	assert.Equal(t, 1000, len(uvs))
	assert.Equal(t, uint64(0), uvs[0])
	assert.Equal(t, uint64(999), uvs[999])

	// number over 512 with int
	uvs= uvs[:0]
	for i := 0; i < 1500; i++ {
		uvs = append(uvs, Zigzag(int64(1000 - i)))
	}
	irl.Signed = true

	buf.Reset()
	err = irl.WriteValues(buf, uvs)
	assert.Nil(t, err)

	uvs=uvs[:0]
	for buf.Len()!=0 {
		uvs, err = irl.ReadValues(buf, uvs)
		assert.Nil(t, err)
	}
	vs= vs[:0]
	for _, v := range uvs {
		vs= append(vs, UnZigzag(v))
	}
	assert.Equal(t, 1500, len(vs))
	assert.Equal(t, int64(1000), vs[0])
	assert.Equal(t, int64(-499), vs[1499])
}

func TestIntRunLengthV2Direct(t *testing.T) {
	irl := &IntRleV2{}
	buf := &bytes.Buffer{}

	//uint
	uvs := []uint64{23713, 57005, 43806, 48879}
	encoded := []byte{0x5e, 0x03, 0x5c, 0xa1, 0xde, 0xad, 0xab, 0x1e, 0xbe, 0xef}
	irl.Signed = false

	err := irl.WriteValues(buf, uvs)
	assert.Nil(t, err)
	assert.Equal(t, encoded, buf.Bytes())

	var uvalues []uint64
	uvalues, err = irl.ReadValues(buf, uvalues)
	assert.Equal(t, uvs, uvalues)

	uvs = []uint64{999, 900203003, 688888888, 857340643}

	buf.Reset()
	err = irl.WriteValues(buf, uvs)
	assert.Nil(t, err)

	uvalues= uvalues[:0]
	uvalues, err = irl.ReadValues(buf, uvalues)
	assert.Nil(t, err)
	assert.Equal(t, uvs, uvalues)

	uvalues = []uint64{6, 7, 8} // width 4
	buf.Reset()
	if err := irl.writeDirect(buf, false, uvalues); err != nil {
		t.Fatalf("%+v", err)
	}
	uvs= uvs[:0]
	uvs, err = irl.ReadValues(buf, uvs)
	assert.Nil(t, err)
	assert.Equal(t, uvalues, uvs)



	// int
	irl.Signed = true
	values := []int64{1, 1, 2,2,2,2,2} // width 2
	uvs= uvs[:0]
	for _, v:= range values {
		uvs= append(uvs, Zigzag(v))
	}
	buf.Reset()
	if err := irl.writeDirect(buf, true, uvs); err != nil {
		t.Fatalf("%+v", err)
	}

	uvs= uvs[:0]
	uvs, err = irl.ReadValues(buf, uvs)
	assert.Nil(t, err)
	var vs []int64
	for _, v :=range uvs {
		vs= append(vs, UnZigzag(v))
	}
	assert.Equal(t, values, vs)

	values = []int64{6, 7, 8} // width 8 because zigzag
	uvs= uvs[:0]
	for _, v :=range values {
		uvs= append(uvs, Zigzag(v))
	}
	buf.Reset()
	if err := irl.writeDirect(buf, false, uvs); err != nil {
		t.Fatalf("%+v", err)
	}
	uvs= uvs[:0]
	uvs, err = irl.ReadValues(buf,  uvs)
	assert.Nil(t, err)
	vs= vs[:0]
	for _, v := range uvs {
		vs= append(vs, UnZigzag(v))
	}
	assert.Equal(t, values, vs)

	// test width 16
	/*irl.sub= Encoding_DIRECT
	irl.signed=false
	irl.uliterals= []uint64{0x5ff}
	buf.Reset()
	if err := irl.writeDirect(buf, 0, 1, true); err != nil {
		t.Fatalf("%+v", err)
	}
	irl.reset()
	if err := irl.readValues(buf); err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, uint64(0x5ff), irl.uliterals[0])

	// test width 11
	irl.reset()
	irl.sub= Encoding_DIRECT
	irl.signed=false
	irl.uliterals= []uint64{0b100_0000_0001, 0b100_0000_0011}
	buf.Reset()
	if err := irl.writeDirect(buf, 0, 2, false); err != nil {
		t.Fatalf("%+v", err)
	}
	irl.reset()
	if err := irl.readValues(buf); err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, 2, irl.len())
	assert.Equal(t, uint64(0b100_0000_0001), irl.uliterals[0])
	assert.Equal(t, uint64(0b100_0000_0011), irl.uliterals[1])*/
}

func TestIntRunLengthV2Patch(t *testing.T) {
	irl := &IntRleV2{}
	irl.Signed = true

	values := []int64{2030, 2000, 2020, 1000000, 2040, 2050, 2060, 2070, 2080, 2090, 2100, 2110, 2120, 2130,
		2140, 2150, 2160, 2170, 2180, 2190}
	bs := []byte{0x8e, 0x13, 0x2b, 0x21, 0x07, 0xd0, 0x1e, 0x00, 0x14, 0x70, 0x28, 0x32, 0x3c, 0x46, 0x50, 0x5a,
		0x64, 0x6e, 0x78, 0x82, 0x8c, 0x96, 0xa0, 0xaa, 0xb4, 0xbe, 0xfc, 0xe8}

	var uvs []uint64
	var vs []int64

	uvs, err := irl.ReadValues(bytes.NewBuffer(bs), uvs)
	assert.Nil(t, err)
	for _, v := range uvs {
		vs= append(vs, UnZigzag(v))
	}
	assert.Equal(t, values, vs)

	/*buf := &bytes.Buffer{}
	uvs= uvs[:0]
	for _, v := range values {
		uvs= append(uvs, Zigzag(v))
	}
	if err := irl.WriteValues(buf, signed, uvs); err != nil {
		t.Fatalf("fail %+v", err)
	}
	assert.Equal(t, bs, buf.Bytes())

	rle.reset()
	rle.signed = true
	if err := rle.readValues(buf); err != nil {
		t.Fatalf("decode error %+v", err)
	}
	assert.Equal(t, v, rle.literals)*/

	/*values = []int64{-2030, -2000, -2020, 1000000, 2040, -2050, -2060, -2070, -2080, -2090, -2100, -2110, -2120, -2130,
		-2140, -2150, -2160, -2170, -2180, -2190}
	if err := rle.writeValues(buf); err != nil {
		t.Fatalf("encoding error %+v", err)
	}
	rle.reset()
	rle.signed = true
	if err := rle.readValues(buf); err != nil {
		t.Fatalf("decoding error %+v", err)
	}
	assert.Equal(t, v, rle.literals)*/
}

func TestIntRunLengthV2(t *testing.T) {
	irl := &IntRleV2{}
	//short repeat
	irl.Signed = false
	bs := []byte{0x0a, 0x27, 0x10}
	buf := bytes.NewBuffer(bs)

	var uvs []uint64
	uvs, err := irl.ReadValues(buf, uvs)
	assert.Nil(t, err)
	assert.Equal(t, 5, len(uvs))
	assert.Equal(t, 10000, int(uvs[0]))
	assert.Equal(t, 10000, int(uvs[4]))


	buf.Reset()
	err = irl.WriteValues(buf, uvs)
	assert.Nil(t, err)
	assert.Equal(t, bs, buf.Bytes())

	irl.Signed = true
	values := make([]int64, 10)
	for i := 0; i < 10; i++ {
		values[i] = -1
	}
	buf.Reset()
	uvs= uvs[:0]
	for _, v := range values {
		uvs= append(uvs, Zigzag(v))
	}
	err = irl.WriteValues(buf, uvs) //encoding
	assert.Nil(t, err)

	uvs= uvs[:0]
	uvs, err = irl.ReadValues(buf, uvs)
	assert.Nil(t, err)
	var vs []int64
	for _, v := range uvs {
		vs = append(vs, UnZigzag(v))
	}
	assert.Equal(t, values, vs)

	// direct
	uvalues := []uint64{23713, 43806, 57005, 48879}
	buf = bytes.NewBuffer([]byte{0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xef})
	uvs= uvs[:0]
	uvs, err = irl.ReadValues(buf,  uvs)
	assert.Nil(t, err)
	assert.Equal(t, uvalues, uvs)
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
	assert.Equal(t, uint64(0x0a), encodingNano(1000))
	assert.Equal(t, uint64(0x0c), encodingNano(100000))
}

func TestNanoEncoding(t *testing.T) {
	assert.Equal(t, uint64(0x0a), encodingNano(uint64(1000)))
	assert.Equal(t, uint64(0x0c), encodingNano(uint64(100000)))
}
