package orc

import (
	"bytes"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
)

func init() {
	logrus.SetLevel(logrus.TraceLevel)
}

func TestByteRunLength(t *testing.T) {
	buf := bytes.NewBuffer([]byte{0x61, 0x00})
	brl := &byteRunLength{}
	v:=[]byte{0x01,0x02,0x03,0x4,0x05,0x05,0x05,0x05, 0x06,0x07, 0x08,0x08,0x08, 0x09,0x10}

	if err := brl.readValues(buf); err != nil {
		t.Error(err)
	}
	if len(brl.literals) != 100 {
		t.Fatal("literal number should be 100")
	}
	if brl.literals[0] != 0 || brl.literals[99] != 0 {
		t.Fatal("literal value should 0x00")
	}

	t2 := bytes.NewBuffer([]byte{0xfe, 0x44, 0x45})
	brl = &byteRunLength{}
	if err := brl.readValues(t2); err != nil {
		t.Error(err)
	}
	if len(brl.literals) != 2 {
		t.Fatal("literal number error")
	}
	if brl.literals[0] != 0x44 || brl.literals[1] != 0x45 {
		t.Fatal("literal content error")
	}

	brl.reset()

	brl.literals= v
	buf.Reset()
	if err:=brl.writeValues(buf);err!=nil {
		t.Fatalf("fail %+v", err)
	}
	brl.reset()
	if err:=brl.readValues(buf);err!=nil {
		t.Fatalf("fail %+v", err)
	}
	assert.Equal(t, v, brl.literals)

	v= v[:0]
	for i:=0; i<=130; i++ { // run 131
		v= append(v, 0x01)
	}
	v= append(v, 0x02, 0x03)

	brl.literals= v
	buf.Reset()
	if err:=brl.writeValues(buf);err!=nil {
		t.Fatalf("fail %+v", err)
	}
	brl.reset()
	if err:=brl.readValues(buf);err!=nil {
		t.Fatalf("fail %+v", err)
	}
	assert.Equal(t, v, brl.literals)

}

func TestBoolRunLength(t *testing.T)  {
	vs:=[]bool{true, false, false, false, false, false, false, false}
	bs:= []byte{0xff, 0x80}

	brl:=&boolRunLength{}
	brl.bools= vs
	buf:= &bytes.Buffer{}
	buf.Reset()

	if err:=brl.writeValues(buf);err!=nil {
		t.Fatalf("fail %+v", err)
	}
	assert.Equal(t, bs, buf.Bytes())

	brl.reset()
	if err:= brl.readValues(buf); err!=nil {
		t.Fatalf("fail %+v", err)
	}
	assert.Equal(t, vs, brl.bools)
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

func TestIntRunLengthV2_Delta(t *testing.T)  {
	var err error
	rle := &intRleV2{}
	bw := bytes.NewBuffer(make([]byte, 100))
	bw.Reset()

	rle.signed=false
	r := []uint64{2, 3, 5, 7, 11, 13, 17, 19, 23, 29} // unsigned
	bs := []byte{0xc6, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46}
	rle.uliterals = r
	err = rle.writeValues(bw)
	assert.Nil(t, err)
	assert.Equal(t, bs, bw.Bytes())
	br := bytes.NewBuffer(bs)
	rle.reset()
	err = rle.readValues(br) // decoding
	assert.Nil(t, err)
	assert.Equal(t, 10, rle.len())
	assert.EqualValues(t, r, rle.uliterals[0:10])

	vs := []int64{-2, -3, -5, -7, -11, -13, -17, -19, -23, -29} // signed
	rle.reset()
	rle.signed = true
	rle.literals = vs
	bw.Reset()
	err = rle.writeValues(bw)
	assert.Nil(t, err)
	rle.reset()
	rle.signed = true
	err = rle.readValues(bw)
	assert.Nil(t, err)
	assert.Equal(t, Encoding_DELTA, rle.sub)
	assert.Equal(t, 10, rle.len())
	assert.Equal(t, rle.literals, vs)

	// fixed delta 0
	vs = []int64{-2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2}
	rle.reset()
	rle.signed = true
	rle.literals = vs
	bw.Reset()
	err = rle.writeValues(bw)
	assert.Nil(t, err)
	rle.reset()
	rle.signed = true
	err = rle.readValues(bw)
	assert.Nil(t, err)
	assert.Equal(t, Encoding_DELTA, rle.sub)
	assert.Equal(t, 11, rle.len())
	assert.Equal(t, rle.literals, vs)

	// over 512 numbers with uint
	data:= make([]uint64, 1000)
	for i:=0; i< 1000; i++ {
		data[i]= uint64(i)
	}
	rle.reset()
	rle.signed= false
	rle.uliterals= data
	bw.Reset()
	err= rle.writeValues(bw)
	assert.Nil(t, err)
	if err!=nil {
		fmt.Printf("%+v", err)
	}
	rle.reset()
	rle.signed= false
	err= rle.readValues(bw)
	assert.Nil(t, err)
	if err!=nil {
		fmt.Printf("%+v", err)
	}
	assert.Equal(t, 1000, rle.len())
	assert.Equal(t, uint64(0), rle.uliterals[0])
	assert.Equal(t, uint64(999), rle.uliterals[999])

	// number over 512 with int
	idata:= make([]int64, 1500)
	for i:=0; i<1500; i++ {
		idata[i]= int64(1000-i)
	}
	rle.reset()
	rle.signed= true
	rle.literals= idata
	bw.Reset()
	err= rle.writeValues(bw)
	assert.Nil(t, err)
	if err!=nil {
		fmt.Printf("%+v", err)
	}
	rle.reset()
	rle.signed= true
	err= rle.readValues(bw)
	assert.Nil(t, err)
	if err!=nil {
		fmt.Printf("%+v", err)
	}
	assert.Equal(t, 1500, rle.len())
	assert.Equal(t, int64(1000), rle.literals[0])
	assert.Equal(t, int64(-499), rle.literals[1499])
}

func TestIntRunLengthV2Direct(t *testing.T)  {
	v:= []uint64{23713, 57005, 43806, 48879}
	encoded:= []byte{0x5e, 0x03, 0x5c, 0xa1, 0xde, 0xad, 0xab, 0x1e, 0xbe, 0xef}
	rle:= &intRleV2{}
	rle.signed= false
	buf:= &bytes.Buffer{}
	buf.Reset()
	rle.uliterals= v
	if err:= rle.writeValues(buf);err!=nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, encoded, buf.Bytes())
	rle.reset()
	rle.signed= false
	if err:=rle.readValues(buf);err!=nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, v, rle.uliterals)
}


func TestIntRunLengthV2Patch(t *testing.T)  {
	rle := &intRleV2{}
	rle.signed= true
	buf :=&bytes.Buffer{}

	v:= []int64{2030, 2000, 2020, 1000000, 2040, 2050, 2060, 2070, 2080, 2090, 2100, 2110, 2120, 2130,
		2140, 2150, 2160, 2170, 2180, 2190}
	bs := []byte{0x8e, 0x13, 0x2b, 0x21, 0x07, 0xd0, 0x1e, 0x00, 0x14, 0x70, 0x28, 0x32, 0x3c, 0x46, 0x50, 0x5a,
		0x64, 0x6e, 0x78, 0x82, 0x8c, 0x96, 0xa0, 0xaa, 0xb4, 0xbe, 0xfc, 0xe8}
	err:= rle.readValues(bytes.NewBuffer(bs))
	if err!=nil {
		t.Fatalf("err %+v", err)
	}
	assert.Equal(t, v, rle.literals)

	rle.reset()
	rle.signed=true
	rle.literals=  v
	buf.Reset()
	if err:= rle.writeValues(buf);err!=nil {
		t.Fatalf("fail %+v", err)
	}
	assert.Equal(t, bs, buf.Bytes())
	rle.reset()
	rle.signed=true
	if err:= rle.readValues(buf);err!=nil {
		t.Fatalf("decode error %+v", err)
	}
	assert.Equal(t, v, rle.literals)

	v= []int64{-2030, -2000, -2020, 1000000, 2040, -2050, -2060, -2070, -2080, -2090, -2100, -2110, -2120, -2130,
		-2140, -2150, -2160, -2170, -2180, -2190}
	rle.reset()
	rle.signed= true
	rle.literals= v
	buf.Reset()
	if err:= rle.writeValues(buf);err!=nil {
		t.Fatalf("encoding error %+v", err)
	}
	rle.reset()
	rle.signed= true
	if err:=rle.readValues(buf);err!=nil {
		t.Fatalf("decoding error %+v", err)
	}
	assert.Equal(t, v, rle.literals)
}

func TestIntRunLengthV2(t *testing.T) {
	rle := &intRleV2{}
	//short repeat
	rle.signed = false
	bs := []byte{0x0a, 0x27, 0x10}
	b1 := bytes.NewBuffer(bs)
	err := rle.readValues(b1)
	assert.Nil(t, err)
	assert.Equal(t, Encoding_SHORT_REPEAT, rle.sub)
	assert.Equal(t, 5, rle.len())
	assert.Equal(t, 10000, int(rle.uliterals[0]))
	assert.Equal(t, 10000, int(rle.uliterals[4]))
	b1.Reset()
	err = rle.writeValues(b1)
	assert.Nil(t, err)
	assert.Equal(t, bs, b1.Bytes())

	rle.reset()
	rle.signed = true
	v := make([]int64, 10)
	for i := 0; i < 10; i++ {
		v[i] = -1
	}
	rle.literals = v
	b1.Reset()
	err = rle.writeValues(b1) //encoding
	assert.Nil(t, err)
	rle.reset()
	err = rle.readValues(b1) // decoding
	assert.Nil(t, err)
	assert.Equal(t, 10, rle.len())
	assert.Equal(t, int64(-1), rle.literals[0])
	assert.Equal(t, int64(-1), rle.literals[9])

	// direct
	rle.signed= false
	rle.reset()
	r := []uint64{23713, 43806, 57005, 48879}
	b2 := bytes.NewBuffer([]byte{0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xef})
	err = rle.readValues(b2)
	assert.Nil(t, err)
	assert.Equal(t, 4, rle.len())
	assert.EqualValues(t, r, rle.uliterals[0:4])
}

func TestZigzag(t *testing.T) {
	assert.Equal(t, uint64(1), zigzag(-1))
	assert.Equal(t, int64(-1), unZigzag(1))

	var x int64 = 2147483647
	assert.Equal(t, uint64(4294967294), zigzag(x))
	assert.Equal(t, x, unZigzag(zigzag(x)))

	var y int64 = -2147483648
	assert.Equal(t, uint64(4294967295), zigzag(y))
	assert.Equal(t, y, unZigzag(zigzag(y)))
}

func TestChunkHeader(t *testing.T) {
	l := 100000
	v := []byte{0x40, 0x0d, 0x03}

	h := encChunkHeader(l, false)
	assert.Equal(t, h, v)
	dl, o := decChunkHeader(v)
	assert.Equal(t, l, dl)
	assert.Equal(t, o, false)
}

func TestTimestampTrailing(t *testing.T)  {
	assert.Equal(t, uint64(0x0a), encodingNano(1000))
	assert.Equal(t, uint64(0x0c), encodingNano(100000))
}