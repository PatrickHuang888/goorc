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
	t1 := bytes.NewBuffer([]byte{0x61, 0x00})

	brl := &byteRunLength{}
	if err := brl.readValues(t1); err != nil {
		t.Error(err)
	}
	if brl.numLiterals != 100 {
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
	if brl.numLiterals != 2 {
		t.Fatal("literal number error")
	}
	if brl.literals[0] != 0x44 || brl.literals[1] != 0x45 {
		t.Fatal("literal content error")
	}
}

func TestIntRunLengthV1(t *testing.T) {
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
	assert.Nil(t, err)
	assert.Equal(t, irl.numLiterals, 5)
	if irl.uliterals[0] != 2 {
		t.Fatal("uliteral error")
	}
	if irl.uliterals[4] != 11 {
		t.Fatal("uliteral error")
	}
}

func TestIntRunLengthV2_Delta(t *testing.T)  {
	var err error
	irl := &intRleV2{}
	bw := bytes.NewBuffer(make([]byte, 100))
	bw.Reset()

	irl.signed=false
	r := []uint64{2, 3, 5, 7, 11, 13, 17, 19, 23, 29} // unsigned
	bs := []byte{0xc6, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46}
	irl.uliterals = r
	irl.numLiterals = 10
	err = irl.writeValues(bw)
	assert.Nil(t, err)
	assert.Equal(t, bs, bw.Bytes())
	br := bytes.NewBuffer(bs)
	irl.reset()
	err = irl.readValues(br) // decoding
	assert.Nil(t, err)
	assert.Equal(t, 10, irl.numLiterals)
	assert.EqualValues(t, r, irl.uliterals[0:10])

	vs := []int64{-2, -3, -5, -7, -11, -13, -17, -19, -23, -29} // signed
	irl.reset()
	irl.signed = true
	irl.numLiterals = 10
	irl.literals = vs
	bw.Reset()
	err = irl.writeValues(bw)
	assert.Nil(t, err)
	irl.reset()
	irl.signed = true
	err = irl.readValues(bw)
	assert.Nil(t, err)
	assert.Equal(t, Encoding_DELTA, irl.sub)
	assert.Equal(t, 10, irl.numLiterals)
	assert.Equal(t, irl.literals, vs)

	// fixed delta 0
	vs = []int64{-2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2}
	irl.reset()
	irl.signed = true
	irl.literals = vs
	irl.numLiterals = 11
	bw.Reset()
	err = irl.writeValues(bw)
	assert.Nil(t, err)
	irl.reset()
	irl.signed = true
	err = irl.readValues(bw)
	assert.Nil(t, err)
	assert.Equal(t, Encoding_DELTA, irl.sub)
	assert.Equal(t, 11, irl.numLiterals)
	assert.Equal(t, irl.literals, vs)

	// over 512 numbers with uint
	data:= make([]uint64, 1000)
	for i:=0; i< 1000; i++ {
		data[i]= uint64(i)
	}
	irl.reset()
	irl.signed= false
	irl.numLiterals= 1000
	irl.uliterals= data
	bw.Reset()
	err= irl.writeValues(bw)
	assert.Nil(t, err)
	if err!=nil {
		fmt.Printf("%+v", err)
	}
	irl.reset()
	irl.signed= false
	err= irl.readValues(bw)
	assert.Nil(t, err)
	if err!=nil {
		fmt.Printf("%+v", err)
	}
	assert.Equal(t, 1000, irl.numLiterals)
	assert.Equal(t, uint64(0), irl.uliterals[0])
	assert.Equal(t, uint64(999), irl.uliterals[999])

	// number over 512 with int
	idata:= make([]int64, 1500)
	for i:=0; i<1500; i++ {
		idata[i]= int64(1000-i)
	}
	irl.reset()
	irl.signed= true
	irl.numLiterals= 1500
	irl.literals= idata
	bw.Reset()
	err= irl.writeValues(bw)
	assert.Nil(t, err)
	if err!=nil {
		fmt.Printf("%+v", err)
	}
	irl.reset()
	irl.signed= true
	err= irl.readValues(bw)
	assert.Nil(t, err)
	if err!=nil {
		fmt.Printf("%+v", err)
	}
	assert.Equal(t, 1500, irl.numLiterals)
	assert.Equal(t, int64(1000), irl.literals[0])
	assert.Equal(t, int64(-499), irl.literals[1499])
}

func TestIntRunLengthV2(t *testing.T) {
	irl := &intRleV2{}
	//short repeat
	bs := []byte{0x0a, 0x27, 0x10}
	b1 := bytes.NewBuffer(bs)
	irl.signed = false
	err := irl.readValues(b1)
	assert.Nil(t, err)
	assert.Equal(t, Encoding_SHORT_REPEAT, irl.sub)
	assert.Equal(t, 5, irl.numLiterals)
	assert.Equal(t, 10000, int(irl.uliterals[0]))
	assert.Equal(t, 10000, int(irl.uliterals[4]))
	b1.Reset()
	err = irl.writeValues(b1)
	assert.Nil(t, err)
	assert.Equal(t, bs, b1.Bytes())

	irl.reset()
	irl.signed = true
	irl.numLiterals = 10
	v := make([]int64, 10)
	for i := 0; i < 10; i++ {
		v[i] = -1
	}
	irl.literals = v
	b1.Reset()
	err = irl.writeValues(b1) //encoding
	assert.Nil(t, err)
	irl.reset()
	irl.signed = true
	err = irl.readValues(b1) // decoding
	assert.Nil(t, err)
	assert.Equal(t, 10, int(irl.numLiterals))
	assert.Equal(t, int64(-1), irl.literals[0])
	assert.Equal(t, int64(-1), irl.literals[9])

	// direct
	irl.reset()
	r := []uint64{23713, 43806, 57005, 48879}
	b2 := bytes.NewBuffer([]byte{0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xef})
	err = irl.readValues(b2)
	assert.Nil(t, err)
	assert.Equal(t, 4, irl.numLiterals)
	assert.EqualValues(t, r, irl.uliterals[0:4])


}

func TestZigzag(t *testing.T) {
	assert.Equal(t, uint64(1), EncodeZigzag(-1))
	assert.Equal(t, int64(-1), DecodeZigzag(1))

	var x int64 = 2147483647
	assert.Equal(t, uint64(4294967294), EncodeZigzag(x))
	assert.Equal(t, x, DecodeZigzag(EncodeZigzag(x)))

	var y int64 = -2147483648
	assert.Equal(t, uint64(4294967295), EncodeZigzag(y))
	assert.Equal(t, y, DecodeZigzag(EncodeZigzag(y)))
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
