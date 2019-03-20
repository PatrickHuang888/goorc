package orc

import (
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

type bstream struct {
	value []byte
	pos   int
}

func (bs *bstream) ReadByte() (byte, error) {
	v := bs.value[bs.pos]
	bs.pos++
	return v, nil
}
func (bs *bstream) Read(p []byte) (n int, err error) {
	if len(p) != len(bs.value)-bs.pos {
		return 0, errors.New("read copy slice length error")
	}
	copy(p, bs.value[bs.pos:])
	bs.pos += len(p)
	return len(p), nil
}

func TestByteRunLength(t *testing.T) {
	t1 := &bstream{value: []byte{0x61, 0x00}}

	brl := &byteRunLength{
		literals: make([]byte, MAX_LITERAL_SIZE),
	}
	if err := brl.readValues(false, t1); err != nil {
		t.Error(err)
	}
	if brl.repeat == false {
		t.Fatal("repeat should be false")
	}
	if brl.numLiterals != 100 {
		t.Fatal("literal number should be 100")
	}
	if brl.literals[0] != 0 {
		t.Fatal("literal value should 0x00")
	}

	t2 := &bstream{value: []byte{0xfe, 0x44, 0x45}}
	brl = &byteRunLength{
		literals: make([]byte, MAX_LITERAL_SIZE),
	}
	if err := brl.readValues(false, t2); err != nil {
		t.Error(err)
	}
	if brl.repeat == true {
		t.Fatal("repeat error")
	}
	if brl.numLiterals != 2 {
		t.Fatal("literal number error")
	}
	if brl.literals[0] != 0x44 || brl.literals[1] != 0x45 {
		t.Fatal("literal content error")
	}
}

func TestIntRunLengthV1(t *testing.T) {
	t1 := &bstream{value: []byte{0x61, 0x00, 0x07}}
	irl := &intRunLengthV1{
		literals: make([]uint64, MAX_LITERAL_SIZE),
	}
	if err := irl.readValues(t1); err != nil {
		t.Error(err)
	}
	if irl.run != true {
		t.Fatal("run error")
	}
	if irl.numLiterals != 100 {
		t.Fatal("num literals error")
	}
	if irl.literals[0] != 7 {
		t.Fatal("literal error")
	}

	t2 := &bstream{value: []byte{0xfb, 0x02, 0x03, 0x04, 0x07, 0xb}}
	err := irl.readValues(t2)
	assert.Nil(t, err)
	assert.Equal(t, irl.run, false)
	assert.Equal(t, irl.numLiterals, 5)
	assert.Equal(t, irl.literals[4], uint64(11))
}

func TestIntRunLengthV2(t *testing.T)  {
	t1 := &bstream{value: []byte{0x0a, 0x27, 0x10}}
	irl := &intRleV2{
		uliterals: make([]uint64, MAX_LITERAL_SIZE),
	}
	err:= irl.readValues(t1)
	assert.Nil(t, err)
	assert.Equal(t, SHORT_REPEAT, irl.sub)
	assert.Equal(t, uint32(5), irl.numLiterals)
	assert.Equal(t, 10000, int(irl.uliterals[0]))
}

func TestZigzag(t *testing.T)  {
	assert.Equal(t, uint64(1), EncodeZigzag(-1))
	assert.Equal(t, int64(-1), DecodeZigzag(1))

	var x int64= 2147483647
	assert.Equal(t, uint64(4294967294), EncodeZigzag(x))
	assert.Equal(t, x, DecodeZigzag(EncodeZigzag(x)))

	var y int64= -2147483648
	assert.Equal(t, uint64(4294967295), EncodeZigzag(y))
	assert.Equal(t, y, DecodeZigzag(EncodeZigzag(y)))
}
