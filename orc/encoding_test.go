package orc

import (
	"github.com/pkg/errors"
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
