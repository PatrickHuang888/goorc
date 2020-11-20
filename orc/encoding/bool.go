package encoding

import (
	"bytes"
	"io"
)

const MAX_BOOL_RL = 8

type boolRunLength struct {
	brl       *byteRunLength
	offset    int
	values    []bool
	positions []uint64
}

/*func (e *boolRunLength) BufferedSize() int {
	return e.brl.BufferedSize()
}*/

func NewBoolEncoder() *boolRunLength {
	return &boolRunLength{offset: -1, values: make([]bool, MAX_BOOL_RL), brl: NewByteEncoder()}
}

func (e *boolRunLength) MarkPosition() {
	e.positions = append(e.positions, uint64(e.offset+1))
}

func (e *boolRunLength) PopPositions() []uint64 {
	r := e.positions
	e.positions = nil
	return r
}

// Reset except positions
func (e *boolRunLength) Reset() {
	e.offset = -1
	e.brl.Reset()
}

func (e *boolRunLength) Encode(v interface{}, out *bytes.Buffer) error {
	value := v.(bool)

	e.offset++
	e.values[e.offset] = value

	if e.offset >= MAX_BOOL_RL-1 {
		var b byte
		for i := 0; i <= 7; i++ {
			if e.values[i] {
				b |= 0x01 << byte(7-i)
			}
		}
		if err := e.brl.Encode(b, out); err != nil {
			return err
		}
		e.offset = -1
		return nil
	}

	return nil
}

func (e *boolRunLength) Flush(out *bytes.Buffer) error {
	if e.offset != -1 {
		var b byte
		for i := 0; i <= e.offset; i++ {
			if e.values[i] {
				b |= 0x01 << byte(7-i)
			}
		}
		if err := e.brl.Encode(b, out); err != nil {
			return err
		}
	}

	if err := e.brl.Flush(out); err != nil {
		return err
	}

	e.Reset()
	return nil
}

func DecodeBools(in io.ByteReader, vs []bool) ([]bool, error) {
	var bs []byte
	var err error

	if bs, err = DecodeByteRL(in, bs); err != nil {
		return vs, err
	}

	for _, b := range bs {
		for i := 0; i < 8; i++ {
			v := (b>>byte(7-i))&0x01 == 0x01
			vs = append(vs, v)
		}
	}
	return vs, err
}
