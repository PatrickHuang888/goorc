package encoding

import (
	"bytes"
	"io"
)

const MaxBoolRunLength = 8

type boolRunLength struct {
	brl      *byteRunLength
	index    int
	value    byte
	position int
}

func NewBoolEncoder(resetPosition bool) *boolRunLength {
	if resetPosition {
		return &boolRunLength{brl: NewByteEncoder(true), position: 0, index: -1}
	}
	return &boolRunLength{brl: NewByteEncoder(false), position: -1, index: -1}
}

func (e *boolRunLength) ResetPosition() {
	e.position = 0
	e.brl.ResetPosition()
}

func (e *boolRunLength) GetPosition() []uint64 {
	var r []uint64

	// refactor: operate on byte run length directly
	r = append(r, uint64(e.brl.position))
	r = append(r, uint64(e.position))

	e.brl.ResetPosition()
	e.ResetPosition()
	return r
}

func (e *boolRunLength) Reset() {
	e.value = 0
	e.index = -1
	e.brl.Reset()
}

func (e *boolRunLength) Encode(v interface{}, out *bytes.Buffer) error {
	value := v.(bool)
	e.index++

	if e.position != -1 {
		e.position++
	}

	if value {
		e.value |= 1 << byte(7-e.index)
	}

	if e.index >= MaxBoolRunLength-1 { // finish a byte
		if err := e.brl.Encode(e.value, out); err != nil {
			return err
		}
		e.index = -1
		e.value = 0

		if e.position != -1 {
			e.position = 0
		}
	}
	return nil
}

func (e *boolRunLength) Flush(out *bytes.Buffer) error {
	if e.index != -1 {
		if err := e.brl.Encode(e.value, out); err != nil {
			return err
		}
	}
	if err := e.brl.Flush(out); err != nil {
		return err
	}
	// rethink:
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

func DecodeBoolsFromByte(b byte) []bool {
	var vs []bool
	for i := 0; i < 8; i++ {
		v := (b>>byte(7-i))&0x01 == 0x01
		vs = append(vs, v)
	}
	return vs
}
