package encoding

import "io"

const MAX_BOOL_RL = 8

type boolRunLength struct {
	brl       *byteRunLength
	offset    int
	values    []bool
	positions []uint64
}

func (e *boolRunLength) BufferedSize() int {
	return e.brl.BufferedSize()
}

func NewBoolEncoder() *boolRunLength {
	return &boolRunLength{offset: -1, values: make([]bool, MAX_BOOL_RL), brl: NewByteEncoder()}
}

func (e *boolRunLength) MarkPosition() {
	e.positions = append(e.positions, uint64(e.offset+1))
}

func (e *boolRunLength) GetAndClearPositions() []uint64 {
	r := e.positions
	e.positions = e.positions[:0]
	return r
}

// Reset except positions
func (e *boolRunLength) Reset() {
	e.offset = -1
	e.brl.Reset()
	//e.values= e.values[:0]
}

func (e *boolRunLength) Encode(v interface{}) (err error) {
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
		if err = e.brl.Encode(b); err != nil {
			return
		}
		e.offset = -1
		return
	}

	return
}

func (e *boolRunLength) Flush() (data []byte, err error) {
	if e.offset != -1 {
		var b byte
		for i := 0; i <= e.offset; i++ {
			if e.values[i] {
				b |= 0x01 << byte(7-i)
			}
		}
		if err = e.brl.Encode(b); err != nil {
			return
		}
		e.offset = -1
	}

	if data, err = e.brl.Flush(); err != nil {
		return
	}
	e.Reset()
	return
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
