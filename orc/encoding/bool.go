package encoding

import "io"

const MAX_BOOL_RL = 8

type BoolRunLength struct {
	brl    *ByteRunLength
	offset int
	values []bool
	positions []uint64
}

func NewBoolEncoder() *BoolRunLength {
	return &BoolRunLength{offset: -1, values: make([]bool, MAX_BOOL_RL), brl: NewByteEncoder()}
}

func (e *BoolRunLength) MarkPosition() {
	e.positions= append(e.positions, uint64(e.offset+1))
}

func (e *BoolRunLength) GetAndClearPositions() []uint64 {
	r:= e.positions
	e.positions= e.positions[:0]
	return r
}

// reset except positions
func (e *BoolRunLength) Reset() {
	e.offset= -1
	e.brl.Reset()
	e.values= e.values[:0]
}

func (e *BoolRunLength) Encode(v interface{}) (data []byte, err error) {
	value := v.(bool)

	e.offset++
	e.values[e.offset] = value

	if e.offset >= MAX_BOOL_RL-1 {
		var b byte
		for i := 0; i <= 7; i++ {
			if e.values[i] {
				b |= 0x01 << byte(7-i)
			}
			i++
		}
		if data, err = e.brl.Encode(b); err != nil {
			return
		}
		e.offset = -1
		return
	}

	return
}

func (e *BoolRunLength) Flush() (data []byte, err error) {
	if e.offset != -1 {
		var b byte
		for i := 0; i <= e.offset; i++ {
			if e.values[i] {
				b |= 0x01 << byte(7-i)
			}
		}
		if data, err = e.brl.Encode(b); err != nil {
			return
		}
		e.offset = -1
	}

	var dd []byte
	if dd, err = e.brl.Flush(); err != nil {
		return
	}
	if data == nil {
		data = dd
	} else {
		data = append(data, dd...)
	}
	return
}

func DecodeBools(in io.ByteReader, vs []bool) ([]bool, error) {
	var bs []byte
	var err error

	if bs, err = DecodeBytes(in, bs); err != nil {
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



