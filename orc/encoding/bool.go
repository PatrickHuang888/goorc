package encoding

import (
	"bytes"
	"github.com/pkg/errors"
	"io"
)

const MAX_BOOL_RL = 8

/*
 stride will not less than MaxBoolRL
 */
type boolRunLength struct {
	brl       *byteRunLength
	values    []bool

	positions []uint64
	positionMarked bool
}

func NewBoolEncoder() *boolRunLength {
	return &boolRunLength{values: make([]bool, 0, MAX_BOOL_RL), brl: NewByteEncoder()}
}

func (e *boolRunLength) MarkPosition() {
	e.positions = append(e.positions, uint64(len(e.values)))
	// maybe byte runlength no values at this time
	//e.brl.MarkPosition()
	e.positionMarked= true
}

func (e *boolRunLength) PopPositions() [][]uint64 {
	r:= e.brl.PopPositions()
	if len(r)!=len(e.positions) {
		logger.Panicf("position error %d %d", len(r), len(e.positions))
	}
	for i:=0; i<len(r);i++ {
		r[i]= append(r[i], e.positions[i])
	}
	e.positions = e.positions[:0]
	return r
}

func (e *boolRunLength) Reset() {
	e.values= e.values[:0]
	e.positions= e.positions[:0]
	e.brl.Reset()
}

func (e *boolRunLength) Encode(v interface{}, out *bytes.Buffer) error {
	value := v.(bool)

	e.values= append(e.values, value)

	if len(e.values) >= MAX_BOOL_RL {
		var b byte
		for i := 0; i <= 7; i++ {
			if e.values[i] {
				b |= 0x01 << byte(7-i)
			}
		}
		if err := e.brl.Encode(b, out); err != nil {
			return err
		}
		if e.positionMarked {
			e.brl.MarkPosition()
			e.positionMarked= false
		}
		e.values= e.values[8:]
		return nil
	}
	return nil
}

func (e *boolRunLength) Flush(out *bytes.Buffer) error {
	if len(e.values) > 7 {
		return errors.New("flush values error")
	}
	if len(e.values)!=0 {
		var b byte
		for i := 0; i < len(e.values); i++ {
			if e.values[i] {
				b |= 0x01 << byte(7-i)
			}
		}
		if err := e.brl.Encode(b, out); err != nil {
			return err
		}
		if e.positionMarked {
			e.brl.MarkPosition()
			e.positionMarked= false
		}
		e.values= e.values[:0]
	}
	if err := e.brl.Flush(out); err != nil {
		return err
	}
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
