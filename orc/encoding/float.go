package encoding

import (
	"bytes"
	"encoding/binary"
	"github.com/pkg/errors"
	"io"
	"math"
)

/*
	IEEE754 does not specify endianness
*/
func NewFloatEncoder() Encoder {
	return &float{}
}

type float struct {
	positions []uint64
}

func (f float) Encode(v interface{}, out *bytes.Buffer) error {
	value, ok := v.(float32)
	if !ok {
		return errors.New("float encoding value should be type float32")
	}
	bb := make([]byte, 4)
	binary.BigEndian.PutUint32(bb, math.Float32bits(value))
	if _, err := out.Write(bb); err != nil {
		return err
	}
	return nil
}

func (f float) Flush(out *bytes.Buffer) error {
	//
	return nil
}

func (f *float) MarkPosition() {
	f.positions = append(f.positions, 1)
}

func (f *float) PopPositions() [][]uint64 {
	// todo:
	/*r := f.positions
	f.positions = nil
	return r*/
	return nil
}

func (f *float) Reset() {
	f.positions = f.positions[:0]
}

func DecodeFloat(in io.Reader) (float32, error) {
	bb := make([]byte, 4)
	if _, err := io.ReadFull(in, bb); err != nil {
		return 0, errors.WithStack(err)
	}
	v := math.Float32frombits(binary.BigEndian.Uint32(bb))
	return v, nil
}

func DecodeDouble(in io.Reader) (float64, error) {
	bb := make([]byte, 8)
	if _, err := io.ReadFull(in, bb); err != nil {
		return 0, errors.WithStack(err)
	}
	// !!
	v := math.Float64frombits(binary.BigEndian.Uint64(bb))
	//v := math.Float64frombits(binary.LittleEndian.Uint64(bb))
	return v, nil
}

func NewDoubleEncoder() Encoder {
	return &double{}
}

type double struct {
	positions []uint64
}

func (d double) Encode(v interface{}, out *bytes.Buffer) error {
	value, ok := v.(float64)
	if !ok {
		return errors.New("double encoding value should be type float64")
	}
	bb := make([]byte, 8)
	//binary.LittleEndian.PutUint64(bb, math.Float64bits(value))
	binary.BigEndian.PutUint64(bb, math.Float64bits(value))
	if _, err := out.Write(bb); err != nil {
		return err
	}
	return nil
}

func (d double) Flush(out *bytes.Buffer) error {
	//
	return nil
}

func (d *double) MarkPosition() {
	d.positions = append(d.positions, 1)
}

func (d *double) PopPositions() [][]uint64 {
	/*r := d.positions
	d.positions = nil
	return r*/
	// todo:
	return nil
}

func (d *double) Reset() {
	d.positions = d.positions[:0]
}
