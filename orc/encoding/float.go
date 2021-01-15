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

func (f *float) GetPosition() []uint64 {
	// rethink:
	return []uint64{0}
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
	//v := math.Float64frombits(binary.BigEndian.Uint64(bb))
	// Java writer write with little endian
	v := math.Float64frombits(binary.LittleEndian.Uint64(bb))
	return v, nil
}

func NewDoubleEncoder() Encoder {
	return &double{}
}

type double struct {
}

func (d double) Encode(v interface{}, out *bytes.Buffer) error {
	value, ok := v.(float64)
	if !ok {
		return errors.New("double encoding value should be type float64")
	}
	bb := make([]byte, 8)
	binary.LittleEndian.PutUint64(bb, math.Float64bits(value))
	//binary.BigEndian.PutUint64(bb, math.Float64bits(value))
	if _, err := out.Write(bb); err != nil {
		return err
	}
	return nil
}

func (d double) Flush(out *bytes.Buffer) error {
	//
	return nil
}

func (d *double) GetPosition() []uint64 {
	return []uint64{0}
}

func (d *double) Reset() {
	//
}
