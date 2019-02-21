package orc

import (
	"github.com/pkg/errors"
	"io"
)

const (
	MIN_REPEAT_SIZE  = 3
	MAX_LITERAL_SIZE = 128
)

type RunLengthEncoding interface {
	Read(reader io.Reader) (next byte, err error)
}

type InputStream interface {
	io.Reader
	io.ByteReader
}

type OutputStream interface {
	io.ByteWriter
	io.WriteCloser
}

type byteRunLength struct {
	repeat      bool
	literals    []byte
	numLiterals int
}

func (brl *byteRunLength) readValues(ignoreEof bool, in InputStream) (err error) {
	control, err := in.ReadByte()
	if err != nil {
		if err == io.EOF {
			if !ignoreEof {
				return errors.Errorf("read past end RLE byte")
			}
		}
		return errors.WithStack(err)
	}
	if control < 0x80 { // control
		brl.repeat = true
		brl.numLiterals = int(control) + MIN_REPEAT_SIZE
		val, err := in.ReadByte()
		if err != nil {
			if err == io.EOF {
				return errors.New("reading RLE byte go EOF")
			}
			return errors.WithStack(err)
		}
		brl.literals[0] = val
	} else { // literals
		brl.repeat = false
		brl.numLiterals = 0x100 - int(control)
		if _, err = io.ReadFull(in, brl.literals[:brl.numLiterals]); err != nil {
			return errors.WithStack(err)
		}
	}
	return
}

type intRunLengthV1 struct {
	numLiterals int
	repeat      bool
	delta       int8
	literals    []uint64
	sLiterals   []int64
	signed      bool
}

func (irl *intRunLengthV1) readValues(in InputStream) (err error) {
	control, err := in.ReadByte()
	if err != nil {
		return errors.WithStack(err)
	}
	if control < 0x80 { // run
		irl.numLiterals = int(control) + MIN_REPEAT_SIZE
		irl.repeat = true
		b, err := in.ReadByte()
		if err != nil {
			return errors.WithStack(err)
		}
		irl.delta = int8(b)
		irl.literals[0], err = ReadVUint(in)
		if err != nil {
			return errors.WithStack(err)
		}
	} else {
		n := int(int8(control))
		irl.numLiterals = -n
	}
	return
}

func (irl *intRunLengthV1) writeValues(out OutputStream) error {
	if irl.numLiterals != 0 {

	}
}

// base 128 varuint
func ReadVUint(in io.ByteReader) (r uint64, err error) {
	var b byte
	var shift uint
	for {
		b, err = in.ReadByte()
		if err != nil {
			errors.WithStack(err)
		}
		r |= uint64(0x7f&b) << shift
		shift += 7
		if b < 0x80 {
			break
		}
	}
	return
}

func Convert(u uint64) int64 {
	x := int64(u >> 1)
	if u&1 != 0 {
		x = ^x
	}
	return x
}
