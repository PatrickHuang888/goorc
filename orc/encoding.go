package orc

import (
	"github.com/pkg/errors"
	"io"
)

const (
	MIN_REPEAT_SIZE  = 3
	MAX_LITERAL_SIZE = 128

	SHORT_REPEAT byte = 0
	DIRECT            = 1
	PatchedBase
	Delta
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
	run         bool
	delta       int8
	literals    []uint64
	sLiterals   []int64
	signed      bool
}

func (irl *intRunLengthV1) readValues(in InputStream) error {
	control, err := in.ReadByte()
	if err != nil {
		return errors.WithStack(err)
	}
	if control < 0x80 { // run
		irl.numLiterals = int(control) + MIN_REPEAT_SIZE
		irl.run = true
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
		irl.run = false
		n := -int(int8(control))
		irl.numLiterals = n
		for i := 0; i < n; i++ {
			irl.literals[i], err = ReadVUint(in)
			if err != nil {
				return errors.WithStack(err)
			}
		}
	}
	return nil
}

func (irl *intRunLengthV1) writeValues(out OutputStream) error {
	if irl.numLiterals != 0 {

	}
	return nil
}

type intRunLengthV2 struct {
	sub         byte
	signed      bool
	literals    []int64
	uliterals   []uint64
	numLiterals int
}

func (irl *intRunLengthV2) readValues(in InputStream) error {
	header, err := in.ReadByte()
	if err != nil {
		errors.WithStack(err)
	}
	irl.sub = header >> 6
	switch irl.sub {
	case SHORT_REPEAT:
		width := 1 + (header>>3)&0x07 // W bytes
		repeatCount := 3 + (header & 0x07)
		irl.numLiterals = int(repeatCount)

		var x uint64
		for i := width; i > 0; {
			i--
			b, err := in.ReadByte()
			if err != nil {
				errors.WithStack(err)
			}
			x |= uint64(b) << (8 * i)
		}

		if irl.signed { // zigzag
			irl.literals[0] = DecodeZigzag(x)
		} else {
			irl.uliterals[0] = x
		}
	case DIRECT:
		/*width := (header >> 3) & 0x07
		var encodingWidth int
		switch width {
		case 0:
			encodingWidth = 0
		case 1:
			encodingWidth = 0
		case 2:
			encodingWidth = 1
		case 4:
			encodingWidth = 3
		case 8:
			encodingWidth = 7
		case 16:
			encodingWidth = 15
		case 24:
			encodingWidth = 23
		case 32:
			encodingWidth = 27
		case 40:
			encodingWidth = 28
		case 48:
			encodingWidth = 29
		case 56:
			encodingWidth = 30
		case 64:
			encodingWidth = 31
		default:
			return errors.Errorf("run length integer v2, direct width(W) %d deprecated", width)
		}*/
		/*header1, err := in.ReadByte()
		if err != nil {
			return errors.WithStack(err)
		}
		length :=*/
	}

	return nil
}

func DecodeZigzag(x uint64) int64 {
	return int64(x >> 1) ^ -int64(x&1)
}

func EncodeZigzag(x int64) uint64 {
	return uint64(x<<1) ^ uint64(x >> 63)
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
