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
	PATCHED_BASE      = 2
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

// int run length encoding v2
type intRleV2 struct {
	sub         byte
	signed      bool
	literals    []int64 // fixme: allocate
	uliterals   []uint64
	numLiterals uint32
}

func (rle *intRleV2) readValues(in InputStream) error {
	// header from MSB to LSB
	b, err := in.ReadByte()
	if err != nil {
		errors.WithStack(err)
	}
	rle.sub = b >> 6
	switch rle.sub {
	case SHORT_REPEAT:
		header := b
		width := 1 + (header>>3)&0x07 // 3bit([3,)6) width
		repeatCount := 3 + (header & 0x07)
		rle.numLiterals = uint32(repeatCount)

		var x uint64
		for i := width; i > 0; { // big endian
			i--
			b, err := in.ReadByte()
			if err != nil {
				errors.WithStack(err)
			}
			x |= uint64(b) << (8 * i)
		}

		if rle.signed { // zigzag
			rle.literals[0] = DecodeZigzag(x)
		} else {
			rle.uliterals[0] = x
		}
	case DIRECT:
		b1, err := in.ReadByte()
		if err != nil {
			errors.WithStack(err)
		}
		header := uint16(b)<<8 | uint16(b1) // 2 byte header
		w := (header >> 9) & 0x1F           // 5bit([3,8)) width
		width, err := getWidth(byte(w))
		if err != nil {
			return errors.WithStack(err)
		}
		length := header&0x1FF + 1
		rle.numLiterals = uint32(length)
		for i := uint16(0); i < length; i++ {
			var x uint64
			switch width {
			case 0:
				// todo:
				fallthrough
			case 1:
				// todo:
				return errors.Errorf("direct width %d not impl", width)
			case 2:
				fallthrough
			case 4:
				fallthrough
			case 8:
				b, err := in.ReadByte()
				if err != nil {
					return errors.WithStack(err)
				}
				x = uint64(b)
			case 16:
				b := make([]byte, 2)
				if _, err = io.ReadFull(in, b); err != nil {
					return errors.WithStack(err)
				}
				x = uint64(b[1]) | uint64(b[0])<<8
			case 24:
				b := make([]byte, 3)
				if _, err = io.ReadFull(in, b); err != nil {
					return errors.WithStack(err)
				}
				x = uint64(b[2]) | uint64(b[1])<<8 | uint64(b[0])<<16
			case 32:
				b := make([]byte, 4)
				if _, err = io.ReadFull(in, b); err != nil {
					return errors.WithStack(err)
				}
				x = uint64(b[3]) | uint64(b[2])<<8 | uint64(b[1])<<16 | uint64(b[0])<<24
			case 40:
				b := make([]byte, 5)
				if _, err = io.ReadFull(in, b); err != nil {
					return errors.WithStack(err)
				}
				x = uint64(b[4]) | uint64(b[3])<<8 | uint64(b[2])<<16 | uint64(b[1])<<24 | uint64(b[0])<<32
			case 48:
				b := make([]byte, 6)
				if _, err = io.ReadFull(in, b); err != nil {
					return errors.WithStack(err)
				}
				x = uint64(b[5]) | uint64(b[4])<<8 | uint64(b[4])<<16 | uint64(b[2])<<24 | uint64(b[1])<<32 |
					uint64(b[0])<<40
			case 56:
				b := make([]byte, 7)
				if _, err = io.ReadFull(in, b); err != nil {
					return errors.WithStack(err)
				}
				x = uint64(b[6]) | uint64(b[5])<<8 | uint64(b[4])<<16 | uint64(b[3])<<24 | uint64(b[2])<<32 |
					uint64(b[1])<<40 | uint64(b[0])<<48
			case 65:
				b := make([]byte, 8)
				if _, err = io.ReadFull(in, b); err != nil {
					return errors.WithStack(err)
				}
				x = uint64(b[7]) | uint64(b[1])<<8 | uint64(b[6])<<16 | uint64(b[5])<<24 | uint64(b[4])<<32 |
					uint64(b[3])<<40 | uint64(b[2])<<48 | uint64(b[1])<<56
			default:
				return errors.Errorf("direct width %d not deprecated", width)
			}

			if rle.signed {
				rle.literals[i] = DecodeZigzag(x)
			} else {
				rle.uliterals[i] = x
			}
		}

	case PATCHED_BASE:
		header:= make([]byte, 4)  // 4 byte header
		_, err= io.ReadFull(in, header[1:4])
		if err!=nil {
			return errors.WithStack(err)
		}
		header[0]= b
		w := byte(header >> 25 & 0x1F)           // 5bit([3,8)) width
		width, err := getWidth(w)
		if err != nil {
			return errors.WithStack(err)
		}
		length := header&0x1FF + 1

	default:
		return errors.Errorf("sub encoding %d for int rle v2 not recognized", rle.sub)
	}
	return nil
}

func getWidth(w byte) (width byte, err error) {
	switch w {
	case 0:
		width = 0 // delta
		// todo:
		// width= 1 // non-delta
	case 1:
		width = 2
	case 3:
		width = 4
	case 7:
		width = 8
	case 15:
		width = 16
	case 23:
		width = 24
	case 27:
		width = 32
	case 28:
		width = 40
	case 29:
		width = 48
	case 30:
		width = 56
	case 31:
		width = 64
	default:
		return 0, errors.Errorf("run length integer v2, direct width(W) %d deprecated", width)
	}
	return
}

func DecodeZigzag(x uint64) int64 {
	return int64(x>>1) ^ -int64(x & 1)
}

func EncodeZigzag(x int64) uint64 {
	return uint64(x<<1) ^ uint64(x>>63)
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
