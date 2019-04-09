package orc

import (
	"bytes"
	"encoding/binary"
	"github.com/pkg/errors"
	"io"
)

const (
	MIN_REPEAT_SIZE  = 3
	MAX_LITERAL_SIZE = 128

	Encoding_INT_RLEV2 = iota

	Encoding_SHORT_REPEAT byte = 0
	Encoding_DIRECT            = 1
	Encoding_PATCHED_BASE      = 2
	Encoding_DELTA             = 3
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
	sub         byte // sub encoding
	signed      bool
	literals    []int64 // fixme: allocate
	uliterals   []uint64
	numLiterals uint32
}

func (rle *intRleV2) reset()  {
	rle.sub=0

}

func (rle *intRleV2) readValues(in *bytes.Buffer) error {
	// header from MSB to LSB
	firstByte, err := in.ReadByte()
	if err != nil {
		return errors.WithStack(err)
	}
	rle.sub = firstByte >> 6
	switch rle.sub {
	case Encoding_SHORT_REPEAT:
		header := firstByte
		width := 1 + (header>>3)&0x07 // 3bit([3,)6) width
		repeatCount := 3 + (header & 0x07)
		rle.numLiterals = uint32(repeatCount)

		var x uint64
		for i := width; i > 0; { // big endian
			i--
			b, err := in.ReadByte()
			if err != nil {
				return errors.WithStack(err)
			}
			x |= uint64(b) << (8 * i)
		}

		if rle.signed { // zigzag
			rle.literals = []int64{DecodeZigzag(x)}
		} else {
			rle.uliterals = []uint64{x}
		}

	case Encoding_DIRECT:
		b1, err := in.ReadByte()
		if err != nil {
			return errors.WithStack(err)
		}
		header := uint16(firstByte)<<8 | uint16(b1) // 2 byte header
		w := (header >> 9) & 0x1F                   // 5bit([3,8)) width
		width, err := getWidth(byte(w), false)
		if err != nil {
			return errors.WithStack(err)
		}
		length := header&0x1FF + 1
		rle.numLiterals = uint32(length)
		/*if rle.signed {
			rle.literals = make([]int64, length)
		} else {
			rle.uliterals = make([]uint64, length)
		}*/

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

	case Encoding_PATCHED_BASE:
		header := make([]byte, 4) // 4 byte header
		_, err = io.ReadFull(in, header[1:4])
		if err != nil {
			return errors.WithStack(err)
		}
		header[0] = firstByte
		// 5 bit width
		/*width, err := getWidth(header[0]>>1&0x1f, false)
		if err != nil {
			return errors.WithStack(err)
		}
		length := uint32(header[0])&0x01<<8 | uint32(header[1]) + 1
		// 3 bit base value width
		bw := uint16(header[2])>>5&0x07 + 1
		// 5 bit patch width
		pw, err := getWidth(header[2] & 0x1f, false)
		if err != nil {
			return errors.WithStack(err)
		}
		// 3 bit patch gap width
		pgw := uint16(header[3])>>5&0x07 + 1
		// 5 bit patch list length
		pll := header[3] & 0x1f*/
		// todo:
		return errors.New("int rle patched base not impl")

	case Encoding_DELTA:
		// first two number cannot be identical
		// header: 2 bytes
		// base value: varint
		// delta base: signed varint
		header := make([]byte, 2)
		header[0] = firstByte
		header[1], err = in.ReadByte()
		if err != nil {
			return errors.WithStack(err)
		}
		deltaWidth, err := getWidth(header[0]>>1&0x1f, true)
		if err != nil {
			return errors.WithStack(err)
		}
		length := uint32(header[0])&0x01<<8 | uint32(header[1]) + 1
		rle.numLiterals = length
		if rle.signed {
			rle.literals = make([]int64, length)
		} else {
			rle.uliterals = make([]uint64, length)
		}

		if rle.signed {
			baseValue, err := binary.ReadVarint(in)
			if err != nil {
				return errors.WithStack(err)
			}
			rle.literals[0] = baseValue
		} else {
			baseValue, err := binary.ReadUvarint(in)
			if err != nil {
				return errors.WithStack(err)
			}
			rle.uliterals[0] = baseValue
		}
		deltaBase, err := binary.ReadVarint(in)
		if err != nil {
			return errors.WithStack(err)
		}

		if deltaBase > 0 {
			rle.setValue(1, true, uint64(deltaBase))
		} else {
			rle.setValue(1, false, uint64(-deltaBase))
		}

		// delta values: W * (L-2)
		i := uint32(2)
		for i < length {
			switch deltaWidth {
			case 0:
				// fix delta based on delta base
				if deltaBase > 0 {
					rle.setValue(i, true, uint64(deltaBase))
				} else {
					rle.setValue(i, false, uint64(-deltaBase))
				}
				i++

			case 2:
				b, err := in.ReadByte()
				if err != nil {
					return errors.WithStack(err)
				}
				for j := uint32(0); j < 4; j++ {
					delta := uint64(b) >> (3 - j) * 2 & 0x03
					if i+j <= length {
						rle.setValue(i, deltaBase > 0, delta)
					}
				}
				i = 4 * (i + 1)

			case 4:
				b, err := in.ReadByte()
				if err != nil {
					return errors.WithStack(err)
				}
				delta := uint64(b) >> 4
				rle.setValue(i, deltaBase > 0, delta)
				i++
				if i < length {
					delta = uint64(b) & 0x0f
					rle.setValue(i, deltaBase > 0, delta)
					i++
				}

			case 8:
				delta, err := in.ReadByte()
				if err != nil {
					return errors.WithStack(err)
				}
				if rle.signed {
					if deltaBase > 0 {
						rle.literals[i] = rle.literals[i-1] + int64(delta)
					} else {
						rle.literals[i] = rle.literals[i-1] - int64(delta)
					}
				} else {
					if deltaBase > 0 {
						rle.uliterals[i] = rle.uliterals[i-1] + uint64(delta)
					} else {
						rle.uliterals[i] = rle.uliterals[i-1] - uint64(delta)
					}
				}
				i++

			case 16:
				b, err := in.ReadByte()
				if err != nil {
					return errors.WithStack(err)
				}
				b1, err := in.ReadByte()
				if err != nil {
					return errors.WithStack(err)
				}
				delta := uint64(b)<<8 | uint64(b1)
				rle.setValue(i, deltaBase > 0, delta)
				i++

			case 24:
				bs := make([]byte, 3)
				if _, err = io.ReadFull(in, bs); err != nil {
					return errors.WithStack(err)
				}
				delta := uint64(bs[0])<<16 | uint64(bs[1])<<8 | uint64(bs[2])
				rle.setValue(i, deltaBase > 0, delta)
				i++

			case 32:
				bs := make([]byte, 4)
				if _, err = io.ReadFull(in, bs); err != nil {
					return errors.WithStack(err)
				}
				delta := uint64(bs[0])<<24 | uint64(bs[1])<<16 | uint64(bs[2])<<8 | uint64(bs[3])
				rle.setValue(i, deltaBase > 0, delta)
				i++

			case 40:
				bs := make([]byte, 5)
				if _, err = io.ReadFull(in, bs); err != nil {
					return errors.WithStack(err)
				}
				delta := uint64(bs[0])<<32 | uint64(bs[1])<<24 | uint64(bs[2])<<16 | uint64(bs[3])<<8 |
					uint64(bs[4])
				rle.setValue(i, deltaBase > 0, delta)
				i++

			case 48:
				bs := make([]byte, 6)
				if _, err = io.ReadFull(in, bs); err != nil {
					return errors.WithStack(err)
				}
				delta := uint64(bs[0])<<40 | uint64(bs[1])<<32 | uint64(bs[2])<<24 | uint64(bs[3])<<16 |
					uint64(bs[4])<<8 | uint64(bs[5])
				rle.setValue(i, deltaBase > 0, delta)
				i++

			case 56:
				bs := make([]byte, 7)
				if _, err = io.ReadFull(in, bs); err != nil {
					return errors.WithStack(err)
				}
				delta := uint64(bs[0])<<48 | uint64(bs[1])<<40 | uint64(bs[2])<<32 | uint64(bs[3])<<24 |
					uint64(bs[4])<<16 | uint64(bs[5])<<8 | uint64(bs[6])
				rle.setValue(i, deltaBase > 0, delta)
				i++

			case 64:
				bs := make([]byte, 8)
				if _, err = io.ReadFull(in, bs); err != nil {
					return errors.WithStack(err)
				}
				// fixme: cast to uint64
				delta := uint64(bs[0])<<56 | uint64(bs[1])<<48 | uint64(bs[2])<<40 | uint64(bs[3])<<32 |
					uint64(bs[4])<<24 | uint64(bs[5])<<16 | uint64(bs[6])<<8 | uint64(bs[7])
				rle.setValue(i, deltaBase > 0, delta)
				i++

			default:
				return errors.Errorf("int rle v2 encoding delta width %d deprecated", deltaWidth)
			}
		}

	default:
		return errors.Errorf("sub encoding %d for int rle v2 not recognized", rle.sub)
	}
	return nil
}

func (rle *intRleV2) setValue(i uint32, positive bool, delta uint64) {
	if rle.signed {
		if positive {
			rle.literals[i] = rle.literals[i-1] + int64(delta)
		} else {
			rle.literals[i] = rle.literals[i-1] - int64(delta)
		}
	} else {
		if positive {
			rle.uliterals[i] = rle.uliterals[i-1] + uint64(delta)
		} else {
			rle.uliterals[i] = rle.uliterals[i-1] - uint64(delta)
		}
	}
}

func getWidth(w byte, delta bool) (width byte, err error) {
	switch w {
	case 0:
		if delta {
			width = 0
		} else {
			width = 1
		}
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
