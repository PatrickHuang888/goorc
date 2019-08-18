package orc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
	"math"
)

const (
	MIN_REPEAT_SIZE = 3

	Encoding_SHORT_REPEAT = byte(0)
	Encoding_DIRECT       = byte(1)
	Encoding_PATCHED_BASE = byte(2)
	Encoding_DELTA        = byte(3)
	Encoding_UNSET        = byte(255)

	BITS_SLOTS = 65
	MAX_SCOPE  = 512
)

type Decoder interface {
	readValues(in *bytes.Buffer) error
	reset()
	len() int
}

type Encoder interface {
	writeValues(out *bytes.Buffer) error
}

type decoder struct {
	consumedIndex int
}

func (d *decoder) reset() {
	d.consumedIndex = 0
}

type byteRunLength struct {
	decoder
	literals []byte
}

func (brl *byteRunLength) readValues(in *bytes.Buffer) error {
	for in.Len() > 0 {
		control, err := in.ReadByte()
		if err != nil {
			return errors.WithStack(err)
		}
		if control < 0x80 { // run
			l := int(control) + MIN_REPEAT_SIZE
			v, err := in.ReadByte()
			if err != nil {
				return errors.WithStack(err)
			}
			for i := 0; i < l; i++ {
				brl.literals = append(brl.literals, v)
			}
		} else { // literals
			l := int(-int8(control))
			for i := 0; i < l; i++ {
				v, err := in.ReadByte()
				if err != nil {
					return errors.WithStack(err)
				}
				brl.literals = append(brl.literals, v)
			}
		}
	}
	return nil
}

func (brl *byteRunLength) writeValues(out *bytes.Buffer) error {
	// toAssure: in case run==0
	mark := 0
	for i := 0; i < len(brl.literals); {
		b := brl.literals[i]
		length := 1

		// run
		if (i+2 < len(brl.literals)) && (brl.literals[i+1] == b && brl.literals[i+2] == b) {
			if mark != i { // write out length before run
				l := i - mark
				if err := out.WriteByte(byte(-int8(l))); err != nil {
					return errors.WithStack(err)
				}
				if _, err := out.Write(brl.literals[mark:i]); err != nil {
					return errors.WithStack(err)
				}
			}
			length = 3
			for ; i+length < len(brl.literals) && length < 130 && brl.literals[i+length] == b; length++ {
			}
			if err := out.WriteByte(byte(length - 3)); err != nil {
				return errors.WithStack(err)
			}
			if err := out.WriteByte(b); err != nil {
				return errors.WithStack(err)
			}
			mark = i + length
		}
		i += length
	}

	if mark < len(brl.literals) { // write left length
		l := len(brl.literals) - mark
		if err := out.WriteByte(byte(-int8(l))); err != nil {
			return errors.WithStack(err)
		}
		if _, err := out.Write(brl.literals[mark:len(brl.literals)]); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (brl *byteRunLength) reset() {
	brl.literals = brl.literals[:0]
	brl.decoder.reset()
}

func (brl *byteRunLength) len() int {
	return len(brl.literals)
}

type boolRunLength struct {
	bools []bool
	byteRunLength
}

func (brl *boolRunLength) reset() {
	brl.bools = brl.bools[:0]
	brl.byteRunLength.reset()
}

// bool run length use byte run length encoding, but how to know the length of bools?
func (brl *boolRunLength) readValues(in *bytes.Buffer) error {
	if err := brl.byteRunLength.readValues(in); err != nil {
		return errors.WithStack(err)
	}
	bs := brl.byteRunLength.literals
	for i := 0; i < len(bs); i++ {
		for j := 0; j <= 7; j++ {
			v := bs[i]>>byte(7-j) == 0x01
			brl.bools = append(brl.bools, v)
		}
	}
	return nil
}

func (brl *boolRunLength) writeValues(out *bytes.Buffer) error {
	var bs []byte
	for i := 0; i < len(brl.bools); {
		j := 0
		var b byte
		for ; j <= 7 && (i+j) < len(brl.bools); j++ {
			if brl.bools[i+j] {
				b |= 0x01 << byte(7-j)
			}
		}
		bs = append(bs, b)
		i += j
	}
	brl.byteRunLength.literals = bs
	if err := brl.byteRunLength.writeValues(out); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

/*type intRunLengthV1 struct {
	signed      bool
	numLiterals int
	literals    []int64
	uliterals   []uint64
}

func (irl *intRunLengthV1) readValues(in *bytes.Buffer) error {
	for in.Len() > 0 {
		control, err := in.ReadByte()
		if err != nil {
			return errors.WithStack(err)
		}
		mark := irl.numLiterals
		if control < 0x80 { // run
			num := int(control) + MIN_REPEAT_SIZE
			irl.numLiterals += num
			if irl.signed {
				ls := make([]int64, irl.numLiterals)
				copy(ls, irl.literals)
				irl.literals = ls
			} else {
				ls := make([]uint64, irl.numLiterals)
				copy(ls, irl.uliterals)
				irl.uliterals = ls
			}
			// delta
			d, err := in.ReadByte()
			if err != nil {
				return errors.WithStack(err)
			}
			delta := int64(int8(d))
			if irl.signed {
				v, err := binary.ReadVarint(in)
				if err != nil {
					return errors.WithStack(err)
				}
				for i := 0; i < num; i++ {
					irl.literals[mark+i] = v + delta
				}
			} else {
				v, err := binary.ReadUvarint(in)
				if err != nil {
					return errors.WithStack(err)
				}
				for i := 0; i < num; i++ {
					if delta > 0 {
						irl.uliterals[mark+i] = v + uint64(delta)
					} else {
						irl.uliterals[mark+i] = v - uint64(-delta)
					}
				}
			}

		} else {
			num := -int(int8(control))
			irl.numLiterals += num
			if irl.signed {
				ls := make([]int64, irl.numLiterals)
				copy(ls, irl.literals)
				irl.literals = ls
			} else {
				ls := make([]uint64, irl.numLiterals)
				copy(ls, irl.uliterals)
				irl.uliterals = ls
			}
			if irl.signed {
				for i := 0; i < num; i++ {
					v, err := binary.ReadVarint(in)
					if err != nil {
						return errors.WithStack(err)
					}
					irl.literals[mark+i] = v
				}
			} else {
				for i := 0; i < num; i++ {
					v, err := binary.ReadUvarint(in)
					if err != nil {
						return errors.WithStack(err)
					}
					irl.uliterals[mark+i] = v
				}
			}
		}
	}
	return nil
}

func (irl *intRunLengthV1) reset() {
	irl.numLiterals = 0
	irl.signed = false
}

func (irl *intRunLengthV1) writeValues(out *bytes.Buffer) error {
	if irl.numLiterals != 0 {

	}
	return nil
}*/

// direct v2 for string/char/varchar
type bytesDirectV2 struct {
	decoder
	content [][]byte // utf-8 bytes
	length  []uint64
	pos     int
}

func (bd *bytesDirectV2) reset() {
	bd.content = bd.content[:0]
	//bd.length = nil
	//bd.pos = 0
	bd.decoder.reset()
}

func (bd *bytesDirectV2) len() int {
	return len(bd.content)
}

// decode bytes, but should have extracted length stream first by rle v2
// as length field
func (bd *bytesDirectV2) readValues(in *bytes.Buffer) error {
	if bd.length == nil {
		return errors.New("length stream should extracted first")
	}
	for in.Len() > 0 {
		if bd.pos == len(bd.length) {
			return errors.New("beyond length data")
		}
		length := bd.length[bd.pos]
		b := make([]byte, length)
		if _, err := io.ReadFull(in, b); err != nil {
			return errors.WithStack(err)
		}
		bd.content = append(bd.content, b)
		bd.pos++
	}
	return nil
}

// write out content do not base length field, just base on len of content
func (bd *bytesDirectV2) writeValues(out *bytes.Buffer) error {
	for _, c := range bd.content {
		if _, err := out.Write(c); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// int run length encoding v2
type intRleV2 struct {
	sub           byte // sub encoding
	signed        bool
	literals      []int64
	uliterals     []uint64
	consumedIndex int // for read
}

func (rle *intRleV2) reset() {
	if rle.signed {
		rle.literals = rle.literals[:0]
	} else {
		rle.uliterals = rle.uliterals[:0]
	}
	rle.sub = Encoding_UNSET
	rle.consumedIndex = 0
}

func (rle *intRleV2) len() int {
	if rle.signed {
		return len(rle.literals)
	} else {
		return len(rle.uliterals)
	}
}

// decoding buffer all to u/literals
func (rle *intRleV2) readValues(in *bytes.Buffer) error {
	for in.Len() > 0 {
		// header from MSB to LSB
		firstByte, err := in.ReadByte()
		if err != nil {
			return errors.WithStack(err)
		}
		rle.sub = firstByte >> 6
		switch rle.sub {
		case Encoding_SHORT_REPEAT:
			header := firstByte
			width := 1 + (header>>3)&0x07 // width 3bit at position 3
			repeatCount := int(3 + (header & 0x07))
			log.Tracef("decoding: int rl v2 Short Repeat of count %d, and len now is %d",
				repeatCount, rle.len)

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
				for i := 0; i < repeatCount; i++ {
					rle.literals = append(rle.literals, unZigzag(x))
				}
			} else {
				for i := 0; i < repeatCount; i++ {
					rle.uliterals = append(rle.uliterals, x)
				}
			}

		case Encoding_DIRECT: // numbers encoding in big-endian
			b1, err := in.ReadByte()
			if err != nil {
				return errors.WithStack(err)
			}
			header := uint16(firstByte)<<8 | uint16(b1) // 2 byte header
			w := (header >> 9) & 0x1F                   // width 5bits, bit 3 to 8
			width, err := widthDecoding(byte(w), false)
			if err != nil {
				return errors.WithStack(err)
			}
			length := int(header&0x1FF + 1)
			log.Tracef("decoding: int rl v2 Direct width %d length %d len now %d", width, length, rle.len())

			for i := 0; i < length; {
				var x uint64
				switch width {
				case 1:
					b, err := in.ReadByte()
					if err != nil {
						return errors.WithStack(err)
					}
					j := 0
					for ; j <= 7 && i+j < length; j++ {
						x = uint64(b >> byte(7-j) & 0x01)
						if rle.signed {
							rle.literals = append(rle.literals, unZigzag(x))
						} else {
							rle.uliterals = append(rle.uliterals, x)
						}
					}
					i += j
				case 2:
					b, err := in.ReadByte()
					if err != nil {
						return errors.WithStack(err)
					}
					j := 0
					for ; j <= 3 && i+j < length; j++ {
						x = uint64(b >> byte(3-j) * 2 & 0x03)
						if rle.signed {
							rle.literals = append(rle.literals, unZigzag(x))
						} else {
							rle.uliterals = append(rle.uliterals, x)
						}
					}
					i += j
				case 4:
					b, err := in.ReadByte()
					if err != nil {
						return errors.WithStack(err)
					}
					x := uint64(b >> 4 & 0x0f)
					if rle.signed {
						rle.literals = append(rle.literals, unZigzag(x))
					} else {
						rle.uliterals = append(rle.uliterals, x)
					}
					i++
					if i < length {
						x := uint64(b & 0x0f)
						if rle.signed {
							rle.literals = append(rle.literals, unZigzag(x))
						} else {
							rle.uliterals = append(rle.uliterals, x)
						}
						i++
					}
				case 8:
					b, err := in.ReadByte()
					if err != nil {
						return errors.WithStack(err)
					}
					x = uint64(b)
					if rle.signed {
						rle.literals = append(rle.literals, unZigzag(x))
					} else {
						rle.uliterals = append(rle.uliterals, x)
					}
					i++
				case 16:
					b := make([]byte, 2)
					if _, err = io.ReadFull(in, b); err != nil {
						return errors.WithStack(err)
					}
					x = uint64(b[1]) | uint64(b[0])<<8
					if rle.signed {
						rle.literals = append(rle.literals, unZigzag(x))
					} else {
						rle.uliterals = append(rle.uliterals, x)
					}
					i++
				case 24:
					b := make([]byte, 3)
					if _, err = io.ReadFull(in, b); err != nil {
						return errors.WithStack(err)
					}
					x = uint64(b[2]) | uint64(b[1])<<8 | uint64(b[0])<<16
					if rle.signed {
						rle.literals = append(rle.literals, unZigzag(x))
					} else {
						rle.uliterals = append(rle.uliterals, x)
					}
					i++
				case 32:
					b := make([]byte, 4)
					if _, err = io.ReadFull(in, b); err != nil {
						return errors.WithStack(err)
					}
					x = uint64(b[3]) | uint64(b[2])<<8 | uint64(b[1])<<16 | uint64(b[0])<<24
					if rle.signed {
						rle.literals = append(rle.literals, unZigzag(x))
					} else {
						rle.uliterals = append(rle.uliterals, x)
					}
					i++
				case 40:
					b := make([]byte, 5)
					if _, err = io.ReadFull(in, b); err != nil {
						return errors.WithStack(err)
					}
					x = uint64(b[4]) | uint64(b[3])<<8 | uint64(b[2])<<16 | uint64(b[1])<<24 | uint64(b[0])<<32
					if rle.signed {
						rle.literals = append(rle.literals, unZigzag(x))
					} else {
						rle.uliterals = append(rle.uliterals, x)
					}
					i++
				case 48:
					b := make([]byte, 6)
					if _, err = io.ReadFull(in, b); err != nil {
						return errors.WithStack(err)
					}
					x = uint64(b[5]) | uint64(b[4])<<8 | uint64(b[4])<<16 | uint64(b[2])<<24 | uint64(b[1])<<32 |
						uint64(b[0])<<40
					if rle.signed {
						rle.literals = append(rle.literals, unZigzag(x))
					} else {
						rle.uliterals = append(rle.uliterals, x)
					}
					i++
				case 56:
					b := make([]byte, 7)
					if _, err = io.ReadFull(in, b); err != nil {
						return errors.WithStack(err)
					}
					x = uint64(b[6]) | uint64(b[5])<<8 | uint64(b[4])<<16 | uint64(b[3])<<24 | uint64(b[2])<<32 |
						uint64(b[1])<<40 | uint64(b[0])<<48
					if rle.signed {
						rle.literals = append(rle.literals, unZigzag(x))
					} else {
						rle.uliterals = append(rle.uliterals, x)
					}
					i++
				case 64:
					b := make([]byte, 8)
					if _, err = io.ReadFull(in, b); err != nil {
						return errors.WithStack(err)
					}
					x = uint64(b[7]) | uint64(b[1])<<8 | uint64(b[6])<<16 | uint64(b[5])<<24 | uint64(b[4])<<32 |
						uint64(b[3])<<40 | uint64(b[2])<<48 | uint64(b[1])<<56
					if rle.signed {
						rle.literals = append(rle.literals, unZigzag(x))
					} else {
						rle.uliterals = append(rle.uliterals, x)
					}
					i++
				default:
					return errors.Errorf("decoding: int rl v2 Direct width %d not supported", width)
				}
			}

		case Encoding_PATCHED_BASE:
			// fixme: according to base value is a signed smallest value, patch should always signed?
			if !rle.signed {
				return errors.New("decoding: int rl v2 patch signed setting should not false")
			}

			mark := len(rle.literals)
			header := make([]byte, 4) // 4 byte header
			_, err = io.ReadFull(in, header[1:4])
			if err != nil {
				return errors.WithStack(err)
			}
			header[0] = firstByte
			// 5 bit width
			w := header[0] >> 1 & 0x1f
			width, err := widthDecoding(w, false) // 5 bits W
			if err != nil {
				return errors.WithStack(err)
			}
			length := uint16(header[0])&0x01<<8 | uint16(header[1]) + 1 // 9 bits length, value 1 to 512
			bw := uint16(header[2])>>5&0x07 + 1                         // 3 bits base value width(BW), value 1 to 8 bytes
			pw, err := widthDecoding(header[2]&0x1f, false)             // 5 bits patch width(PW), value on table
			if err != nil {
				return errors.WithStack(err)
			}
			pgw := uint16(header[3])>>5&0x07 + 1 // 3 bits patch gap width(PGW), value 1 to 8 bits
			if (int(pw) + int(pgw)) > 64 {
				return errors.New("decoding: int rl v2, patchWidth+gapWidth must less or equal to 64")
			}
			pll := header[3] & 0x1f // 5bits patch list length, value 0 to 31

			baseBytes := make([]byte, bw)
			if _, err = io.ReadFull(in, baseBytes); err != nil {
				return errors.WithStack(err)
			}
			// base value big endian with msb of negative mark
			var base int64
			if bw == 0 {
				return errors.New("decoding: int rl v2 Patch baseWidth 0 not impl")
			}
			neg := (baseBytes[0] >> 7) == 0x01
			baseBytes[0] = baseBytes[0] & 0x7f // remove msb
			var ubase uint64
			for i := uint16(0); i < bw; i++ {
				ubase |= uint64(baseBytes[i]) << (byte(bw-i-1) * 8)
			}
			base = int64(ubase)
			if neg {
				base = -base
			}

			log.Tracef("decoding: int rl v2 Patch width %d length %d bw %d patchWidth %d pll %d base %d",
				width, length, bw, pw, pll, base)

			// data values
			// base is the smallest one, so data values all positive
			for i := 0; i < int(length); {
				switch width {
				case 1:
					b, err := in.ReadByte()
					if err != nil {
						return errors.WithStack(err)
					}
					j := 0
					for ; j < 8 && i+j < int(length); j++ {
						x := base + int64(b>>byte(j)&0x01)
						rle.literals = append(rle.literals, x)
					}
					i += j
				case 2:
					b, err := in.ReadByte()
					if err != nil {
						return errors.WithStack(err)
					}
					j := 0
					for ; j < 4 && i+j < int(length); j++ {
						x := base + int64(b>>byte(j*2)&0x03)
						rle.literals = append(rle.literals, x)
					}
					i += j
				case 3:
					return errors.New("decoding: int rl v2 patch W 3 not impl")
				case 4:
					b, err := in.ReadByte()
					if err != nil {
						return errors.WithStack(err)
					}
					x := base + int64(b>>4)
					rle.literals = append(rle.literals, x)
					i++
					if i < int(length) {
						x = base + int64(x&0x0f)
						rle.literals = append(rle.literals, x)
					}
					i++
				case 5:
					return errors.New("decoding: int rl v2 patch W 5 not impl")
				case 6:
					return errors.New("decoding: int rl v2 patch W 6 not impl")
				case 7:
					return errors.New("decoding: int rl v2 patch W 7 not impl")
				case 8:
					b, err := in.ReadByte()
					if err != nil {
						return errors.WithStack(err)
					}
					x := base + int64(b)
					rle.literals = append(rle.literals, x)
					i++
				case 9:
					fallthrough
				case 10:
					fallthrough
				case 11:
					return errors.Errorf("decoding: int rl v2 patch W %d not impl", width)
				case 12:
					b0, err := in.ReadByte()
					if err != nil {
						return errors.WithStack(err)
					}
					b1, err := in.ReadByte()
					if err != nil {
						return errors.WithStack(err)
					}
					x := base + (int64(b1&0x0f) | int64(b0)<<4)
					rle.literals = append(rle.literals, x)
					i++
					if i < int(length) {
						b2, err := in.ReadByte()
						if err != nil {
							return errors.WithStack(err)
						}
						x = base + (int64(b1>>4)<<8 | int64(b2))
						rle.literals = append(rle.literals, x)
					}
				case 13:
					fallthrough
				case 14:
					fallthrough
				case 15:
					return errors.Errorf("decoding: int rl v2 patch W %d not impl", width)
				case 16:
					bs := make([]byte, 2)
					if _, err := io.ReadFull(in, bs); err != nil {
						return errors.WithStack(err)
					}
					x := base + (int64(bs[0])<<8 | int64(bs[1])) // big endian?
					rle.literals = append(rle.literals, x)
					i++
				case 17:
					fallthrough
				case 18:
					fallthrough
				case 19:
					fallthrough
				case 20:
					fallthrough
				case 21:
					return errors.Errorf("decoding: int rl v2 patch W %d not impl", width)
				case 24:
					bs := make([]byte, 3)
					if _, err := io.ReadFull(in, bs); err != nil {
						return errors.WithStack(err)
					}
					x := base + (int64(bs[0])<<16 | int64(bs[1])<<8 | int64(bs[2]))
					rle.literals = append(rle.literals, x)
					i++
				case 26:
					fallthrough
				case 28:
					fallthrough
				case 30:
					return errors.Errorf("decoding: int rl v2 patch W %d not impl", width)
				case 32:
					bs := make([]byte, 4)
					if _, err := io.ReadFull(in, bs); err != nil {
						return errors.WithStack(err)
					}
					x := base + (int64(bs[0])<<24 | int64(bs[1])<<16 | int64(bs[2])<<8 | int64(bs[3]))
					rle.literals = append(rle.literals, x)
					i++
				case 40:
					bs := make([]byte, 5)
					if _, err := io.ReadFull(in, bs); err != nil {
						return errors.WithStack(err)
					}
					x := base + (int64(bs[0])<<30 | int64(bs[1])<<24 | int64(bs[2])<<16 | int64(bs[3])<<8 | int64(bs[4]))
					rle.literals = append(rle.literals, x)
					i++
				case 48:
					bs := make([]byte, 6)
					if _, err := io.ReadFull(in, bs); err != nil {
						return errors.WithStack(err)
					}
					x := base + (int64(bs[0])<<40 | int64(bs[1])<<32 | int64(bs[2])<<24 | int64(bs[3])<<16 |
						int64(bs[4])<<8 | int64(bs[5]))
					rle.literals = append(rle.literals, x)
					i++
				case 56:
					bs := make([]byte, 7)
					if _, err := io.ReadFull(in, bs); err != nil {
						return errors.WithStack(err)
					}
					x := base + (int64(bs[0])<<48 | int64(bs[1])<<40 | int64(bs[2])<<32 | int64(bs[3])<<24 |
						int64(bs[4])<<16 | int64(bs[5])<<8 | int64(bs[6]))
					rle.literals = append(rle.literals, x)
					i++
				case 64:
					bs := make([]byte, 8)
					if _, err := io.ReadFull(in, bs); err != nil {
						return errors.WithStack(err)
					}
					x := base + (int64(bs[0])<<56 | int64(bs[1])<<48 | int64(bs[2])<<40 | int64(bs[3])<<32 |
						int64(bs[4])<<24 | int64(bs[5])<<16 | int64(bs[6])<<8 | int64(bs[7]))
					rle.literals = append(rle.literals, x)
					i++

				default:
					return errors.Errorf("decoding: int rl v2 PATCH width %d not supported", width)
				}
			}
			// patched values, PGW+PW must <= 64
			for i := 0; i < int(pll); i++ {
				var pv uint64 // patch value
				b, err := in.ReadByte()
				if err != nil {
					return errors.WithStack(err)
				}
				// gapWidth 1 to 8 bits
				pg := b >> (8 - pgw) // patch gap
				// todo: pg==0
				mark += int(pg)
				// patchWidth 1 to 64 bits according to encoding table
				pbn := int(math.Ceil((float64(pgw) + float64(pw)) / float64(8))) // patch bytes number
				pbs := make([]byte, pbn)
				pbs[0] = b
				if pbn > 1 { // need more bytes
					if _, err := io.ReadFull(in, pbs[1:]); err != nil {
						return errors.WithStack(err)
					}
				}

				pbs[0] = pbs[0] << pgw >> pgw // patch value, remove gapWidth first
				for j := 0; j < pbn; j++ {
					pv |= uint64(pbs[j]) << (uint(pbn-j-1) * 8)
				}
				bitsLeft := pbn*8 - int(pgw) - int(pw)
				pv = pv >> uint(bitsLeft)
				// pv should be at largest 63 bits?
				v := rle.literals[mark]
				v -= base // remove added base first
				v |= int64(pv << width)
				v += base // add base back
				rle.literals[mark] = v
			}

		case Encoding_DELTA:
			// header: 2 bytes, base value: varint, delta base: signed varint
			header := make([]byte, 2)
			header[0] = firstByte
			header[1], err = in.ReadByte()
			if err != nil {
				return errors.WithStack(err)
			}
			width, err := widthDecoding(header[0]>>1&0x1f, true)
			if err != nil {
				return errors.WithStack(err)
			}
			length := int(header[0])&0x01<<8 | int(header[1]) + 1

			log.Tracef("decoding: irl v2 Delta length %d, width %d, len now %d", length, width, rle.len())

			if rle.signed {
				baseValue, err := binary.ReadVarint(in)
				if err != nil {
					return errors.WithStack(err)
				}
				rle.literals = append(rle.literals, baseValue)
			} else {
				baseValue, err := binary.ReadUvarint(in)
				if err != nil {
					return errors.WithStack(err)
				}
				rle.uliterals = append(rle.uliterals, baseValue)
			}
			deltaBase, err := binary.ReadVarint(in)
			if err != nil {
				return errors.WithStack(err)
			}

			if deltaBase >= 0 {
				rle.addValue(true, uint64(deltaBase))
			} else {
				rle.addValue(false, uint64(-deltaBase))
			}

			// delta values: W * (L-2)
			i := 2
			for i < length {
				switch width {
				case 0:
					// fix delta based on delta base
					if deltaBase >= 0 {
						rle.addValue(true, uint64(deltaBase))
					} else {
						rle.addValue(false, uint64(-deltaBase))
					}
					i++

				case 2:
					b, err := in.ReadByte()
					if err != nil {
						return errors.WithStack(err)
					}
					for j := 0; j < 4; j++ {
						delta := uint64(b >> byte((3-j)*2) & 0x03)
						if i+j < length {
							rle.addValue(deltaBase > 0, delta)
						}
					}
					i += 4

				case 4:
					b, err := in.ReadByte()
					if err != nil {
						return errors.WithStack(err)
					}
					delta := uint64(b) >> 4
					rle.addValue(deltaBase > 0, delta)
					i++
					if i < length {
						delta = uint64(b) & 0x0f
						rle.addValue(deltaBase > 0, delta)
						i++
					}

				case 8:
					delta, err := in.ReadByte()
					if err != nil {
						return errors.WithStack(err)
					}
					rle.addValue(deltaBase > 0, uint64(delta))
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
					rle.addValue(deltaBase > 0, delta)
					i++

				case 24:
					bs := make([]byte, 3)
					if _, err = io.ReadFull(in, bs); err != nil {
						return errors.WithStack(err)
					}
					delta := uint64(bs[0])<<16 | uint64(bs[1])<<8 | uint64(bs[2])
					rle.addValue(deltaBase > 0, delta)
					i++

				case 32:
					bs := make([]byte, 4)
					if _, err = io.ReadFull(in, bs); err != nil {
						return errors.WithStack(err)
					}
					delta := uint64(bs[0])<<24 | uint64(bs[1])<<16 | uint64(bs[2])<<8 | uint64(bs[3])
					rle.addValue(deltaBase > 0, delta)
					i++

				case 40:
					bs := make([]byte, 5)
					if _, err = io.ReadFull(in, bs); err != nil {
						return errors.WithStack(err)
					}
					delta := uint64(bs[0])<<32 | uint64(bs[1])<<24 | uint64(bs[2])<<16 | uint64(bs[3])<<8 |
						uint64(bs[4])
					rle.addValue(deltaBase > 0, delta)
					i++

				case 48:
					bs := make([]byte, 6)
					if _, err = io.ReadFull(in, bs); err != nil {
						return errors.WithStack(err)
					}
					delta := uint64(bs[0])<<40 | uint64(bs[1])<<32 | uint64(bs[2])<<24 | uint64(bs[3])<<16 |
						uint64(bs[4])<<8 | uint64(bs[5])
					rle.addValue(deltaBase > 0, delta)
					i++

				case 56:
					bs := make([]byte, 7)
					if _, err = io.ReadFull(in, bs); err != nil {
						return errors.WithStack(err)
					}
					delta := uint64(bs[0])<<48 | uint64(bs[1])<<40 | uint64(bs[2])<<32 | uint64(bs[3])<<24 |
						uint64(bs[4])<<16 | uint64(bs[5])<<8 | uint64(bs[6])
					rle.addValue(deltaBase > 0, delta)
					i++

				case 64:
					bs := make([]byte, 8)
					if _, err = io.ReadFull(in, bs); err != nil {
						return errors.WithStack(err)
					}
					delta := uint64(bs[0])<<56 | uint64(bs[1])<<48 | uint64(bs[2])<<40 | uint64(bs[3])<<32 |
						uint64(bs[4])<<24 | uint64(bs[5])<<16 | uint64(bs[6])<<8 | uint64(bs[7])
					rle.addValue(deltaBase > 0, delta)
					i++

				default:
					return errors.Errorf("decoding: int rl v2 Delta width %d not support", width)
				}
			}

		default:
			return errors.Errorf("decoding: int rl v2 encoding sub %d not recognized", rle.sub)
		}

	} // finish read buffer

	return nil
}

// write all literals to buffer
func (rle *intRleV2) writeValues(out *bytes.Buffer) error {

	for idx := 0; idx < rle.len(); {
		if (rle.len() - idx) <= MIN_REPEAT_SIZE {
			rle.sub = Encoding_DIRECT
			return rle.writeDirect(out, idx, rle.len()-idx)
		}
		// try repeat first
		c := rle.getRepeat(idx)
		if c >= 3 {
			// short repeat
			if c <= 10 {
				rle.sub = Encoding_SHORT_REPEAT
				var x uint64
				if rle.signed {
					log.Tracef("encoding: irl v2 Short Repeat count %d, value %d at index %d",
						c, rle.literals[idx], idx)
					x = zigzag(rle.literals[idx])
				} else {
					log.Tracef("encoding: irl v2 Short Repeat count %d, value %d at index %d",
						c, rle.uliterals[idx], idx)
					x = rle.uliterals[idx]
				}
				idx += int(c)
				if err := rle.writeShortRepeat(c-3, x, out); err != nil {
					return errors.WithStack(err)
				}
				continue
			}

			// fixed delta 0
			rle.sub = Encoding_DELTA
			b := make([]byte, 8)
			var n int
			if rle.signed {
				n = binary.PutVarint(b, rle.literals[idx])
			} else {
				n = binary.PutUvarint(b, rle.uliterals[idx])
			}
			log.Tracef("encoding: irl v2 Fixed Delta 0 count %d at index %d", c, idx)
			idx += int(c)
			if err := rle.writeDelta(b[:n], 0, c, []uint64{}, out); err != nil {
				return errors.WithStack(err)
			}
			continue
		}

		// delta, need width should be stable?
		var dv deltaValues
		if rle.tryDeltaEncoding(idx, &dv) {
			b := make([]byte, 8)
			var n int
			if rle.signed {
				n = binary.PutVarint(b, rle.literals[idx])
			} else {
				n = binary.PutUvarint(b, rle.uliterals[idx])
			}
			idx += int(dv.length)
			if err := rle.writeDelta(b[:n], dv.base, dv.length, dv.deltas, out); err != nil {
				return errors.WithStack(err)
			}
			continue
		}

		// try patch
		var pvs patchedValues
		sub, err := rle.bitsWidthAnalyze(idx, &pvs)
		if err != nil {
			return errors.WithStack(err)
		}

		// toAssure: encoding until MAX_SCOPE?
		var scope int
		l := rle.len() - idx
		if l >= MAX_SCOPE {
			scope = MAX_SCOPE
		} else {
			scope = l
		}

		if sub == Encoding_PATCHED_BASE {
			rle.sub = Encoding_PATCHED_BASE
			log.Tracef("encoding: int rl v2 Patch %s", pvs.toString())
			if err := writePatch(&pvs, out); err != nil {
				return errors.WithStack(err)
			}
			idx += scope
			continue
		}

		if sub == Encoding_DIRECT {
			rle.sub = Encoding_DIRECT
			if err := rle.writeDirect(out, idx, scope); err != nil {
				return errors.WithStack(err)
			}
			idx += scope
			continue
		}

		return errors.New("encoding: no sub decided!")
	}

	return nil
}

func (rle *intRleV2) writeDirect(out *bytes.Buffer, idx int, length int) error {
	if rle.sub != Encoding_DIRECT {
		return errors.New("encoding: int rl v2 sub error ")
	}
	header := make([]byte, 2)
	header[0] = Encoding_DIRECT << 6
	values := make([]uint64, length)
	var width byte
	for i := 0; i < length; i++ {
		var x uint64
		if rle.signed {
			x = zigzag(rle.literals[idx+i])
		} else {
			x = rle.uliterals[idx+i]
		}
		v := getAlignedWidth(x)
		if v > width {
			width = v
		}
		values[i] = x
	}
	w, err := widthEncoding(width)
	if err != nil {
		return errors.WithStack(err)
	}
	header[0] |= (w & 0x1f) << 1 // 5 bit W
	l := uint16(length - 1)
	header[0] |= byte(l>>8) & 0x01
	header[1] = byte(l)
	log.Tracef("encoding: int rl v2 Direct width %d length %d ", width, length)
	if _, err := out.Write(header); err != nil {
		return errors.WithStack(err)
	}
	for i := 0; i < len(values); i++ {
		j := int(width - 8)
		for ; j >= 0; j -= 8 {
			v := byte(values[i] >> uint(j))
			if err := out.WriteByte(v); err != nil {
				return errors.WithStack(err)
			}
		}
		j += 8
		if j != 0 { // padding
			v := byte(values[i]) << byte(8-j)
			if err := out.WriteByte(v); err != nil {
				return errors.WithStack(err)
			}
		}
	}
	return nil
}

type deltaValues struct {
	base   int64
	length uint16
	deltas []uint64
}

// for monotonically increasing or decreasing sequences
// run length should be at least 3
// todo: fixed delta
func (rle *intRleV2) tryDeltaEncoding(idx int, dv *deltaValues) bool {
	if idx+2 < rle.len() {
		if rle.signed {
			if rle.literals[idx] == rle.literals[idx+1] { // delta can not be same for first 2
				return false
			} else {
				dv.base = rle.literals[idx+1] - rle.literals[idx]
			}

			if dv.base > 0 { // increasing
				if rle.literals[idx+2] >= rle.literals[idx+1] {
					dv.deltas = append(dv.deltas, uint64(rle.literals[idx+2]-rle.literals[idx+1]))
				} else {
					return false
				}
			} else {
				if rle.literals[idx+2] < rle.literals[idx+1] {
					dv.deltas = append(dv.deltas, uint64(rle.literals[idx+1]-rle.literals[idx+2]))
				} else {
					return false
				}
			}
		} else { // uint
			if rle.uliterals[idx] == rle.uliterals[idx+1] {
				return false
			} else {
				if rle.uliterals[idx] < rle.uliterals[idx+1] {
					dv.base = int64(rle.uliterals[idx+1] - rle.uliterals[idx])
				} else {
					dv.base = -int64(rle.uliterals[idx] - rle.uliterals[idx])
				}
			}

			if dv.base > 0 {
				if rle.uliterals[idx+2] >= rle.uliterals[idx+1] {
					dv.deltas = append(dv.deltas, rle.uliterals[idx+2]-rle.uliterals[idx+1])
				} else {
					return false
				}
			} else {
				if rle.uliterals[idx+2] < rle.uliterals[idx+1] {
					dv.deltas = append(dv.deltas, rle.uliterals[idx+1]-rle.uliterals[idx+2])
				} else {
					return false
				}
			}
		}
		dv.length = 3

		for i := idx + 3; i < rle.len() && dv.length < 512; i++ {
			if rle.signed {
				if dv.base > 0 {
					if rle.literals[i] >= rle.literals[i-1] {
						dv.deltas = append(dv.deltas, uint64(rle.literals[i]-rle.literals[i-1]))
					} else {
						break
					}
				} else {
					if rle.literals[i] < rle.literals[i-1] {
						dv.deltas = append(dv.deltas, uint64(rle.literals[i-1]-rle.literals[i]))
					} else {
						break
					}
				}
			} else {
				if dv.base > 0 {
					if rle.uliterals[i] >= rle.uliterals[i-1] {
						dv.deltas = append(dv.deltas, rle.uliterals[i]-rle.uliterals[i-1])
					} else {
						break
					}
				} else {
					if rle.uliterals[i] < rle.uliterals[i-1] {
						dv.deltas = append(dv.deltas, rle.uliterals[i-1]-rle.uliterals[i])
					} else {
						break
					}
				}
			}
			dv.length += 1
		}

	} else { // only 2 number cannot be delta
		return false
	}

	rle.sub = Encoding_DELTA
	return true
}

// direct encoding when constant bit width, length 1 to 512
func (rle *intRleV2) tryDirectEncoding(idx int) bool {
	return false
}

type patchedValues struct {
	values []uint64
	width  byte // value bits width
	base   int64
	//pll        byte     // patch list length
	gapWidth   byte     // patch gap bits width, 1 to 8
	gaps       []byte   // patch gap values
	patchWidth byte     // patch bits width according to table, 1 to 64
	patches    []uint64 // patch values, already shifted valuebits
}

func (pvs *patchedValues) toString() string {
	return fmt.Sprintf("patch values: base %d, value width %d, pll %d, gapWidth %d, patchWidth %d",
		pvs.base, pvs.width, len(pvs.patches), pvs.gapWidth, pvs.patchWidth)
}

func writePatch(pvs *patchedValues, out *bytes.Buffer) error {
	// write header
	header := make([]byte, 4)
	header[0] = Encoding_PATCHED_BASE << 6
	w, err := widthEncoding(pvs.width)
	if err != nil {
		return errors.WithStack(err)
	}
	header[0] |= w << 1
	length := len(pvs.values) - 1 // 1 to 512
	header[0] |= byte(length >> 8 & 0x01)
	header[1] = byte(length & 0xff)
	base := uint64(pvs.base)
	if pvs.base < 0 {
		base = uint64(-pvs.base)
	}
	// base bytes 1 to 8, add 1 bit for negative mark
	baseBytes := byte(math.Ceil(float64(getBitsWidth(base)+1) / 8))
	header[2] = (baseBytes - 1) & 0x07 << 5 // 3bits for 1 to 8
	pw, err := widthEncoding(pvs.patchWidth)
	if err != nil {
		return errors.WithStack(err)
	}
	header[2] |= pw & 0x1f                     // 1f, 5 bits
	header[3] = (pvs.gapWidth - 1) & 0x07 << 5 // 07, 3bits for 1 to 8 bits
	pll := byte(len(pvs.patches))              // patch list length, 0 to 31
	header[3] |= pll & 0x1f
	if _, err := out.Write(header); err != nil {
		return errors.WithStack(err)
	}

	// write base
	if pvs.base < 0 {
		base |= 0x01 << ((baseBytes-1)*8 + 7) // msb set to 1
	}
	for i := int(baseBytes) - 1; i >= 0; i-- { // hxm: watch out loop index
		out.WriteByte(byte((base >> (byte(i) * 8)) & 0xff)) // big endian
	}

	// write W*L positive values
	if err := writeUints(out, w, pvs.values); err != nil {
		return errors.WithStack(err)
	}

	// write patch list, (PLL*(PGW+PW) bytes
	patchBytes := int(math.Ceil(float64(pvs.patchWidth) / float64(8)))
	for i := 0; i < int(pll); i++ {
		v := pvs.gaps[i] << (8 - pvs.gapWidth)
		p := pvs.patches[i]
		shiftW := getBitsWidth(p) - (8 - pvs.gapWidth)
		bhigh := byte(p >> shiftW)
		v |= bhigh
		if err := out.WriteByte(v); err != nil {
			return errors.WithStack(err)
		}
		for j := patchBytes - 2; j >= 0; j-- {
			if shiftW > 8 {
				shiftW -= 8
				v = byte(p >> shiftW)
			} else {
				if j != 0 {
					log.Errorf("patch shift value should less than 8 should be the lowest byte")
				}
				v = byte(p) << (8 - shiftW)
			}
			if err := out.WriteByte(v); err != nil {
				return errors.WithStack(err)
			}
		}
	}
	return nil
}

// analyze bit widths, if return encoding Patch then fill the patchValues, else return encoding Direct
// todo: patch 0
func (rle *intRleV2) bitsWidthAnalyze(idx int, pvs *patchedValues) (sub byte, err error) {
	// toAssure: according to base value is a signed smallest value, patch should always signed?
	if !rle.signed {
		return Encoding_DIRECT, nil
	}

	min := rle.literals[idx]
	var count int
	baseWidthHist := make([]byte, BITS_SLOTS) // slots for 0 to 64 bits widths
	// fixme: patch only apply until to 512?
	for i := 0; i < 512 && idx+i < rle.len(); i++ {
		var x uint64

		v := rle.literals[idx+i]
		if v < min {
			min = v
		}
		// toAssure: using zigzag to decide bits width
		x = zigzag(v)
		baseWidthHist[getAlignedWidth(x)] += 1
		count = i + 1
	}

	// get bits width cover 100% and 90%
	var p100w, p90w byte
	for i := BITS_SLOTS - 1; i >= 0; i-- {
		if baseWidthHist[i] != 0 {
			p100w = byte(i)
			break
		}
	}
	p90 := 0.9
	p90len := int(float64(count) * p90)
	s := 0
	for i := 0; i < BITS_SLOTS; i++ {
		s += int(baseWidthHist[i])
		if s >= p90len && baseWidthHist[i] != 0 {
			p90w = byte(i)
			break
		}
	}
	if p100w-p90w > 1 { // can be patched
		values := make([]uint64, count)
		valuesWidths := make([]byte, count)
		for i := 0; i < count; i++ {
			values[i] = uint64(rle.literals[idx+i] - min)
			valuesWidths[i] = getAlignedWidth(values[i])
		}
		valuesWidthHist := make([]byte, BITS_SLOTS) // values bits width 0 to 64
		for i := 0; i < count; i++ {
			valuesWidthHist[valuesWidths[i]] += 1
		}

		var vp100w, vp95w byte // values width cover 100% and 95%
		for i := BITS_SLOTS - 1; i >= 0; i-- {
			if valuesWidthHist[i] != 0 {
				vp100w = byte(i)
				break
			}
		}
		p95 := 0.95
		v95len := int(float64(count) * p95)
		s = 0
		for i := 0; i < BITS_SLOTS; i++ {
			s += int(valuesWidthHist[i])
			if s >= v95len && valuesWidthHist[i] != 0 {
				vp95w = byte(i)
				break
			}
		}
		if vp100w-vp95w != 0 {
			sub = Encoding_PATCHED_BASE
			pvs.base = min
			pvs.values = values
			pvs.width = vp95w
			// get patches
			for i := 0; i < count; i++ {
				if valuesWidths[i] > pvs.width { // patched value
					pvs.gaps = append(pvs.gaps, byte(i))
					pvs.patches = append(pvs.patches, values[i]>>pvs.width)
				}
			}
			for i := 0; i < len(pvs.gaps); i++ {
				w := getBitsWidth(uint64(pvs.gaps[i]))
				if pvs.gapWidth < w {
					pvs.gapWidth = w
				}
			}
			for i := 0; i < len(pvs.patches); i++ {
				w := getAlignedWidth(pvs.patches[i])
				if pvs.patchWidth < w {
					pvs.patchWidth = w
				}
			}
			if pvs.gapWidth+pvs.patchWidth > 64 {
				return Encoding_UNSET, errors.New("encoding: int rl v2 Patch PGW+PW > 64")
			}
			return
		}
	}
	sub = Encoding_DIRECT
	return
}

func writeUints(out *bytes.Buffer, width byte, values []uint64) error {
	bs := int(math.Ceil(float64(width) / float64(8)))
	for i := 0; i < len(values); i++ {
		for j := bs - 1; j >= 0; j-- {
			x := byte(values[i] >> uint(j*8))
			if err := out.WriteByte(x); err != nil {
				return errors.WithStack(err)
			}
		}
	}
	return nil
}

// first is int64 or uint64 based on signed, length is 9 bit for run length 1 to 512
func (rle *intRleV2) writeDelta(first []byte, deltaBase int64, length uint16, deltas []uint64, out *bytes.Buffer) error {
	var h1, h2 byte   // 2 byte header
	h1 = rle.sub << 6 // 2 bit encoding type
	// delta width
	var w, width byte
	for _, v := range deltas {
		bits := getAlignedWidth(v)
		if bits > width {
			width = bits
		}
	}
	if width == 1 { // delta width no 1?
		width = 2
	}
	//log.Tracef("encoding: irl v2 Delta length %d, width %d", length, width)
	w, err := widthEncoding(width)
	if err != nil {
		return errors.WithStack(err)
	}
	log.Tracef("write delta encoded width %d", w)
	h1 |= w & 0x1F << 1 // 5 bit for W
	length -= 1
	h1 |= byte(length>>8) & 0x01 // 9th bit
	h2 = byte(length & 0xFF)
	if err := out.WriteByte(h1); err != nil {
		return errors.WithStack(err)
	}
	if err := out.WriteByte(h2); err != nil {
		return errors.WithStack(err)
	}
	// write first
	if _, err := out.Write(first); err != nil {
		return errors.WithStack(err)
	}
	// write delta base
	db := make([]byte, 8)
	n := binary.PutVarint(db, deltaBase)
	if _, err := out.Write(db[:n]); err != nil {
		return errors.WithStack(err)
	}
	// write deltas
	for c := 0; c < len(deltas); {
		var v byte
		switch width {
		case 2:
			for j := 3; j >= 0 && c < len(deltas); j-- {
				v |= byte(deltas[c]&0x03) << byte(j*2)
				c++
			}
			if err := out.WriteByte(v); err != nil {
				return errors.WithStack(err)
			}
		case 4:
			v |= byte(deltas[c]&0x0f) << 4
			c++
			if c < len(deltas) {
				v |= byte(deltas[c] & 0x0f)
				c++
			}
			if err := out.WriteByte(v); err != nil {
				return errors.WithStack(err)
			}
		case 8:
			v = byte(deltas[c] & 0xff)
			c++
			if err := out.WriteByte(v); err != nil {
				return errors.WithStack(err)
			}
		case 16:
			for j := 1; j >= 0; j-- {
				v = byte(deltas[c] >> byte(j*8) & 0xff)
				if err := out.WriteByte(v); err != nil {
					return errors.WithStack(err)
				}
			}
			c++
		case 24:
			for j := 2; j >= 0; j-- {
				v = byte(deltas[c] >> byte(j*8) & 0xff)
				if err := out.WriteByte(v); err != nil {
					return errors.WithStack(err)
				}
			}
			c++
		case 32:
			for j := 3; j >= 0; j-- {
				v = byte(deltas[c] >> byte(j*8) & 0xff)
				if err := out.WriteByte(v); err != nil {
					return errors.WithStack(err)
				}
			}
			c++
		case 40:
			for j := 4; j >= 0; j-- {
				v = byte(deltas[c] >> byte(j*8) & 0xff)
				if err := out.WriteByte(v); err != nil {
					return errors.WithStack(err)
				}
			}
			c++
		case 48:
			for j := 5; j >= 0; j-- {
				v = byte(deltas[c] >> byte(j*8) & 0xff)
				if err := out.WriteByte(v); err != nil {
					return errors.WithStack(err)
				}
			}
			c++
		case 56:
			for j := 6; j >= 0; j-- {
				v = byte(deltas[c] >> byte(j*8) & 0xff)
				if err := out.WriteByte(v); err != nil {
					return errors.WithStack(err)
				}
			}
			c++
		case 64:
			for j := 7; j >= 0; j-- {
				v = byte(deltas[c] >> byte(j*8) & 0xff)
				if err := out.WriteByte(v); err != nil {
					return errors.WithStack(err)
				}
			}
			c++
		default:
			return errors.Errorf("width %d not recognized", width)
		}
	}
	return nil
}

// return a uint64 bits width
func getBitsWidth(x uint64) (n byte) {
	for x != 0 {
		x = x >> 1
		n++
	}
	return
}

// Get bits width of x, align to width encoding table
func getAlignedWidth(x uint64) byte {
	var n byte
	for x != 0 {
		x = x >> 1
		n++
	}

	if n > 2 && n <= 4 {
		n = 4
	} else if n > 4 && n <= 8 {
		n = 8
	} else if n > 8 && n <= 16 {
		if n == 12 { // just for patch testing
			return n
		}
		n = 16
	} else if n > 16 && n <= 24 {
		n = 24
	} else if n > 24 && n <= 32 {
		n = 32
	} else if n > 32 && n <= 40 {
		n = 40
	} else if n > 40 && n <= 48 {
		n = 48
	} else if n > 48 && n <= 56 {
		n = 56
	} else if n > 56 && n <= 64 {
		n = 64
	}

	return n
}

// get repeated count from position i, min return is 1 means no repeat
// max return is 512 for fixed delta
func (rle *intRleV2) getRepeat(i int) (count uint16) {
	count = 1
	for j := 1; j < 512 && i+j < rle.len(); j++ {
		if rle.signed {
			if rle.literals[i] == rle.literals[i+j] {
				count = uint16(j + 1)
			} else {
				break
			}
		} else {
			if rle.uliterals[i] == rle.uliterals[i+j] {
				count = uint16(j + 1)
			} else {
				break
			}
		}
	}
	return
}

func (rle *intRleV2) writeShortRepeat(count uint16, x uint64, out *bytes.Buffer) error {
	header := byte(count & 0x07)         // count
	header |= Encoding_SHORT_REPEAT << 6 // encoding
	var w byte
	bb := bytes.NewBuffer(make([]byte, 8))
	bb.Reset()
	for j := 7; j >= 0; j-- { //
		b := byte(x >> uint(8*j))
		if b != 0x00 {
			if byte(j) > w { // [1, 8]
				w = byte(j)
			}
			if err := bb.WriteByte(b); err != nil {
				return errors.WithStack(err)
			}
		}
	}
	header |= w << 3 // W
	if err := out.WriteByte(header); err != nil {
		return errors.WithStack(err)
	}
	if _, err := bb.WriteTo(out); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (rle *intRleV2) addValue(positive bool, delta uint64) {
	if rle.signed {
		var x int64
		if positive {
			x = rle.literals[len(rle.literals)-1] + int64(delta)
		} else {
			// fixme: if delta is 64 bit?
			x = rle.literals[len(rle.literals)-1] - int64(delta)
		}
		rle.literals = append(rle.literals, x)
	} else {
		var x uint64
		if positive {
			x = rle.uliterals[len(rle.uliterals)-1] + delta
		} else {
			x = rle.uliterals[len(rle.uliterals)-1] - delta
		}
		rle.uliterals = append(rle.uliterals, x)
	}
}

type base128VarInt struct {
	decoder
	values []int64
}

func (enc *base128VarInt) writeValues(out *bytes.Buffer) error {
	bb := make([]byte, 10)
	for _, v := range enc.values {
		c := binary.PutVarint(bb, v)
		if _, err := out.Write(bb[:c]); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (dec *base128VarInt) len() int {
	return len(dec.values)
}

func (dec *base128VarInt) readValues(in *bytes.Buffer) error {
	for in.Len()>0 {
		v, err:= binary.ReadVarint(in)
		if err!=nil {
			return errors.WithStack(err)
		}
		dec.values= append(dec.values, v)
	}
	return nil
}

func (dec *base128VarInt) reset()  {
	dec.values=dec.values[:0]
	dec.decoder.reset()
}

func widthEncoding(width byte) (w byte, err error) {
	if 2 <= width && width <= 21 {
		w = width - 1
		if (3 == width) || (5 <= width && width <= 7) || (9 <= width && width <= 15) || (17 <= width && width <= 21) {
			log.Warnf("width %d is deprecated", width)
		}
		return
	}
	if 26 == width {
		log.Warnf("width %d is deprecated", width)
		return 24, nil
	}
	if 28 == width {
		log.Warnf("width %d is deprecated", width)
		return 25, nil
	}
	if 30 == width {
		log.Warnf("width %d is deprecated", width)
		return 26, nil
	}

	switch width {
	case 0:
		w = 0
	case 1:
		w = 0
	case 2:
		w = 1
	case 4:
		w = 3
	case 8:
		w = 7
	case 16:
		w = 15
	case 24:
		w = 23
	case 32:
		w = 27
	case 40:
		w = 28
	case 48:
		w = 29
	case 56:
		w = 30
	case 64:
		w = 31
	default:
		// fixme: return 0
		return 0, errors.Errorf("width %d error", width)
	}
	return
}

func widthDecoding(w byte, delta bool) (width byte, err error) {
	if 2 <= w && w <= 20 {
		if 2 == w || (4 <= w && w <= 6) || (8 <= w && w <= 14) || (16 <= w && w <= 20) {
			log.Warnf("decoding: width code %d is deprecated", w)
		}
		width = w + 1
		return
	}
	if 24 == w {
		log.Warnf("decoding: width code %d is deprecated", w)
		return 26, nil
	}
	if 25 == w {
		log.Warnf("decoding: width code %d is deprecated", w)
		return 28, nil
	}
	if 26 == w {
		log.Warnf("decoding: width code %d is deprecated", w)
		return 30, nil
	}

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
		// should not reach
		return 0, errors.Errorf("run length integer v2 width(W) %d error", width)
	}
	return
}

func unZigzag(x uint64) int64 {
	return int64(x>>1) ^ -int64(x & 1)
}

func zigzag(x int64) uint64 {
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


func encodingNano(nanos uint64) (encoded uint64) {
	if nanos == 0 {
		return 0
	} else if nanos%100 != 0 {
		return uint64(nanos) << 3 // no encoding if less 2 zeros
	} else {
		nanos /= 100
		trailingZeros := 1
		for nanos%10 == 0 && trailingZeros < 7 { // 3 bits
			nanos /= 10
			trailingZeros++
		}
		return nanos<<3 | uint64(trailingZeros)
	}
}

func decodingNano(encoded uint64) (nano uint) {
	zeros := 0x07 & encoded
	nano = uint(encoded >> 3)
	if zeros != 0 {
		for i := 0; i <= int(zeros); i++ {
			nano *= 10
		}
	}
	return
}
