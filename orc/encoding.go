package orc

import (
	"bytes"
	"encoding/binary"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
	"math"
)

const (
	MIN_REPEAT_SIZE  = 3
	MAX_LITERAL_SIZE = 128

	Encoding_SHORT_REPEAT = byte(0)
	Encoding_DIRECT       = byte(1)
	Encoding_PATCHED_BASE = byte(2)
	Encoding_DELTA        = byte(3)
	Encoding_UNSET        = byte(255)
)

type Decoder interface {
	readValues(in *bytes.Buffer) error
	reset()
}

type Encoder interface {
	writeValues(out *bytes.Buffer) error
}

type byteRunLength struct {
	literals    []byte
	numLiterals int
}

func (brl *byteRunLength) readValues(in *bytes.Buffer) error {
	for in.Len() > 0 {
		control, err := in.ReadByte()
		if err != nil {
			return errors.WithStack(err)
		}
		if control < 0x80 { // run
			mark := brl.numLiterals
			brl.numLiterals += int(control) + MIN_REPEAT_SIZE
			ls := make([]byte, brl.numLiterals)
			copy(ls, brl.literals)
			brl.literals = ls
			val, err := in.ReadByte()
			if err != nil {
				return errors.WithStack(err)
			}
			for i := 0; i < int(control); i++ {
				brl.literals[mark+i] = val
			}
		} else { // literals
			mark := brl.numLiterals
			//brl.numLiterals += 0x100 - int(control)
			brl.numLiterals += int(-int8(control))
			ls := make([]byte, brl.numLiterals)
			copy(ls, brl.literals)
			brl.literals = ls
			if _, err = io.ReadFull(in, brl.literals[mark:brl.numLiterals]); err != nil {
				return errors.WithStack(err)
			}
		}
	}
	return nil
}

func (brl *byteRunLength) reset() {
	brl.numLiterals = 0
}

type intRunLengthV1 struct {
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
}

// direct v2 for string/char/varchar
type bytesDirectV2 struct {
	consumeIndex int
	content      [][]byte // utf-8 bytes
	length       []uint64
	pos          int
}

func (bd *bytesDirectV2) reset() {
	bd.content = bd.content[:0]
	bd.length = nil
	bd.pos = 0
	bd.consumeIndex = 0
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

// int run length v2
type intRleV2 struct {
	sub          byte // sub encoding
	signed       bool
	literals     []int64
	uliterals    []uint64
	numLiterals  int
	consumeIndex int
}

func (rle *intRleV2) reset() {
	if rle.signed {
		rle.literals = rle.literals[:0]
	} else {
		rle.uliterals = rle.uliterals[:0]
	}
	rle.signed = false
	rle.sub = Encoding_UNSET
	rle.consumeIndex = 0
	rle.numLiterals = 0
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
			rle.numLiterals += repeatCount
			log.Tracef("decoding: irl v2 reading short-repeat of count %d, and numliteral now is %d",
				repeatCount, rle.numLiterals)

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
					rle.literals = append(rle.literals, DecodeZigzag(x))
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
			rle.numLiterals += length
			log.Tracef("decoding: irl v2 direct get width %d of L %d now have literals number %d",
				width, length, rle.numLiterals)

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
							rle.literals = append(rle.literals, DecodeZigzag(x))
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
							rle.literals = append(rle.literals, DecodeZigzag(x))
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
						rle.literals = append(rle.literals, DecodeZigzag(x))
					} else {
						rle.uliterals = append(rle.uliterals, x)
					}
					i++
					if i < length {
						x := uint64(b & 0x0f)
						if rle.signed {
							rle.literals = append(rle.literals, DecodeZigzag(x))
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
						rle.literals = append(rle.literals, DecodeZigzag(x))
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
						rle.literals = append(rle.literals, DecodeZigzag(x))
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
						rle.literals = append(rle.literals, DecodeZigzag(x))
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
						rle.literals = append(rle.literals, DecodeZigzag(x))
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
						rle.literals = append(rle.literals, DecodeZigzag(x))
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
						rle.literals = append(rle.literals, DecodeZigzag(x))
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
						rle.literals = append(rle.literals, DecodeZigzag(x))
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
						rle.literals = append(rle.literals, DecodeZigzag(x))
					} else {
						rle.uliterals = append(rle.uliterals, x)
					}
					i++
				default:
					return errors.Errorf("int run length encoding direct width %d not supported", width)
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
			pll := header[3] & 0x1f              // 5bits patch list length, value 0 to 31

			bwbs := make([]byte, bw)
			if _, err = io.ReadFull(in, bwbs); err != nil {
				return errors.WithStack(err)
			}
			// base value big endian with msb of negative mark
			var base int64
			for i := 0; i < len(bwbs); i++ {
				base |= int64(bwbs[i]) << byte(len(bwbs)-1) * 8
			}
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
				// pgw 1 to 8 bits
				pg := b >> (8 - pgw)
				// pw 1 to 64 bits according to encoding table
				pbn := int(math.Ceil((float64(pgw) + float64(pw)) / float64(8))) // patch bytes number
				pbs := make([]byte, pbn)
				pbs[0] = b
				if pbn > 1 { // need more bytes
					if _, err := io.ReadFull(in, pbs[1:]); err != nil {
						return errors.WithStack(err)
					}
				}
				pv = uint64(pbs[0]<<pgw) >> pgw // remove pgw
				for j := 1; j < pbn; j++ {
					pv |= uint64(pbs[j]) << uint(pbn-j) * 8
				}
				// pv should be at largest 63 bits?
				rle.literals[mark+int(pg)] += int64(pv << w)
			}

			return errors.New("int rle patched base not impl")

		case Encoding_DELTA:
			// header: 2 bytes, base value: varint, delta base: signed varint
			header := make([]byte, 2)
			header[0] = firstByte
			header[1], err = in.ReadByte()
			if err != nil {
				return errors.WithStack(err)
			}
			deltaWidth, err := widthDecoding(header[0]>>1&0x1f, true)
			if err != nil {
				return errors.WithStack(err)
			}
			length := int(header[0])&0x01<<8 | int(header[1]) + 1
			rle.numLiterals += length

			log.Tracef("decoding: irl v2 delta length %d of width %d delta values, now have numLiterals %d",
				length, deltaWidth, rle.numLiterals)

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
				switch deltaWidth {
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
					return errors.Errorf("int rle v2 encoding delta width %d deprecated", deltaWidth)
				}
			}

		default:
			return errors.Errorf("sub encoding %d for int rle v2 not recognized", rle.sub)
		}

	} // finish read buffer

	return nil
}

// write all literals to buffer
func (rle *intRleV2) writeValues(out *bytes.Buffer) error {
	for i := 0; i < rle.numLiterals; {
		c := rle.getRepeat(i)
		if c >= 3 {
			if c <= 10 { // short repeat
				rle.sub = Encoding_SHORT_REPEAT
				var x uint64
				if rle.signed {
					log.Tracef("encoding: irl v2 write short repeat count %d of %d at index %d",
						c, rle.literals[i], i)
					x = EncodeZigzag(rle.literals[i])
				} else {
					log.Tracef("encoding: irl v2 write short repeat count %d of %d at index %d",
						c, rle.uliterals[i], i)
					x = rle.uliterals[i]
				}
				i += int(c)
				if err := rle.writeShortRepeat(c-3, x, out); err != nil {
					return errors.WithStack(err)
				}
			} else { // fixed delta 0
				rle.sub = Encoding_DELTA
				b := make([]byte, 8)
				var n int
				if rle.signed {
					n = binary.PutVarint(b, rle.literals[i])
				} else {
					n = binary.PutUvarint(b, rle.uliterals[i])
				}
				log.Tracef("encoding: irl v2 writing fixed delta 0 count %d at index %d", c, i)
				i += int(c)
				if err := rle.writeDelta(b[:n], 0, c, []uint64{}, out); err != nil {
					return errors.WithStack(err)
				}
			}
		} else {
			var dv deltaValues
			if rle.tryDeltaEncoding(i, &dv) {
				b := make([]byte, 8)
				var n int
				if rle.signed {
					n = binary.PutVarint(b, rle.literals[i])
				} else {
					n = binary.PutUvarint(b, rle.uliterals[i])
				}
				i += int(dv.length)
				log.Tracef("encoding: irl v2 writing delta count %d, index is %d", dv.length, i)
				if err := rle.writeDelta(b[:n], dv.base, dv.length, dv.deltas, out); err != nil {
					return errors.WithStack(err)
				}
			} else {
				if rle.tryDirectEncoding(i) {

				} else if rle.tryPatchedBase(i) {

				} else {
					return errors.New("no encoding tried")
				}
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
func (rle *intRleV2) tryDeltaEncoding(idx int, dv *deltaValues) bool {
	if idx+2 < rle.numLiterals {
		if rle.signed {
			if rle.literals[idx] == rle.literals[idx+1] { // can not be same for first 2
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

		for i := idx + 3; i < rle.numLiterals && dv.length < 512; i++ {
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

func (rle *intRleV2) tryPatchedBase(idx int) bool {
	return false
}

// first is int64 or uint64 based on signed, length is 9 bit for run length 1 to 512
func (rle *intRleV2) writeDelta(first []byte, deltaBase int64, length uint16, deltas []uint64, out *bytes.Buffer) error {
	var h1, h2 byte   // 2 byte header
	h1 = rle.sub << 6 // 2 bit encoding type
	// delta width
	var w, width byte
	for _, v := range deltas {
		bits := getWidth(v, true)
		if bits > width {
			width = bits
		}
	}
	w, err := widthEncoding(width)
	if err != nil {
		return errors.WithStack(err)
	}
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
		/*case 1:
		for j := byte(7); j > 0 && c < len(deltas); j-- {
			v |= byte(deltas[c]&0x01) << j
			c++
		}
		if err := out.WriteByte(v); err != nil {
			return errors.WithStack(err)
		}*/
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

func getWidth(x uint64, delta bool) byte {
	var n byte
	for x != 0 {
		x = x >> 1
		n++
	}

	if delta && n == 1 {
		n = 2
	} else if n > 2 && n <= 4 {
		n = 4
	} else if n > 4 && n <= 8 {
		n = 8
	} else if n > 8 && n <= 16 {
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
	for j := 1; j < 512 && i+j < int(rle.numLiterals); j++ {
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

func widthEncoding(width byte) (w byte, err error) {
	if width == 1 || width == 0 {
		return 0, nil
	}
	if 2 <= width && width <= 21 {
		w = width - 1
		if (3 == width) || (5 <= width && width <= 7) || (9 <= width && width <= 15) || (17 <= width && width <= 21) {
			log.Warnf("width %d is deprecated", width)
		}
		return
	}
	if 26==width {
		
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
