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

	Encoding_SHORT_REPEAT byte = 0
	Encoding_DIRECT            = 1
	Encoding_PATCHED_BASE      = 2
	Encoding_DELTA             = 3
)

type Decoder interface {
	readValues(buffer *bytes.Buffer) error
	reset()
}

type Encoder interface {
	writeValues(buf *bytes.Buffer) error
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

type stringContentDecoder struct {
	num          int
	consumeIndex int
	content      [][]byte
	length       []uint64
	lengthMark   uint64
}

func (d *stringContentDecoder) reset() {
	d.num = 0
	d.consumeIndex = 0
}

func (d *stringContentDecoder) readValues(in *bytes.Buffer) error {
	for in.Len() > 0 {
		length := d.length[d.lengthMark]
		b := make([]byte, length)
		if _, err := io.ReadFull(in, b); err != nil {
			panic(err)
			return errors.WithStack(err)
		}
		// append?
		d.content = append(d.content, b)
		d.num++
		d.lengthMark++
	}
	return nil
}

// int run length encoding v2
type intRleV2 struct {
	sub          byte // sub encoding
	signed       bool
	literals     []int64 // fixme: allocate
	uliterals    []uint64
	numLiterals  int
	consumeIndex int
}

func (rle *intRleV2) reset() {
	rle.signed = false
	rle.sub = 0 // fixme
	rle.consumeIndex = 0
	rle.numLiterals = 0
}

// write all literals to buffer
func (rle *intRleV2) writeValues(out *bytes.Buffer) error {
	for i := 0; i < int(rle.numLiterals); {
		c := rle.getRepeat(i)
		if c >= 3 {
			if c <= 10 { // short repeat
				rle.sub = Encoding_SHORT_REPEAT
				var x uint64
				if rle.signed {
					x = EncodeZigzag(rle.literals[i])
				} else {
					x = rle.uliterals[i]
				}
				i += c
				if err := rle.writeShortRepeat(c-3, x, out); err != nil {
					return errors.WithStack(err)
				}
			} else { // fixed delta 0
				rle.sub = Encoding_DELTA
				var x uint64
				if rle.signed {
					x = EncodeZigzag(rle.literals[i])
				} else {
					x = rle.uliterals[i]
				}
				i += c
				if err := rle.writeDelta(x, 0, c-2, []uint64{}, out); err != nil {
					return errors.WithStack(err)
				}
			}
		} else {
			var dv *deltaValues
			if rle.tryDeltaEncoding(i, dv) {
				var x uint64
				if rle.signed {
					x = EncodeZigzag(rle.literals[i])
				} else {
					x = rle.uliterals[i]
				}
				i += dv.length
				if err := rle.writeDelta(x, dv.base, dv.length-2, dv.deltas, out); err != nil {
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

func (rle *intRleV2) tryDirectEncoding(idx int) bool {
	return false
}

func (rle *intRleV2) tryPatchedBase(idx int) bool {
	return false
}

type deltaValues struct {
	base   int64
	length int
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
				if rle.literals[idx+2] > rle.literals[idx+1] {
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
				if rle.uliterals[idx+2] > rle.uliterals[idx+1] {
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

		for i := idx + 3; i < rle.numLiterals; i++ {
			if rle.signed {
				if dv.base > 0 {
					if rle.literals[i] > rle.literals[i-1] {
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
					if rle.uliterals[i] > rle.uliterals[i-1] {
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

func (rle *intRleV2) writeDelta(first uint64, deltaBase int64, length int, deltas []uint64, out *bytes.Buffer) error {
	var h1, h2 byte // 2 byte header
	h1 = rle.sub << 6
	// delta width
	var w, width byte
	for _, v := range deltas {
		x := getWidth(v)
		if x > width {
			width = x
		}
	}
	w, err := widthTable(width, true)
	if err != nil {
		return errors.WithStack(err)
	}
	h1 |= w & 0x1F << 3  // 5 bit
	length -= 1 // [1, 512] and 9 bit
	h1 |= byte(length >> 8) & 0x01 // 9th bit
	h2 = byte(length & 0xFF)
	if err := out.WriteByte(h1); err != nil {
		return errors.WithStack(err)
	}
	if err := out.WriteByte(h2); err != nil {
		return errors.WithStack(err)
	}
	if 
	return nil
}

func getWidth(x uint64) byte {
	var width byte
	for j := byte(7); j > 0; j-- {
		if x>>j*8 != 0x00 {
			width = j + 1
			break
		}
	}
	return width
}

// get repeated count from position i, min return is 1 means no repeat
// max return is 512 for fixed delta
func (rle *intRleV2) getRepeat(i int) (count int) {
	count = 1
	for j := 1; j <= 512; j++ {
		if i+j >= int(rle.numLiterals) {
			break
		}
		if rle.signed {
			if rle.literals[i] == rle.literals[i+j] {
				count = j + 1
			} else {
				break
			}
		} else {
			if rle.uliterals[i] == rle.uliterals[i+j] {
				count = j + 1
			} else {
				break
			}
		}
	}
	return
}

func (rle *intRleV2) writeShortRepeat(count int, x uint64, out *bytes.Buffer) error {
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

// read buffer all to literals
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
			width := 1 + (header>>3)&0x07 // 3bit([3,)6) width
			repeatCount := int(3 + (header & 0x07))
			mark := rle.numLiterals
			rle.numLiterals += repeatCount
			if rle.signed {
				ls := make([]int64, rle.numLiterals)
				copy(ls, rle.literals)
				rle.literals = ls
			} else {
				ls := make([]uint64, rle.numLiterals)
				copy(ls, rle.uliterals)
				rle.uliterals = ls
			}

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
					rle.literals[mark+i] = DecodeZigzag(x)
				}
			} else {
				for i := 0; i < repeatCount; i++ {
					rle.uliterals[mark+i] = x
				}
			}

		case Encoding_DIRECT:
			b1, err := in.ReadByte()
			if err != nil {
				return errors.WithStack(err)
			}
			header := uint16(firstByte)<<8 | uint16(b1) // 2 byte header
			w := (header >> 9) & 0x1F                   // 5bit([3,8)) width
			width, err := widthTable(byte(w), false)
			if err != nil {
				return errors.WithStack(err)
			}
			length := header&0x1FF + 1
			mark := rle.numLiterals
			rle.numLiterals += int(length)
			if rle.signed {
				ls := make([]int64, rle.numLiterals)
				copy(ls, rle.literals)
				rle.literals = ls
			} else {
				ls := make([]uint64, rle.numLiterals)
				copy(ls, rle.uliterals)
				rle.uliterals = ls
			}

			for i := 0; i < int(length); i++ {
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
					rle.literals[mark+i] = DecodeZigzag(x)
				} else {
					rle.uliterals[mark+i] = x
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
			deltaWidth, err := widthTable(header[0]>>1&0x1f, true)
			if err != nil {
				return errors.WithStack(err)
			}
			length := int(header[0])&0x01<<8 | int(header[1]) + 1

			mark := rle.numLiterals
			rle.numLiterals += length

			// rethink: oom?
			// fixme: allocate every time ?
			if rle.signed {
				ls := make([]int64, rle.numLiterals)
				copy(ls, rle.literals)
				rle.literals = ls
			} else {
				ls := make([]uint64, rle.numLiterals)
				copy(ls, rle.uliterals)
				rle.uliterals = ls
			}

			if rle.signed {
				baseValue, err := binary.ReadVarint(in)
				if err != nil {
					return errors.WithStack(err)
				}
				rle.literals[mark] = baseValue
			} else {
				baseValue, err := binary.ReadUvarint(in)
				if err != nil {
					return errors.WithStack(err)
				}
				rle.uliterals[mark] = baseValue
			}
			deltaBase, err := binary.ReadVarint(in)
			if err != nil {
				return errors.WithStack(err)
			}

			if deltaBase > 0 {
				rle.setValue(mark+1, true, uint64(deltaBase))
			} else {
				rle.setValue(mark+1, false, uint64(-deltaBase))
			}

			// delta values: W * (L-2)
			i := 2
			for i < length {
				switch deltaWidth {
				case 0:
					// fix delta based on delta base
					if deltaBase > 0 {
						rle.setValue(mark+i, true, uint64(deltaBase))
					} else {
						rle.setValue(mark+i, false, uint64(-deltaBase))
					}
					i++

				case 2:
					b, err := in.ReadByte()
					if err != nil {
						return errors.WithStack(err)
					}
					for j := 0; j < 4; j++ {
						delta := uint64(b) >> byte(3-j) * 2 & 0x03
						if i+j <= length {
							rle.setValue(mark+i+j, deltaBase > 0, delta)
						}
					}
					i += 4

				case 4:
					b, err := in.ReadByte()
					if err != nil {
						return errors.WithStack(err)
					}
					delta := uint64(b) >> 4
					rle.setValue(mark+i, deltaBase > 0, delta)
					i++
					if i < length {
						delta = uint64(b) & 0x0f
						rle.setValue(mark+i, deltaBase > 0, delta)
						i++
					}

				case 8:
					delta, err := in.ReadByte()
					if err != nil {
						return errors.WithStack(err)
					}
					pos := mark + i
					if rle.signed {
						if deltaBase > 0 {
							rle.literals[pos] = rle.literals[pos-1] + int64(delta)
						} else {
							rle.literals[pos] = rle.literals[pos-1] - int64(delta)
						}
					} else {
						if deltaBase > 0 {
							rle.uliterals[pos] = rle.uliterals[pos-1] + uint64(delta)
						} else {
							rle.uliterals[pos] = rle.uliterals[pos-1] - uint64(delta)
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
					rle.setValue(mark+i, deltaBase > 0, delta)
					i++

				case 24:
					bs := make([]byte, 3)
					if _, err = io.ReadFull(in, bs); err != nil {
						return errors.WithStack(err)
					}
					delta := uint64(bs[0])<<16 | uint64(bs[1])<<8 | uint64(bs[2])
					rle.setValue(mark+i, deltaBase > 0, delta)
					i++

				case 32:
					bs := make([]byte, 4)
					if _, err = io.ReadFull(in, bs); err != nil {
						return errors.WithStack(err)
					}
					delta := uint64(bs[0])<<24 | uint64(bs[1])<<16 | uint64(bs[2])<<8 | uint64(bs[3])
					rle.setValue(mark+i, deltaBase > 0, delta)
					i++

				case 40:
					bs := make([]byte, 5)
					if _, err = io.ReadFull(in, bs); err != nil {
						return errors.WithStack(err)
					}
					delta := uint64(bs[0])<<32 | uint64(bs[1])<<24 | uint64(bs[2])<<16 | uint64(bs[3])<<8 |
						uint64(bs[4])
					rle.setValue(mark+i, deltaBase > 0, delta)
					i++

				case 48:
					bs := make([]byte, 6)
					if _, err = io.ReadFull(in, bs); err != nil {
						return errors.WithStack(err)
					}
					delta := uint64(bs[0])<<40 | uint64(bs[1])<<32 | uint64(bs[2])<<24 | uint64(bs[3])<<16 |
						uint64(bs[4])<<8 | uint64(bs[5])
					rle.setValue(mark+i, deltaBase > 0, delta)
					i++

				case 56:
					bs := make([]byte, 7)
					if _, err = io.ReadFull(in, bs); err != nil {
						return errors.WithStack(err)
					}
					delta := uint64(bs[0])<<48 | uint64(bs[1])<<40 | uint64(bs[2])<<32 | uint64(bs[3])<<24 |
						uint64(bs[4])<<16 | uint64(bs[5])<<8 | uint64(bs[6])
					rle.setValue(mark+i, deltaBase > 0, delta)
					i++

				case 64:
					bs := make([]byte, 8)
					if _, err = io.ReadFull(in, bs); err != nil {
						return errors.WithStack(err)
					}
					// fixme: cast to uint64
					delta := uint64(bs[0])<<56 | uint64(bs[1])<<48 | uint64(bs[2])<<40 | uint64(bs[3])<<32 |
						uint64(bs[4])<<24 | uint64(bs[5])<<16 | uint64(bs[6])<<8 | uint64(bs[7])
					rle.setValue(mark+i, deltaBase > 0, delta)
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

func (rle *intRleV2) setValue(i int, positive bool, delta uint64) {
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

func widthTable(width byte, delta bool) (w byte, err error) {
	switch width {
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
