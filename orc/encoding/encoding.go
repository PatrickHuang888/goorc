package encoding

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
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

	BYTES = 8
)

type Decoder interface {
	ReadValues(in BufferedReader) error
	Reset()
	Remaining() int
	Pack()
	NextValue() (v interface{})
}

type Encoder interface {
	WriteValues(out *bytes.Buffer) error
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

func (brl *byteRunLength) ReadValues(in BufferedReader) error {
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
	return nil
}

func (brl *byteRunLength) Reset() {
	brl.literals = brl.literals[:0]
	brl.decoder.reset()
}

func (brl *byteRunLength) Remaining() int {
	return len(brl.literals) - brl.consumedIndex
}

func (brl *byteRunLength) Pack() {
	brl.literals = brl.literals[brl.consumedIndex:len(brl.literals):cap(brl.literals)]
	brl.reset()
}

func (brl *byteRunLength) NextValue() (v interface{}) {
	v = brl.literals[brl.consumedIndex]
	brl.consumedIndex++
	return
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

// bool run length use byte run length encoding, but how to know the length of bools?
type boolRunLength struct {
	byteRunLength
}

func (brl *boolRunLength) NextValue() (v interface{}) {
	pos := brl.consumedIndex / 8
	off := brl.consumedIndex % 8
	b := brl.literals[pos]
	v = b>>byte(7-off) == 0x01
	return
}

func (brl *boolRunLength) WriteValues(out *bytes.Buffer) error {
	var bs []byte
	// todo:
	/*for i := 0; i < len(brl.bools); {
		j := 0
		var b byte
		for ; j <= 7 && (i+j) < len(brl.bools); j++ {
			if brl.bools[i+j] {
				b |= 0x01 << byte(7-j)
			}
		}
		bs = append(bs, b)
		i += j
	}*/
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

// string contents, decoding need length decoder
type bytesContentV2 struct {
	decoder
	content       [][]byte // utf-8 bytes
	lengthDecoder Decoder
	pos           int
}

// length and pos cannot be reset, used for next readValues
func (bd *bytesContentV2) Reset() {
	bd.content = bd.content[:0]
	bd.lengthDecoder = nil
	//bd.pos = 0
	bd.reset()
}

func (bd *bytesContentV2) Remaining() int {
	return len(bd.content) - bd.consumedIndex
}

func (bd *bytesContentV2) Pack() {
	bd.content = bd.content[bd.consumedIndex:len(bd.content):cap(bd.content)]
	bd.reset()
}

// decode bytes, but should have extracted length stream first by rle v2 as length field
func (bd *bytesContentV2) ReadValues(in BufferedReader) error {
	length := bd.lengthDecoder.NextValue().(uint64)
	b := make([]byte, length)
	// optimize: slice copy
	if _, err := io.ReadFull(in, b); err != nil {
		return errors.WithStack(err)
	}
	bd.content = append(bd.content, b)
	bd.pos++
	return nil
}

func (bd *bytesContent) NextValue() (v interface{}) {
	v = bd.content[bd.consumedIndex]
	bd.consumedIndex++
	return
}

// write out content do not base length field, just base on len of content
func (bd *bytesContent) WriteValues(out *bytes.Buffer) error {
	for _, c := range bd.content {
		if _, err := out.Write(c); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// int run length encoding v2
type intRleV2 struct {
	sub    byte // sub encoding
	signed bool
	//literals      []int64
	uliterals     []uint64
	consumedIndex int  // for read
	lastByte      byte // different align when using read/writer, used in r/w
	bitsLeft      int  // used in r/w
}

func (rle *intRleV2) Pack() {
	rle.uliterals = rle.uliterals[rle.consumedIndex:len(rle.uliterals):cap(rle.uliterals)]
	rle.consumedIndex = 0
}

func (rle *intRleV2) Reset() {
	rle.signed = false
	rle.uliterals = rle.uliterals[:0]
	rle.sub = Encoding_UNSET
	rle.consumedIndex = 0
	rle.lastByte = 0
	rle.bitsLeft = 0
}

// decoded remaining
func (rle *intRleV2) len() int {
	return len(rle.uliterals) - rle.consumedIndex
}

func (rle *intRleV2) nextInt64() (v int64, err error) {
	if !rle.signed {
		return 0, errors.New("signed error")
	}
	x, err := rle.nextUInt64()
	return unZigzag(x), err
}

func (rle *intRleV2) nextUInt64() (v uint64, err error) {
	if rle.consumedIndex == len(rle.uliterals) {
		return 0, errors.New("no more values")
	}
	v = rle.uliterals[rle.consumedIndex]
	rle.consumedIndex++
	return
}

// decoding buffer all to u/literals
func (rle *intRleV2) ReadValues(in BufferedReader) error {

	for {

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
				repeatCount, rle.len())

			var x uint64
			for i := width; i > 0; { // big endian
				i--
				b, err := in.ReadByte()
				if err != nil {
					return errors.WithStack(err)
				}
				x |= uint64(b) << (8 * i)
			}

			for i := 0; i < repeatCount; i++ {
				rle.uliterals = append(rle.uliterals, x)
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

			for i := 0; i < length; i++ {
				var x uint64
				x, err := rle.readBits(in, int(width))
				if err != nil {
					return err
				}

				rle.uliterals = append(rle.uliterals, x)
			}
			rle.forgetBits()

		case Encoding_PATCHED_BASE:
			// fixme: according to base value is a signed smallest value, patch should always signed?
			if !rle.signed {
				return errors.New("decoding: int rl v2 patch signed setting should not false")
			}

			if err = rle.readPatched(firstByte, in); err != nil {
				return err
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

			var ubase uint64
			var base int64
			if rle.signed {
				base, err = binary.ReadVarint(in)
				if err != nil {
					return errors.WithStack(err)
				}
				ubase = zigzag(base)
			} else {
				ubase, err = binary.ReadUvarint(in)
				if err != nil {
					return errors.WithStack(err)
				}
			}
			rle.uliterals = append(rle.uliterals, ubase)

			deltaBase, err := binary.ReadVarint(in)
			if err != nil {
				return errors.WithStack(err)
			}

			if rle.signed {
				rle.uliterals = append(rle.uliterals, zigzag(base+deltaBase))
			} else {
				if deltaBase >= 0 {
					rle.uliterals = append(rle.uliterals, ubase+uint64(deltaBase))
				} else {
					rle.uliterals = append(rle.uliterals, ubase-uint64(-deltaBase))
				}
			}

			// delta values: W * (L-2)
			for i := 2; i < length; i++ {
				if width == 0 { //fixed delta
					if rle.signed {
						rle.uliterals = append(rle.uliterals, zigzag(base+deltaBase))
					} else {
						if deltaBase >= 0 {
							rle.uliterals = append(rle.uliterals, ubase+uint64(deltaBase))
						} else {
							rle.uliterals = append(rle.uliterals, ubase-uint64(-deltaBase))
						}
					}
				} else {
					delta, err := rle.readBits(in, int(width))
					if err != nil {
						return err
					}
					if rle.signed {
						prev := unZigzag(rle.uliterals[len(rle.uliterals)-1])
						if deltaBase >= 0 {
							rle.uliterals = append(rle.uliterals, zigzag(prev+int64(delta)))
						} else {
							rle.uliterals = append(rle.uliterals, zigzag(prev-int64(delta)))
						}
					} else {
						prev := rle.uliterals[len(rle.uliterals)-1]
						if deltaBase >= 0 {
							rle.uliterals = append(rle.uliterals, prev+delta)
						} else {
							rle.uliterals = append(rle.uliterals, prev-delta)
						}
					}
				}
			}
			rle.forgetBits()

		default:
			return errors.Errorf("decoding: int rl v2 encoding sub %d not recognized", rle.sub)
		}

		if in.Len() == 0 {
			break
		}

	}

	return nil
}

func (rle *intRleV2) readPatched(firstByte byte, in BufferedReader) (err error) {
	/*var mark int
	if len(rle.literals) != 0 {
		mark = len(rle.literals)
	}*/
	mark := len(rle.uliterals)
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
		return err
	}
	length := uint16(header[0])&0x01<<8 | uint16(header[1]) + 1 // 9 bits length, value 1 to 512
	bw := uint16(header[2])>>5&0x07 + 1                         // 3 bits base value width(BW), value 1 to 8 bytes
	pw, err := widthDecoding(header[2]&0x1f, false)             // 5 bits patch width(PW), value on table
	if err != nil {
		return err
	}
	pgw := uint16(header[3])>>5&0x07 + 1 // 3 bits patch gap width(PGW), value 1 to 8 bits
	if (int(pw) + int(pgw)) >= 64 {
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

	// data values (W*L bits padded to the byte)
	// base is the smallest one, so data values all positive
	for i := 0; i < int(length); i++ {
		delta, err := rle.readBits(in, int(width))
		if err != nil {
			return err
		}
		rle.uliterals = append(rle.uliterals, zigzag(base+int64(delta)))
	}
	rle.forgetBits()

	// patched values, PGW+PW must < 64
	for i := 0; i < int(pll); i++ {
		bs, err := rle.readBits(in, int(pgw)+int(pw))
		if err != nil {
			return err
		}

		patchGap := int(bs >> pw)
		mark += patchGap

		patchMask := (uint64(1) << pw) - 1
		patchValue := bs & patchMask

		if patchGap == 255 && patchValue == 0 {
			mark += 255
			continue
		}

		// patchValue should be at largest 63 bits?
		v := unZigzag(rle.uliterals[mark])
		v -= base // remove added base first
		v |= int64(patchValue << width)
		v += base // add base back
		rle.uliterals[mark] = zigzag(v)
	}
	rle.forgetBits()

	return nil
}

func (rle *intRleV2) readBits(in io.ByteReader, bits int) (value uint64, err error) {
	hasBits := rle.bitsLeft
	data := uint64(rle.lastByte)
	for ; hasBits < bits; hasBits += 8 {
		b, err := in.ReadByte()
		if err != nil {
			return 0, errors.WithStack(err)
		}
		data <<= 8
		data |= uint64(b)
	}

	rle.bitsLeft = hasBits - bits
	value = data >> uint(rle.bitsLeft)
	//leadZeros := uint(8 - rle.bitsLeft)

	// clear leadZeros
	mask := (uint64(1) << rle.bitsLeft) - 1
	rle.lastByte = byte(data & mask)

	//rle.lastByte = (byte(data) << leadZeros) >> leadZeros
	return
}

func (rle *intRleV2) forgetBits() {
	rle.bitsLeft = 0
	rle.lastByte = 0
}

// all bits align to msb
func (rle *intRleV2) writeBits(out *bytes.Buffer, value uint64, bits int) (err error) {
	totalBits := rle.bitsLeft + bits
	if totalBits > 64 {
		return errors.New("write bits > 64")
	}

	data := (uint64(rle.lastByte) << bits) | value // last byte and value will not overlap

	for totalBits -= 8; totalBits > 0; totalBits -= 8 {
		b := byte(data >> totalBits)
		if err = out.WriteByte(b); err != nil {
			return errors.WithStack(err)
		}
	}
	totalBits += 8
	rle.bitsLeft = totalBits
	leadZeros := 8 - totalBits
	// reAssure: clear lead bits
	rle.lastByte = byte(data) << leadZeros >> leadZeros

	/*trailZeros := 8 - rle.bitsLeft
	move := trailZeros + 56 - bits
	assertx(move >= 0)
	v := value << uint(move)
	if rle.bitsLeft != 0 { // has msb byte
		v = (uint64(rle.lastByte) << 56) | v
	}


	writeCount := 0
	for ; totalBits > 0; totalBits -= 8 {
		b := byte(v >> uint(56-writeCount*8))
		if err = out.WriteByte(b); err != nil {
			return errors.WithStack(err)
		}
		writeCount++
	}
	totalBits += 8

	rle.lastByte = byte(v >> uint(56-writeCount*8))
	rle.bitsLeft = totalBits*/
	return
}

// write all literals to buffer
func (rle *intRleV2) writeValues(out *bytes.Buffer) error {

	for idx := 0; idx < rle.len(); {
		if (rle.len() - idx) <= MIN_REPEAT_SIZE {
			rle.sub = Encoding_DIRECT
			return rle.writeDirect(out, idx, rle.len()-idx, true)
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
						c, unZigzag(rle.uliterals[idx]), idx)
				} else {
					log.Tracef("encoding: irl v2 Short Repeat count %d, value %d at index %d",
						c, rle.uliterals[idx], idx)
				}
				x = rle.uliterals[idx]
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
				n = binary.PutVarint(b, unZigzag(rle.uliterals[idx]))
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
				n = binary.PutVarint(b, unZigzag(rle.uliterals[idx]))
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
			if err := rle.writeDirect(out, idx, scope, true); err != nil {
				return errors.WithStack(err)
			}
			idx += scope
			continue
		}

		return errors.New("encoding: no sub decided!")
	}

	return nil
}

// idx rle literals write start, length write length
func (rle *intRleV2) writeDirect(out *bytes.Buffer, idx int, length int, widthAlign bool) error {
	// refactor: reset these two fields
	rle.bitsLeft = 0
	rle.lastByte = 0

	if rle.sub != Encoding_DIRECT {
		return errors.New("encoding: int rl v2 sub error ")
	}
	header := make([]byte, 2)
	header[0] = Encoding_DIRECT << 6
	values := make([]uint64, length)
	var width int
	for i := 0; i < length; i++ {
		x := rle.uliterals[idx+i]
		var w int
		if widthAlign {
			w = getAlignedWidth(x)
		} else {
			w = getBitsWidth(x)
		}
		if w > width {
			width = w
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

	for i := 0; i < len(values); {
		var v byte
		j := 0
		switch width {
		case 1:
			for ; j < 8 && (i+j) < len(values); j++ {
				v |= byte(values[i+j]) & 0x1 << uint(7-j)
			}
			if err := out.WriteByte(v); err != nil {
				return errors.WithStack(err)
			}
			i += j
		case 2:
			for ; j < 4 && (i+j) < len(values); j++ {
				v |= byte(values[i+j]) & 0x3 << uint(6-j*2)
			}
			if err := out.WriteByte(v); err != nil {
				return errors.WithStack(err)
			}
			i += j

		case 4:
			for ; j < 2 && (i+j) < len(values); j++ {
				v |= byte(values[i+j] & 0xf << uint(4-j*4))
			}
			if err := out.WriteByte(v); err != nil {
				return errors.WithStack(err)
			}
			i += j
		case 8:
			v = byte(values[i])
			if err := out.WriteByte(v); err != nil {
				return errors.WithStack(err)
			}
			i++

		case 11:
			if err := rle.writeBits(out, values[i], 11); err != nil {
				return err
			}
			i++

		case 16:
			for ; j < 2; j++ {
				v = byte(values[i] >> uint((1-j)*8))
				if err := out.WriteByte(v); err != nil {
					return errors.WithStack(err)
				}
			}
			i++
		case 24:
			for ; j < 3; j++ {
				v = byte(values[i] >> uint((2-j)*8))
				if err := out.WriteByte(v); err != nil {
					return errors.WithStack(err)
				}
			}
			i++
		case 32:
			for ; j < 4; j++ {
				v = byte(values[i] >> uint((3-j)*8))
				if err := out.WriteByte(v); err != nil {
					return errors.WithStack(err)
				}
			}
			i++
		case 40:
			for ; j < 5; j++ {
				v = byte(values[i] >> uint((4-j)*8))
				if err := out.WriteByte(v); err != nil {
					return errors.WithStack(err)
				}
			}
			i++
		case 48:
			for ; j < 6; j++ {
				v = byte(values[i] >> uint((5-j)*8))
				if err := out.WriteByte(v); err != nil {
					return errors.WithStack(err)
				}
			}
			i++
		case 56:
			for ; j < 7; j++ {
				v = byte(values[i] >> uint((6-j)*8))
				if err := out.WriteByte(v); err != nil {
					return errors.WithStack(err)
				}
			}
			i++
		case 64:
			for ; j < 8; j++ {
				v = byte(values[i] >> uint((7-j)*8))
				if err := out.WriteByte(v); err != nil {
					return errors.WithStack(err)
				}
			}
			i++

		default:
			return errors.Errorf("encoding int rl v2 Direct width %d unknown", width)
		}
	}
	if rle.bitsLeft != 0 {
		b := rle.lastByte << (8 - rle.bitsLeft) // last align to msb
		if err := out.WriteByte(b); err != nil {
			return errors.WithStack(err)
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
			if rle.uliterals[idx] == rle.uliterals[idx+1] { // delta can not be same for first 2
				return false
			} else {
				dv.base = unZigzag(rle.uliterals[idx+1]) - unZigzag(rle.uliterals[idx])
			}

			if dv.base > 0 { // increasing
				if rle.uliterals[idx+2] >= rle.uliterals[idx+1] {
					dv.deltas = append(dv.deltas, uint64(rle.uliterals[idx+2]-rle.uliterals[idx+1]))
				} else {
					return false
				}
			} else {
				if rle.uliterals[idx+2] < rle.uliterals[idx+1] {
					dv.deltas = append(dv.deltas, uint64(rle.uliterals[idx+1]-rle.uliterals[idx+2]))
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
	w, err := widthEncoding(int(pvs.width))
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
	pw, err := widthEncoding(int(pvs.patchWidth))
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
		shiftW := byte(getBitsWidth(p)) - (8 - pvs.gapWidth)
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

	min := unZigzag(rle.uliterals[idx])
	var count int
	baseWidthHist := make([]byte, BITS_SLOTS) // slots for 0 to 64 bits widths
	// fixme: patch only apply until to 512?
	for i := 0; i < 512 && idx+i < rle.len(); i++ {
		var x uint64

		v := unZigzag(rle.uliterals[idx+i])
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
			values[i] = uint64(unZigzag(rle.uliterals[idx+i]) - min)
			valuesWidths[i] = byte(getAlignedWidth(values[i]))
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
				w := byte(getBitsWidth(uint64(pvs.gaps[i])))
				if pvs.gapWidth < w {
					pvs.gapWidth = w
				}
			}
			for i := 0; i < len(pvs.patches); i++ {
				w := getAlignedWidth(pvs.patches[i])
				if int(pvs.patchWidth) < w {
					pvs.patchWidth = byte(w)
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
		if bits > int(width) {
			width = byte(bits)
		}
	}
	if width == 1 { // delta width no 1?
		width = 2
	}
	//log.Tracef("encoding: irl v2 Delta length %d, width %d", length, width)
	w, err := widthEncoding(int(width))
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
func getBitsWidth(x uint64) (w int) {
	for x != 0 {
		x = x >> 1
		w++
	}
	return w
}

// Get bits width of x, align to width encoding table
func getAlignedWidth(x uint64) int {
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
			return int(n)
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

	return int(n)
}

// get repeated count from position i, min return is 1 means no repeat
// max return is 512 for fixed delta
func (rle *intRleV2) getRepeat(i int) (count uint16) {
	count = 1
	for j := 1; j < 512 && i+j < rle.len(); j++ {
		if rle.uliterals[i] == rle.uliterals[i+j] {
			count = uint16(j + 1)
		} else {
			break
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

func (dec *base128VarInt) readValues(in BufferedReader) error {
	for in.Len() > 0 {
		v, err := binary.ReadVarint(in)
		if err != nil {
			return errors.WithStack(err)
		}
		dec.values = append(dec.values, v)
	}
	return nil
}

func (dec *base128VarInt) reset() {
	dec.values = dec.values[:0]
	dec.decoder.reset()
}

type ieee754Double struct {
	decoder
	values []float64
}

func (dec *ieee754Double) readValues(in BufferedReader) error {
	for in.Len() > 0 {
		bb := make([]byte, 8)
		if _, err := io.ReadFull(in, bb); err != nil {
			return errors.WithStack(err)
		}
		//v := math.Float64frombits(binary.BigEndian.Uint64(bb))
		// !!!
		v := math.Float64frombits(binary.LittleEndian.Uint64(bb))
		dec.values = append(dec.values, v)
	}
	return nil
}

func (dec *ieee754Double) len() int {
	return len(dec.values)
}

func (dec *ieee754Double) reset() {
	dec.values = dec.values[:0]
	dec.decoder.reset()
}

func (enc *ieee754Double) writeValues(out *bytes.Buffer) error {
	bb := make([]byte, 8)
	for _, v := range enc.values {
		binary.BigEndian.PutUint64(bb, math.Float64bits(v))
		if _, err := out.Write(bb); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func widthEncoding(width int) (w byte, err error) {
	if 2 <= width && width <= 21 {
		w = byte(width - 1)
		if (3 == width) || (5 <= width && width <= 7) || (9 <= width && width <= 15) || (17 <= width && width <= 21) {
			log.Warnf("encoding: width %d is deprecated", width)
		}
		return
	}
	if 26 == width {
		log.Warnf("encoding: width %d is deprecated", width)
		return 24, nil
	}
	if 28 == width {
		log.Warnf("encoding: width %d is deprecated", width)
		return 25, nil
	}
	if 30 == width {
		log.Warnf("encoding: width %d is deprecated", width)
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
	if 2 <= w && w <= 23 {
		if 2 == w || (4 <= w && w <= 6) || (8 <= w && w <= 14) || (16 <= w && w <= 22) {
			log.Warnf("decoding: width encoded %d is deprecated", w)
		}
		width = w + 1
		return
	}
	if 24 == w {
		log.Warnf("decoding: width encoded  %d is deprecated", w)
		return 26, nil
	}
	if 25 == w {
		log.Warnf("decoding: width encoded  %d is deprecated", w)
		return 28, nil
	}
	if 26 == w {
		log.Warnf("decoding: width encoded  %d is deprecated", w)
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
		return 0, errors.Errorf("run length integer v2 width(W) %d error", w)
	}
	return
}

func unZigzag(x uint64) int64 {
	return int64(x>>1) ^ -int64(x&1)
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
