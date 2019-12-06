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

/*type Decoder interface {
	ReadValues(in BufferedReader, values interface{}) error
}

type Encoder interface {
	WriteValues(values interface{}, out *bytes.Buffer) error
}*/

/*type decoder struct {
	consumedIndex int
}

func (d *decoder) reset() {
	d.consumedIndex = 0
}
*/

type BufferedReader interface {
	io.ByteReader
	io.Reader
}

type ByteRunLength struct {
}

func (d *ByteRunLength) ReadValues(in BufferedReader, values []byte) ([]byte, error) {
	control, err := in.ReadByte()
	if err != nil {
		return values, err
	}
	if control < 0x80 { // run
		l := int(control) + MIN_REPEAT_SIZE
		v, err := in.ReadByte()
		if err != nil {
			return values, err
		}
		for i := 0; i < l; i++ {
			values = append(values, v)
		}
	} else { // literals
		l := int(-int8(control))
		for i := 0; i < l; i++ {
			v, err := in.ReadByte()
			if err != nil {
				return values, err
			}
			values = append(values, v)
		}
	}
	return values, nil
}

func (e *ByteRunLength) WriteValues(values []byte, out *bytes.Buffer) error {
	run := -1
	length := 0

	mark := 0
	for i := 0; i < len(values); {
		repeat:= findBytesRepeat(values[i:])
		if repeat<=3 {

		}

		/*// run
		if (i+2 < len(values)) && (values[i+1] == b && values[i+2] == b) {
			if mark != i { // write out length before run
				l := i - mark
				if err := out.WriteByte(byte(-int8(l))); err != nil {
					return errors.WithStack(err)
				}
				if _, err := out.Write(values[mark:i]); err != nil {
					return errors.WithStack(err)
				}
			}
			length = 3
			for ; i+length < len(values) && length < 130 && values[i+length] == b; length++ {
			}
			if err := out.WriteByte(byte(length - 3)); err != nil {
				return errors.WithStack(err)
			}
			if err := out.WriteByte(b); err != nil {
				return errors.WithStack(err)
			}
			mark = i + length
		}
		i += length*/
	}

	if mark < len(values) { // write left length
		l := len(values) - mark
		if err := out.WriteByte(byte(-int8(l))); err != nil {
			return errors.WithStack(err)
		}
		if _, err := out.Write(values[mark:len(values)]); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// bool run length use byte run length encoding, but how to know the length of bools?
type BoolRunLength struct {
}

/*func (brl *boolRunLength) NextValue() (v interface{}) {
	pos := brl.consumedIndex / 8
	off := brl.consumedIndex % 8
	b := brl.literals[pos]
	v = b>>byte(7-off) == 0x01
	return
}*/

func (e *BoolRunLength) WriteValues(values []bool, out *bytes.Buffer) (err error) {
	for i := 0; i < len(values); {
		var b byte
		for j := 0; j <= 7 && (i < len(values)); j++ {
			if values[i] {
				b |= 0x01 << byte(7-j)
			}
			i++
		}
		if err = out.WriteByte(b); err != nil {
			return errors.WithStack(err)
		}
	}
	return
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
/*type BytesContent struct {
	decoder
	content       [][]byte // utf-8 bytes
	lengthDecoder Decoder
	pos           int
}


// decode bytes, but should have extracted length stream first by rle v2 as length field
func (bd *BytesContent) ReadValues(in BufferedReader, values []byte) (err error) {
	if _, err= in.Read(values);err!=nil {
		return errors.WithStack(err)
	}
	return nil
}

func ()  {

}


// write out content do not base length field, just base on len of content
func (bd *bytesContent) WriteValues(out *bytes.Buffer) error {
	for _, c := range bd.content {
		if _, err := out.Write(c); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}*/

// int run length encoding v2
type IntRleV2 struct {
	sub      byte
	lastByte byte // different align when using read/writer, used in r/w
	bitsLeft int  // used in r/w
}

// decoding buffer all to u/literals
func (d *IntRleV2) ReadValues(in BufferedReader, signed bool, values []uint64) (err error) {
	// header from MSB to LSB
	firstByte, err := in.ReadByte()
	if err != nil {
		return errors.WithStack(err)
	}

	d.sub = firstByte >> 6
	switch d.sub {
	case Encoding_SHORT_REPEAT:
		header := firstByte
		width := 1 + (header>>3)&0x07 // width 3bit at position 3
		repeatCount := int(3 + (header & 0x07))
		log.Tracef("decoding: int rl v2 Short Repeat of count %d", repeatCount)

		var v uint64
		for i := width; i > 0; { // big endian
			i--
			b, err := in.ReadByte()
			if err != nil {
				return errors.WithStack(err)
			}
			v |= uint64(b) << (8 * i)
		}

		for i := 0; i < repeatCount; i++ {
			values = append(values, v)
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
		log.Tracef("decoding: int rl v2 Direct width %d length %d", width, length)

		for i := 0; i < length; i++ {
			v, err := d.readBits(in, int(width))
			if err != nil {
				return err
			}
			values = append(values, v)
		}
		d.forgetBits()

	case Encoding_PATCHED_BASE:
		// rethink: according to base value is a signed smallest value, patch should always signed?
		if !signed {
			return errors.New("decoding: int rl v2 patch signed setting should not false")
		}

		if err = d.readPatched(in, firstByte, values); err != nil {
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

		log.Tracef("decoding: irl v2 Delta length %d, width %d", length, width)

		var ubase uint64
		var base int64
		if signed {
			base, err = binary.ReadVarint(in)
			if err != nil {
				return errors.WithStack(err)
			}
			ubase = Zigzag(base)
		} else {
			ubase, err = binary.ReadUvarint(in)
			if err != nil {
				return errors.WithStack(err)
			}
		}
		values = append(values, ubase)

		deltaBase, err := binary.ReadVarint(in)
		if err != nil {
			return errors.WithStack(err)
		}

		if signed {
			values = append(values, Zigzag(base+deltaBase))
		} else {
			if deltaBase >= 0 {
				values = append(values, ubase+uint64(deltaBase))
			} else {
				values = append(values, ubase-uint64(-deltaBase))
			}
		}

		// delta values: W * (L-2)
		for i := 2; i < length; i++ {
			if width == 0 { //fixed delta
				if signed {
					values = append(values, Zigzag(base+deltaBase))
				} else {
					if deltaBase >= 0 {
						values = append(values, ubase+uint64(deltaBase))
					} else {
						values = append(values, ubase-uint64(-deltaBase))
					}
				}
			} else {
				delta, err := d.readBits(in, int(width))
				if err != nil {
					return err
				}
				if signed {
					prev := UnZigzag(values[len(values)-1])
					if deltaBase >= 0 {
						values = append(values, Zigzag(prev+int64(delta)))
					} else {
						values = append(values, Zigzag(prev-int64(delta)))
					}
				} else {
					prev := values[len(values)-1]
					if deltaBase >= 0 {
						values = append(values, prev+delta)
					} else {
						values = append(values, prev-delta)
					}
				}
			}
		}
		d.forgetBits()

	default:
		return errors.Errorf("decoding: int rl v2 encoding sub %d not recognized", d.sub)
	}

	return
}

func (d *IntRleV2) readPatched(in BufferedReader, firstByte byte, values []uint64) (err error) {
	mark := len(values)
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
		delta, err := d.readBits(in, int(width))
		if err != nil {
			return err
		}
		values = append(values, Zigzag(base+int64(delta)))
	}
	d.forgetBits()

	// patched values, PGW+PW must < 64
	for i := 0; i < int(pll); i++ {
		bs, err := d.readBits(in, int(pgw)+int(pw))
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
		v := UnZigzag(values[mark])
		v -= base // remove added base first
		v |= int64(patchValue << width)
		v += base // add base back
		values[mark] = Zigzag(v)
	}
	d.forgetBits()

	return nil
}

func (d *IntRleV2) readBits(in io.ByteReader, bits int) (value uint64, err error) {
	hasBits := d.bitsLeft
	data := uint64(d.lastByte)
	for ; hasBits < bits; hasBits += 8 {
		b, err := in.ReadByte()
		if err != nil {
			return 0, errors.WithStack(err)
		}
		data <<= 8
		data |= uint64(b)
	}

	d.bitsLeft = hasBits - bits
	value = data >> uint(d.bitsLeft)
	//leadZeros := uint(8 - rle.bitsLeft)

	// clear leadZeros
	mask := (uint64(1) << d.bitsLeft) - 1
	d.lastByte = byte(data & mask)

	//rle.lastByte = (byte(data) << leadZeros) >> leadZeros
	return
}

func (d *IntRleV2) forgetBits() {
	d.bitsLeft = 0
	d.lastByte = 0
}

// all bits align to msb
func (e *IntRleV2) writeBits(value uint64, bits int, out *bytes.Buffer) (err error) {
	totalBits := e.bitsLeft + bits
	if totalBits > 64 {
		return errors.New("write bits > 64")
	}

	data := (uint64(e.lastByte) << bits) | value // last byte and value will not overlap

	for totalBits -= 8; totalBits > 0; totalBits -= 8 {
		b := byte(data >> totalBits)
		if err = out.WriteByte(b); err != nil {
			return errors.WithStack(err)
		}
	}
	totalBits += 8
	e.bitsLeft = totalBits
	leadZeros := 8 - totalBits
	// reAssure: clear lead bits
	e.lastByte = byte(data) << leadZeros >> leadZeros

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

func (e *IntRleV2) WriteValues(values []uint64, signed bool, out *bytes.Buffer) (err error) {
	if len(values) <= MIN_REPEAT_SIZE {
		return e.writeDirect(values, true, out)
	}

	for i := 0; i < len(values); {

		// try repeat first
		repeat := getRepeat(values, i)
		if repeat >= 3 {
			// short repeat
			if repeat <= 10 {
				//e.sub = Encoding_SHORT_REPEAT
				var v uint64
				if signed {
					log.Tracef("encoding: irl v2 Short Repeat count %d, value %d at index %d",
						repeat, UnZigzag(values[i]), i)
				} else {
					log.Tracef("encoding: irl v2 Short Repeat count %d, value %d at index %d",
						repeat, values[i], i)
				}
				v = values[i]
				i += int(repeat)
				if err := e.writeShortRepeat(repeat-3, v, out); err != nil {
					return errors.WithStack(err)
				}
				continue
			}

			// fixed delta 0
			//e.sub = Encoding_DELTA
			b := make([]byte, 8)
			var n int
			if signed {
				n = binary.PutVarint(b, UnZigzag(values[i]))
			} else {
				n = binary.PutUvarint(b, values[i])
			}
			log.Tracef("encoding: irl v2 Fixed Delta 0 count %d at index %d", repeat, i)
			i += int(repeat)
			if err = e.writeDelta(b[:n], 0, repeat, []uint64{}, out); err != nil {
				return
			}
			continue
		}

		// delta, need width should be stable?
		dv := &deltaValues{}
		if e.tryDeltaEncoding(values[i:], signed, dv) {
			b := make([]byte, 8)
			var n int
			if signed {
				n = binary.PutVarint(b, UnZigzag(values[i]))
			} else {
				n = binary.PutUvarint(b, values[i])
			}
			i += int(dv.length)
			if err = e.writeDelta(b[:n], dv.base, dv.length, dv.deltas, out); err != nil {
				return
			}
			continue
		}

		var sub byte
		var pvs patchedValues
		if signed {
			// try patch
			sub, err = bitsWidthAnalyze(values[i:], &pvs)
			if err != nil {
				return err
			}
		} else {
			// fixme: make sure this
			sub = Encoding_DIRECT
		}

		// toAssure: encoding until MAX_SCOPE?
		var scope int
		l := len(values) - i
		if l >= MAX_SCOPE {
			scope = MAX_SCOPE
		} else {
			scope = l
		}

		if sub == Encoding_PATCHED_BASE {
			//e.sub = Encoding_PATCHED_BASE
			log.Tracef("encoding: int rl v2 Patch %s", pvs.toString())
			if err = e.writePatch(&pvs, out); err != nil {
				return
			}
			i += scope
			continue
		}

		if sub == Encoding_DIRECT {
			//e.sub = Encoding_DIRECT
			if err = e.writeDirect(values[i:i+scope], true, out); err != nil {
				return
			}
			i += scope
			continue
		}

		return errors.New("encoding: no sub decided!")
	}

	return
}

func (e *IntRleV2) writeDirect(values []uint64, widthAlign bool, out *bytes.Buffer) error {
	e.forgetBits()

	header := make([]byte, 2)
	header[0] = Encoding_DIRECT << 6

	var width int
	for _, v := range values {
		var w int
		if widthAlign {
			w = getAlignedWidth(v)
		} else {
			w = getBitsWidth(v)
		}
		if w > width {
			width = w
		}
	}

	encodedWidth, err := widthEncoding(width)
	if err != nil {
		return errors.WithStack(err)
	}
	header[0] |= (encodedWidth & 0x1f) << 1 // 5 bit W
	l := len(values)
	header[0] |= byte(l>>8) & 0x01
	header[1] = byte(l)
	log.Tracef("encoding: int rl v2 Direct width %d values %d ", width, l)
	if _, err := out.Write(header); err != nil {
		return errors.WithStack(err)
	}

	for _, v := range values {
		if err := e.writeBits(v, width, out); err != nil {
			return err
		}
	}

	// expand to byte
	if e.bitsLeft != 0 {
		b := e.lastByte << (8 - e.bitsLeft) // last align to msb
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
func (e *IntRleV2) tryDeltaEncoding(values []uint64, signed bool, dv *deltaValues) bool {
	if len(values) <= 2 {
		return false
	}

	if values[0] == values[1] {
		return false
	}

	if signed {
		dv.base = UnZigzag(values[1]) - UnZigzag(values[0])

		v2 := UnZigzag(values[2])
		v1 := UnZigzag(values[1])
		if dv.base > 0 {
			if v2 >= v1 {
				dv.deltas = append(dv.deltas, uint64(v2-v1))
			} else {
				return false
			}
		} else {
			if v2 >= v1 {
				return false
			} else {
				dv.deltas = append(dv.deltas, uint64(v1-v2))
			}
		}

	} else {
		if values[0] < values[1] {
			dv.base = int64(values[1] - values[0])
		} else {
			dv.base = -int64(values[0] - values[1])
		}

		if dv.base > 0 {
			if values[2] >= values[1] {
				dv.deltas = append(dv.deltas, values[2]-values[1])
			} else {
				return false
			}
		} else {
			if values[2] < values[1] {
				dv.deltas = append(dv.deltas, values[1]-values[2])
			} else {
				return false
			}
		}
	}

	dv.length = 3

	for i := 3; i < len(values) && dv.length < 512; i++ {
		if signed {
			vi := UnZigzag(values[i])
			vi_1 := UnZigzag(values[i-1])
			if dv.base >= 0 {
				if vi >= vi_1 {
					dv.deltas = append(dv.deltas, uint64(vi-vi_1))
				} else {
					break
				}
			} else {
				if vi < vi_1 {
					dv.deltas = append(dv.deltas, uint64(vi_1-vi))
				} else {
					break
				}
			}

		} else {
			if dv.base >= 0 {
				if values[i] >= values[i-1] {
					dv.deltas = append(dv.deltas, values[i]-values[i-1])
				} else {
					break
				}
			} else {
				if values[i] < values[i-1] {
					dv.deltas = append(dv.deltas, values[i-1]-values[i])
				} else {
					break
				}
			}
		}
		dv.length += 1
	}

	return true
}

// direct encoding when constant bit width, length 1 to 512
func (rle *IntRleV2) tryDirectEncoding(idx int) bool {
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

func (e *IntRleV2) writePatch(pvs *patchedValues, out *bytes.Buffer) error {
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
func bitsWidthAnalyze(values []uint64, pvs *patchedValues) (sub byte, err error) {
	min := UnZigzag(values[0])
	var count int
	baseWidthHist := make([]byte, BITS_SLOTS) // slots for 0 to 64 bits widths

	// fixme: patch only apply until to 512?
	for i := 1; i < 512 && i < len(values); i++ {
		v := UnZigzag(values[i])
		if v < min {
			min = v
		}
		// toAssure: using zigzag to decide bits width
		baseWidthHist[getAlignedWidth(values[i])] += 1
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
			values[i] = uint64(UnZigzag(values[i]) - min)
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
func (e *IntRleV2) writeDelta(first []byte, deltaBase int64, length uint16, deltas []uint64, out *bytes.Buffer) error {
	var h1, h2 byte          // 2 byte header
	h1 = Encoding_DELTA << 6 // 2 bit encoding type
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
func getRepeat(values []uint64, i int) (count uint16) {
	count = 1
	for j := 1; j < 512 && i+j < len(values); j++ {
		if values[i] == values[i+j] {
			count = uint16(j + 1)
		} else {
			break
		}
	}
	return
}

func findBytesRepeat(values []byte) (count int) {
	if values == nil {
		panic("values nil")
	}
	v := values[0]
	for i := 1; i < len(values); i++ {
		if values[i] == v {
			count++
		} else {
			return
		}
	}
	return
}

func (e *IntRleV2) writeShortRepeat(count uint16, x uint64, out *bytes.Buffer) error {
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

type Base128VarInt struct {
}

func (e *Base128VarInt) WriteValues(values []int64, out *bytes.Buffer) error {
	bb := make([]byte, 10)
	for _, v := range values {
		c := binary.PutVarint(bb, v)
		if _, err := out.Write(bb[:c]); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (d *Base128VarInt) ReadValues(in BufferedReader, values []int64) (err error) {
	v, err := binary.ReadVarint(in)
	if err != nil {
		return errors.WithStack(err)
	}
	values = append(values, v)
	return
}

type Ieee754Double struct {
}

func (d *Ieee754Double) ReadValues(in BufferedReader, values []float64) (err error) {
	bb := make([]byte, 8)
	if _, err := io.ReadFull(in, bb); err != nil {
		return errors.WithStack(err)
	}
	//v := math.Float64frombits(binary.BigEndian.Uint64(bb))
	// !!!
	v := math.Float64frombits(binary.LittleEndian.Uint64(bb))
	values = append(values, v)
	return
}

func (e *Ieee754Double) WriteValues(values []float64, out *bytes.Buffer) error {
	bb := make([]byte, 8)
	for _, v := range values {
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

func UnZigzag(x uint64) int64 {
	return int64(x>>1) ^ -int64(x&1)
}

func Zigzag(x int64) uint64 {
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
