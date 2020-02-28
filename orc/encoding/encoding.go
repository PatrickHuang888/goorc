package encoding

import (
	"bytes"
	"encoding/binary"
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

type Encoder interface {
	Encode(out *bytes.Buffer, values interface{}) error
}

type BufferedReader interface {
	io.ByteReader
	io.Reader
}

type ByteRunLength struct {
}

func (d *ByteRunLength) Decode(in io.ByteReader, values []byte) ([]byte, error) {
	control, err := in.ReadByte()
	if err != nil {
		return values, err
	}
	if control < 0x80 { // run
		l := int(control) + MIN_REPEAT_SIZE
		v, err := in.ReadByte()
		if err != nil {
			return values, errors.WithStack(err)
		}
		for i := 0; i < l; i++ {
			values = append(values, v)
		}
	} else { // literals
		l := int(-int8(control))
		for i := 0; i < l; i++ {
			v, err := in.ReadByte()
			if err != nil {
				return values, errors.WithStack(err)
			}
			values = append(values, v)
		}
	}
	return values, nil
}

func (e *ByteRunLength) Encode(out *bytes.Buffer, v interface{}) error {
	vs := v.([]byte)

	for i := 0; i < len(vs); {
		l := len(vs) - i
		if l > 128 { // max 128
			l = 128
		}

		values := vs[i : i+l]
		repeats := findRepeatsInBytes(values, 3)

		if repeats == nil {
			if err := out.WriteByte(byte(-l)); err != nil {
				return errors.WithStack(err)
			}
			if _, err := out.Write(values); err != nil {
				return errors.WithStack(err)
			}
			i += l
			continue
		}

		if repeats[0].start != 0 {
			if err := out.WriteByte(byte(-(repeats[0].start))); err != nil {
				return errors.WithStack(err)
			}
			if _, err := out.Write(values[:repeats[0].start]); err != nil {
				return errors.WithStack(err)
			}
		}
		for j := 0; j < len(repeats); j++ {

			if err := out.WriteByte(byte(repeats[j].count - MIN_REPEAT_SIZE)); err != nil {
				return errors.WithStack(err)
			}
			if err := out.WriteByte(values[repeats[j].start]); err != nil {
				return errors.WithStack(err)
			}

			if j+1 < len(repeats) {
				if err := out.WriteByte(byte(-(repeats[j+1].start - (repeats[j].start + repeats[j].count)))); err != nil {
					return errors.WithStack(err)
				}
				if _, err := out.Write(values[repeats[j].start+repeats[j].count : repeats[j+1].start]); err != nil {
					return errors.WithStack(err)
				}
			}
		}
		left := repeats[len(repeats)-1].start + repeats[len(repeats)-1].count // first not include in repeat
		if left < l {
			if err := out.WriteByte(byte(-(l - left))); err != nil {
				return errors.WithStack(err)
			}
			if _, err := out.Write(values[left:l]); err != nil {
				return errors.WithStack(err)
			}
		}

		i += l
	}

	return nil
}

// bool run length use byte run length encoding, but how to know the length of bools?
type BoolRunLength struct {
	*ByteRunLength
}

func (d *BoolRunLength) Decode(in io.ByteReader, vs []bool) (result []bool, err error) {
	var bs []byte
	if bs, err = d.ByteRunLength.Decode(in, bs); err != nil {
		return
	}
	for _, b := range bs {
		for i := 0; i < 8; i++ {
			v := (b>>byte(7-i))&0x01 == 0x01
			vs = append(vs, v)
		}
	}
	result = vs
	return
}

func (e *BoolRunLength) Encode(out *bytes.Buffer, vs interface{}) error {
	values := vs.([]bool)
	var bs []byte
	for i := 0; i < len(values); {
		var b byte
		for j := 0; j <= 7 && (i < len(values)); j++ {
			if values[i] {
				b |= 0x01 << byte(7-j)
			}
			i++
		}
		bs = append(bs, b)
	}
	return e.ByteRunLength.Encode(out, bs)
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

func (irl *intRunLengthV1) Encode(out *bytes.Buffer) error {
	if irl.numLiterals != 0 {

	}
	return nil
}*/

// string contents, decoding need length decoder
type BytesContent struct {
	/*	decoder
		content       [][]byte // utf-8 bytes
		lengthDecoder Decoder
		pos           int*/
}

func (bd *BytesContent) DecodeNext(in BufferedReader, byteLength int) (value []byte, err error) {
	value= make([]byte, byteLength)
	if _, err = io.ReadFull(in, value); err != nil {
		return value, errors.WithStack(err)
	}
	return
}

// write out content do not base length field, just base on len of content
func (e *BytesContent) Encode(out *bytes.Buffer, vs interface{}) error {
	values := vs.([][]byte)
	for _, v := range values {
		if _, err := out.Write(v); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// int run length encoding v2
type IntRleV2 struct {
	lastByte byte // different align when using read/writer, used in r/w
	bitsLeft int  // used in r/w

	Signed bool // for read
}

// decoding buffer all to u/literals
func (d *IntRleV2) Decode(in BufferedReader, values []uint64) ([]uint64, error) {
	// header from MSB to LSB
	firstByte, err := in.ReadByte()
	if err != nil {
		return values, errors.WithStack(err)
	}

	sub := firstByte >> 6
	switch sub {
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
				return values, errors.WithStack(err)
			}
			v |= uint64(b) << (8 * i)
		}

		for i := 0; i < repeatCount; i++ {
			values = append(values, v)
		}

	case Encoding_DIRECT: // numbers encoding in big-endian
		b1, err := in.ReadByte()
		if err != nil {
			return values, errors.WithStack(err)
		}
		header := uint16(firstByte)<<8 | uint16(b1) // 2 byte header
		w := (header >> 9) & 0x1F                   // width 5bits, bit 3 to 8
		width, err := widthDecoding(byte(w), false)
		if err != nil {
			return values, errors.WithStack(err)
		}
		length := int(header&0x1FF + 1)
		log.Tracef("decoding: int rl v2 Direct width %d length %d", width, length)

		d.forgetBits()
		for i := 0; i < length; i++ {
			v, err := d.readBits(in, int(width))
			if err != nil {
				return values, err
			}
			values = append(values, v)
		}

	case Encoding_PATCHED_BASE:
		// rethink: according to base value is a signed smallest value, patch should always signed?
		if !d.Signed {
			return values, errors.New("decoding: int rl v2 patch signed setting should not false")
		}

		if values, err = d.readPatched(in, firstByte, values); err != nil {
			return values, err
		}

	case Encoding_DELTA:
		// header: 2 bytes, base value: varint, delta base: signed varint
		header := make([]byte, 2)
		header[0] = firstByte
		header[1], err = in.ReadByte()
		if err != nil {
			return values, errors.WithStack(err)
		}
		width, err := widthDecoding(header[0]>>1&0x1f, true)
		if err != nil {
			return values, errors.WithStack(err)
		}
		length := int(header[0])&0x01<<8 | int(header[1]) + 1

		log.Tracef("decoding: irl v2 Delta length %d, width %d", length, width)

		var ubase uint64
		var base int64
		if d.Signed {
			base, err = binary.ReadVarint(in)
			if err != nil {
				return values, errors.WithStack(err)
			}
			ubase = Zigzag(base)
		} else {
			ubase, err = binary.ReadUvarint(in)
			if err != nil {
				return values, errors.WithStack(err)
			}
		}
		values = append(values, ubase)

		deltaBase, err := binary.ReadVarint(in)
		if err != nil {
			return values, errors.WithStack(err)
		}

		if d.Signed {
			values = append(values, Zigzag(base+deltaBase))
		} else {
			if deltaBase >= 0 {
				values = append(values, ubase+uint64(deltaBase))
			} else {
				values = append(values, ubase-uint64(-deltaBase))
			}
		}

		// delta values: W * (L-2)
		d.forgetBits()
		for i := 2; i < length; i++ {
			if width == 0 { //fixed delta
				if d.Signed {
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
					return values, err
				}
				if d.Signed {
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

	default:
		return values, errors.Errorf("decoding: int rl v2 encoding sub %d not recognized", sub)
	}

	return values, nil
}

func (d *IntRleV2) readPatched(in BufferedReader, firstByte byte, values []uint64) ([]uint64, error) {
	mark := len(values)
	header := make([]byte, 4) // 4 byte header
	if _, err := io.ReadFull(in, header[1:4]); err != nil {
		return values, errors.WithStack(err)
	}
	header[0] = firstByte

	// 5 bit width
	w := header[0] >> 1 & 0x1f
	width, err := widthDecoding(w, false) // 5 bits W
	if err != nil {
		return values, err
	}
	length := uint16(header[0])&0x01<<8 | uint16(header[1]) + 1 // 9 bits length, value 1 to 512
	bw := uint16(header[2])>>5&0x07 + 1                         // 3 bits base value width(BW), value 1 to 8 bytes
	pw, err := widthDecoding(header[2]&0x1f, false)             // 5 bits patch width(PW), value on table
	if err != nil {
		return values, err
	}
	pgw := uint16(header[3])>>5&0x07 + 1 // 3 bits patch gap width(PGW), value 1 to 8 bits
	if (int(pw) + int(pgw)) >= 64 {
		return values, errors.New("decoding: int rl v2, patchWidth+gapWidth must less or equal to 64")
	}
	pll := header[3] & 0x1f // 5bits patch list length, value 0 to 31

	baseBytes := make([]byte, bw)
	if _, err = io.ReadFull(in, baseBytes); err != nil {
		return values, errors.WithStack(err)
	}
	// base value big endian with msb of negative mark
	var base int64
	if bw == 0 {
		return values, errors.New("decoding: int rl v2 Patch baseWidth 0 not impl")
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

	log.Tracef("decoding: int rl v2 Patch values width %d length %d bw %d(bytes) and patchWidth %d pll %d base %d",
		width, length, bw, pw, pll, base)

	// values (W*L bits padded to the byte)
	// base is the smallest one, values should be all positive
	d.forgetBits()
	for i := 0; i < int(length); i++ {
		delta, err := d.readBits(in, int(width))
		if err != nil {
			return values, err
		}
		// rethink: cast int64
		values = append(values, Zigzag(base+int64(delta)))
	}

	// decode patch values, PGW+PW must < 64
	d.forgetBits()
	for i := 0; i < int(pll); i++ {
		pp, err := d.readBits(in, int(pgw)+int(pw))
		if err != nil {
			return values, err
		}

		patchGap := int(pp >> pw)
		mark += patchGap
		patch:= pp & ((1 << pw)-1)

		// todo: check gap==255 and patch==0

		// patchValue should be at largest 63 bits?
		v := UnZigzag(values[mark])
		v -= base // remove added base first
		v |= int64(patch << width)
		v += base // add base back
		values[mark] = Zigzag(v)
	}

	return values, nil
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

func (e *IntRleV2) writeBits(value uint64, bits int, out *bytes.Buffer) (err error) {
	totalBits := e.bitsLeft + bits
	if totalBits > 64 {
		return errors.New("write bits > 64")
	}

	if e.lastByte != 0 {
		value = (uint64(e.lastByte) << bits) | value // last byte and value should not overlap
	}

	for totalBits -= 8; totalBits >= 0; totalBits -= 8 {
		b := byte(value >> totalBits)
		if err = out.WriteByte(b); err != nil {
			return errors.WithStack(err)
		}
	}
	totalBits += 8
	e.bitsLeft = totalBits
	// clear lead bits
	e.lastByte = byte(value) & ((1 << e.bitsLeft) - 1)

	return
}

func (e *IntRleV2) writeLeftBits(out *bytes.Buffer) error {
	if e.bitsLeft != 0 {
		b := e.lastByte << (8 - e.bitsLeft) // last align to msb
		if err := out.WriteByte(b); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (e *IntRleV2) Encode(out *bytes.Buffer, vs interface{}) error {
	values := vs.([]uint64)
	return e.write(out, values)
}

func (e *IntRleV2) write(out *bytes.Buffer, values []uint64) (err error) {
	if len(values) <= MIN_REPEAT_SIZE {
		return e.writeDirect(out, true, values)
	}

	for i := 0; i < len(values); {
		// try repeat first
		repeat := getRepeat(values, i)
		if repeat >= 3 {
			// short repeat
			if repeat <= 10 {
				//e.sub = Encoding_SHORT_REPEAT
				var v uint64
				if e.Signed {
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
			if e.Signed {
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
		if e.tryDeltaEncoding(values[i:], dv) {
			b := make([]byte, 8)
			var n int
			if e.Signed {
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
		if e.Signed {
			// try patch
			sub, err = bitsWidthAnalyze(values[i:], &pvs)
			if err != nil {
				return err
			}
		} else {
			// fixme: make sure this
			sub = Encoding_DIRECT
		}

		if sub == Encoding_PATCHED_BASE {
			//log.Tracef("encoding: int rl v2 Patch %s", pvs.toString())
			if err = e.writePatch(&pvs, out); err != nil {
				return
			}
			// todo: test len(values) > 512
			i += pvs.count
			continue
		}

		// todo: max scope every encoding method varies
		var scope int
		l := len(values) - i
		if l >= MAX_SCOPE {
			scope = MAX_SCOPE
		} else {
			scope = l
		}

		if sub == Encoding_DIRECT {
			if err = e.writeDirect(out, true, values[i:i+scope]); err != nil {
				return
			}
			i += scope
			continue
		}

		return errors.New("encoding: no sub decided!")
	}

	return
}

func (e *IntRleV2) writeDirect(out *bytes.Buffer, widthAlign bool, values []uint64) error {
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
	l := len(values) - 1
	header[0] |= byte(l>>8) & 0x01
	header[1] = byte(l)
	log.Tracef("encoding: int rl v2 Direct width %d values %d ", width, len(values))
	if _, err := out.Write(header); err != nil {
		return errors.WithStack(err)
	}

	e.forgetBits()
	for _, v := range values {
		if err := e.writeBits(v, width, out); err != nil {
			return err
		}
	}
	if err:=e.writeLeftBits(out);err!=nil {
		return err
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
func (e *IntRleV2) tryDeltaEncoding(values []uint64, dv *deltaValues) bool {
	if len(values) <= 2 {
		return false
	}

	if values[0] == values[1] {
		return false
	}

	if e.Signed {
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
		if e.Signed {
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
	count  int
	values []uint64
	width  byte // value bits width
	base   int64
	//pll        byte     // patch list length
	//gapWidth   byte     // patch gap bits width, 1 to 8
	gaps []int // patch gap values
	//patchWidth byte     // patch bits width according to table, 1 to 64
	patches []uint64 // patch values, already shifted valuebits
}

/*func (pvs *patchedValues) toString() string {
	return fmt.Sprintf("patch: base %d, delta value width %d, pll %d, gapWidth %d, patchWidth %d",
		pvs.base, pvs.width, len(pvs.patches), pvs.gapWidth, pvs.patchWidth)
}*/

// analyze bit widths, if return encoding Patch then fill the patchValues, else return encoding Direct
// todo: patch 0
func bitsWidthAnalyze(values []uint64, pvs *patchedValues) (sub byte, err error) {
	base := UnZigzag(values[0])
	var count int
	//baseWidthHist := make([]byte, BITS_SLOTS) // slots for 0 to 64 bits widths

	// max L is 512
	for i := 1; i < 512 && i < len(values); i++ {
		v := UnZigzag(values[i])
		if v < base {
			base = v
		}
		// toAssure: using zigzag to decide bits width
		//baseWidthHist[getAlignedWidth(values[i])] += 1
		count = i + 1
	}

	// get bits width cover 100% and 90%
	/*var p100w, p90w byte
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
	log.Tracef("encoding: int rl try patch, 0.90 width %d and 1.00 width %d", p90w, p100w)*/

	var width, w byte // w is %90 delta values width

	deltaValues := make([]uint64, count)
	deltaValuesWidths := make([]byte, count)
	for i := 0; i < count; i++ {
		deltaValues[i] = uint64(UnZigzag(values[i]) - base) // it should be positive
		width_ := byte(getBitsWidth(deltaValues[i]))
		deltaValuesWidths[i] = width_
		if width_ > width {
			width = width_
		}
	}
	deltaValuesWidthHist := make([]byte, BITS_SLOTS) // values bits width 0 to 64
	for i := 0; i < count; i++ {
		deltaValuesWidthHist[deltaValuesWidths[i]] += 1
	}

	p90len := int(float64(count) * 0.9)
	l := 0
	for i := 0; i < BITS_SLOTS; i++ {
		l += int(deltaValuesWidthHist[i])
		if l >= p90len && deltaValuesWidthHist[i] != 0 {
			w = byte(i)
			break
		}
	}

	log.Tracef("encoding: int rl possible patch, 90%% width %d and width %d of adjusted values", w, width)
	if width > w {
		sub = Encoding_PATCHED_BASE
		pvs.base = base
		pvs.values = deltaValues
		pvs.width = w
		pvs.count = count

		// get patches
		var previousGap int
		for i := 0; i < count; i++ {
			if deltaValuesWidths[i] > w {
				for i-previousGap > 255 {
					// todo: test this block
					// recheck: gap is 255, patch is 0 ?
					pvs.gaps = append(pvs.gaps, 255)
					pvs.patches = append(pvs.patches, 0)
					previousGap += 255
				}
				pvs.gaps = append(pvs.gaps, i-previousGap)
				//Patches are applied by logically or’ing the data values with the relevant patch shifted W bits left
				patch := pvs.values[i]
				pvs.values[i] = patch & ((0x1 << w) - 1) // remove patch bits
				pvs.patches = append(pvs.patches, patch>>w)
				previousGap = i
			}
		}
		if len(pvs.patches) > 31 {
			// fixme: how to handle this
			return Encoding_UNSET, errors.New("patches max 31")
		}

		/*for i := 0; i < len(pvs.gaps); i++ {
			w := byte(getBitsWidth(uint64(pvs.gaps[i])))
			if pvs.gapWidth < w {
				pvs.gapWidth = w
			}
		}*/
		/*for i := 0; i < len(pvs.patches); i++ {
			w := getAlignedWidth(pvs.patches[i])
			if int(pvs.patchWidth) < w {
				pvs.patchWidth = byte(w)
			}
		}
		if pvs.gapWidth+pvs.patchWidth > 64 {
			return Encoding_UNSET, errors.New("encoding: int rl v2 Patch PGW+PW > 64")
		}*/
		return
	}

	//}
	sub = Encoding_DIRECT
	return
}

func (e *IntRleV2) writePatch(pvs *patchedValues, out *bytes.Buffer) error {
	// write header
	header := make([]byte, 4)
	header[0] = Encoding_PATCHED_BASE << 6
	w, err := widthEncoding(int(pvs.width))
	if err != nil {
		return errors.WithStack(err)
	}
	header[0] |= w << 1           // 5 bits for W
	length := len(pvs.values) - 1 // 1 to 512, 9 bits for L
	header[0] |= byte(length >> 8 & 0x01)
	header[1] = byte(length & 0xff)
	base := uint64(pvs.base)
	if pvs.base < 0 {
		base = uint64(-pvs.base)
	}
	// base bytes 1 to 8, add 1 bit for negative mark
	bw := byte(math.Ceil(float64(getBitsWidth(base)+1) / 8)) // base width
	header[2] = (bw - 1) << 5                                // 3bits for 1 to 8 bytes

	// 5 bits for PW (patch width) (1 to 64 bits)
	var patchWidth int
	for _, p := range pvs.patches {
		patchWidth_ := getBitsWidth(p)
		if patchWidth_ > patchWidth {
			patchWidth = patchWidth_
		}
	}
	pw, err := widthEncoding(patchWidth)
	if err != nil {
		return errors.WithStack(err)
	}
	header[2] |= pw & 0b11111

	// 3 bits for PGW (patch gap width) (1 to 8 bits)
	// so max pg for 3 bits pgw is 255
	var pgw int
	for _, g := range pvs.gaps {
		pgw_ := getBitsWidth(uint64(g))
		if pgw_ > pgw {
			pgw = pgw_
		}
	}
	header[3] = byte(pgw-1) & 0b111 << 5

	// 5 bits for PLL (patch list length) (0 to 31)
	// rethink: len(patches)==0 ?
	pll := byte(len(pvs.patches))
	header[3] |= pll & 0b11111

	if _, err := out.Write(header); err != nil {
		return errors.WithStack(err)
	}

	// write base
	if pvs.base < 0 {
		base |= 0b1 << ((bw-1)*8 + 7) // msb set to 1
	}
	for i := int(bw) - 1; i >= 0; i-- { // hxm: watch out loop index
		out.WriteByte(byte((base >> (byte(i) * 8)) & 0xff)) // big endian
	}

	// write W*L values
	e.forgetBits()
	for _, v := range pvs.values {
		if err := e.writeBits(v, int(pvs.width), out); err != nil {
			return err
		}
	}
	if err:= e.writeLeftBits(out);err!=nil {
		return err
	}

	if pgw+patchWidth > 64 {
		return errors.New("pgw+pw must less than or equal to 64")
	}

	// write patch list, (PLL*(PGW+PW) bytes
	e.forgetBits()
	for i, p := range pvs.patches {
		patch := (uint64(pvs.gaps[i]) << patchWidth) | p
		if err:= e.writeBits(patch, pgw+patchWidth, out);err!=nil {
			return err
		}
	}
	if err:= e.writeLeftBits(out);err!=nil {
		return err
	}

	return nil
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

type repeat struct {
	start int
	count int
}

// find repeats larger than minRepeat
func findRepeatsInBytes(values []byte, minRepeat int) (repeats []repeat) {

	for i := 0; i < len(values); {
		repeatCount := 1
		v := values[i]
		start := i

		j := i + 1
		for ; j < len(values); j++ {
			if values[j] == v {
				repeatCount++
			} else {
				if repeatCount >= minRepeat {
					repeats = append(repeats, repeat{start: start, count: repeatCount})
				}
				break
			}
		}

		if j == len(values) && repeatCount >= minRepeat {
			repeats = append(repeats, repeat{start: start, count: repeatCount})
			return
		}

		i = j
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

func (e *Base128VarInt) Encode(out *bytes.Buffer, vs interface{}) error {
	values := vs.([]int64)
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

type Ieee754Float struct {

}

func (d *Ieee754Float) Decode(in BufferedReader) (float32, error) {
	bb := make([]byte, 4)
	if _, err := io.ReadFull(in, bb); err != nil {
		return 0, errors.WithStack(err)
	}
	v := math.Float32frombits(binary.BigEndian.Uint32(bb))
	return v, nil
}

func (e *Ieee754Float) Encode(out *bytes.Buffer, vs interface{}) error {
	values := vs.([]float32)
	bb := make([]byte, 4)
	for _, v := range values {
		binary.BigEndian.PutUint32(bb, math.Float32bits(v))
		if _, err := out.Write(bb); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

type Ieee754Double struct {
}

func (d *Ieee754Double) Decode(in BufferedReader) (float64, error) {
	bb := make([]byte, 8)
	if _, err := io.ReadFull(in, bb); err != nil {
		return 0, errors.WithStack(err)
	}
	v := math.Float64frombits(binary.BigEndian.Uint64(bb))
	// !!!
	//v := math.Float64frombits(binary.LittleEndian.Uint64(bb))
	return v, nil
}

func (e *Ieee754Double) Encode(out *bytes.Buffer, vs interface{}) error {
	values := vs.([]float64)
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
