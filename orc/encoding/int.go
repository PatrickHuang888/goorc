package encoding

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"

	"github.com/pkg/errors"
)

func NewIntRLV2(signed bool, doPosition bool) *IntRL2 {
	if doPosition {
		return &IntRL2{values: make([]uint64, 0, MaxIntRunLength), signed: signed, position: 0}
	}
	return &IntRL2{values: make([]uint64, 0, MaxIntRunLength), signed: signed, position: -1}
}

// 64 bit int run length encoding v2
type IntRL2 struct {
	lastByte byte // different align when using read/writer, used in r/w
	bitsLeft int  // used in r/w

	signed bool

	position int

	values []uint64
}

// decode 1 'block' values, not all data in input bufferedReader
// if d.singed return []int64, else return []uint64
// if EOF empty slices
func (d *IntRL2) Decode(in BufferedReader) (interface{}, error) {
	var values []uint64

	// header from MSB to LSB
	firstByte, err := in.ReadByte()
	if err != nil {
		return nil, err
	}

	sub := firstByte >> 6
	switch sub {
	case Encoding_SHORT_REPEAT:
		header := firstByte
		width := 1 + (header>>3)&0x07 // width 3bit at position 3
		repeatCount := int(3 + (header & 0x07))
		logger.Tracef("decoding: int rl v2 Short Repeat of count %d", repeatCount)

		var v uint64
		for i := width; i > 0; { // big endian
			i--
			b, err := in.ReadByte()
			if err != nil {
				return nil, err
			}
			v |= uint64(b) << (8 * i)
		}

		for i := 0; i < repeatCount; i++ {
			values = append(values, v)
		}

	case Encoding_DIRECT: // numbers encoding in big-endian
		b1, err := in.ReadByte()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		header := uint16(firstByte)<<8 | uint16(b1) // 2 byte header
		w := (header >> 9) & 0x1F                   // width 5bits, bit 3 to 8
		width, err := widthDecoding(byte(w), false)
		if err != nil {
			return nil, err
		}
		length := int(header&0x1FF + 1)
		logger.Tracef("decoding: int rl v2 Direct width %d length %d", width, length)

		d.forgetBits()
		for i := 0; i < length; i++ {
			v, err := d.readBits(in, int(width))
			if err != nil {
				return nil, err
			}
			values = append(values, v)
		}

	case Encoding_PATCHED_BASE:
		// rethink: according to base value is a signed smallest value, patch should always signed?
		if !d.signed {
			return nil, errors.New("decoding: int rl v2 patch signed setting should not false")
		}

		if values, err = d.readPatched(in, firstByte); err != nil {
			return nil, err
		}

	case Encoding_DELTA:
		// header: 2 bytes, base value: varint, delta base: signed varint
		header := make([]byte, 2)
		header[0] = firstByte
		header[1], err = in.ReadByte()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		width, err := widthDecoding(header[0]>>1&0x1f, true)
		if err != nil {
			return nil, err
		}
		length := int(header[0])&0x01<<8 | int(header[1]) + 1

		logger.Tracef("decoding: irl v2 Delta length %d, width %d", length, width)

		var ubase uint64
		var base int64
		if d.signed {
			base, err = binary.ReadVarint(in)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			ubase = Zigzag(base)
		} else {
			ubase, err = binary.ReadUvarint(in)
			if err != nil {
				return nil, errors.WithStack(err)
			}
		}
		values = append(values, ubase)

		deltaBase, err := binary.ReadVarint(in)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		if d.signed {
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
				if d.signed {
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
					return nil, err
				}
				if d.signed {
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
		return nil, errors.Errorf("decoding: int rl v2 encoding sub %d not recognized", sub)
	}

	if d.signed {
		var vs []int64
		for _, v := range values {
			vs = append(vs, UnZigzag(v))
		}
		return vs, nil
	} else {
		return values, nil
	}
}

func (d *IntRL2) readPatched(in BufferedReader, firstByte byte) ([]uint64, error) {
	var values []uint64

	mark := len(values)
	header := make([]byte, 4) // 4 byte header
	if _, err := io.ReadFull(in, header[1:4]); err != nil {
		return nil, errors.WithStack(err)
	}
	header[0] = firstByte

	// 5 bit width
	w := header[0] >> 1 & 0x1f
	width, err := widthDecoding(w, false) // 5 bits W
	if err != nil {
		return nil, err
	}
	length := uint16(header[0])&0x01<<8 | uint16(header[1]) + 1 // 9 bits length, value 1 to 512
	bw := uint16(header[2])>>5&0x07 + 1                         // 3 bits base value width(BW), value 1 to 8 bytes
	pw, err := widthDecoding(header[2]&0x1f, false)             // 5 bits patch width(PW), value on table
	if err != nil {
		return nil, err
	}
	pgw := uint16(header[3])>>5&0x07 + 1 // 3 bits patch gap width(PGW), value 1 to 8 bits
	if (int(pw) + int(pgw)) >= 64 {
		return nil, errors.New("decoding: int rl v2, patchWidth+gapWidth must less or equal to 64")
	}
	pll := header[3] & 0x1f // 5bits patch list length, value 0 to 31

	baseBytes := make([]byte, bw)
	if _, err = io.ReadFull(in, baseBytes); err != nil {
		return nil, errors.WithStack(err)
	}
	// base value big endian with msb of negative mark
	var base int64
	if bw == 0 {
		return nil, errors.New("decoding: int rl v2 Patch baseWidth 0 not impl")
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

	logger.Tracef("decoding: int rl v2 Patch values width %d length %d bw %d(bytes) and patchWidth %d pll %d base %d",
		width, length, bw, pw, pll, base)

	// values (W*L bits padded to the byte)
	// base is the smallest one, values should be all positive
	d.forgetBits()
	for i := 0; i < int(length); i++ {
		delta, err := d.readBits(in, int(width))
		if err != nil {
			return nil, err
		}
		// rethink: cast int64
		values = append(values, Zigzag(base+int64(delta)))
	}

	// decode patch values, PGW+PW must < 64
	d.forgetBits()
	for i := 0; i < int(pll); i++ {
		pp, err := d.readBits(in, int(pgw)+int(pw))
		if err != nil {
			return nil, err
		}

		patchGap := int(pp >> pw)
		mark += patchGap
		patch := pp & ((1 << pw) - 1)

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

func (d *IntRL2) readBits(in io.ByteReader, bits int) (value uint64, err error) {
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

func (d *IntRL2) forgetBits() {
	d.bitsLeft = 0
	d.lastByte = 0
}

func (e *IntRL2) writeBits(value uint64, bits int, out *bytes.Buffer) (err error) {
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

func (e *IntRL2) writeLeftBits(out *bytes.Buffer) error {
	if e.bitsLeft != 0 {
		b := e.lastByte << (8 - e.bitsLeft) // last align to msb
		if err := out.WriteByte(b); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

const MaxIntRunLength = 512

func (e *IntRL2) Encode(v interface{}, out *bytes.Buffer) error {
	var value uint64
	if e.signed {
		vt, ok := v.(int64)
		if !ok {
			return errors.New("int signed encoder encode int64 value")
		}
		value = Zigzag(vt)
	} else {
		vt, ok := v.(uint64)
		if !ok {
			return errors.New("int un-singed encoder encode uint64 value")
		}
		value = vt
	}

	e.values = append(e.values, value)

	if len(e.values) >= MaxIntRunLength {
		if err := e.write(out, false); err != nil {
			return err
		}
	}

	if e.position != -1 {
		e.position= len(e.values)
	}
	return nil
}

func (e *IntRL2) GetPosition() []uint64 {
	r:= []uint64{uint64(e.position)}
	//e.ResetPosition()
	return r
}

func (e *IntRL2) Reset() {
	e.bitsLeft = 0
	e.lastByte = 0
	e.values = e.values[:0]
}

func (e *IntRL2) ResetPosition() {
	e.position = 0
}

func (e *IntRL2) Flush(out *bytes.Buffer) error {
	if len(e.values) != 0 {
		logger.Tracef("encoding int rl v2 FLUSH %d values", len(e.values))
		if err := e.write(out, true); err != nil {
			return err
		}
	}
	e.Reset()
	return nil
}

func (e *IntRL2) write(out *bytes.Buffer, toEnd bool) error {
	if len(e.values) <= MinRepeats {
		if err := e.writeDirect(out, true); err != nil {
			return err
		}
		e.values = e.values[:0]
		return nil
	}

	repeat := getRepeats(e.values)
	if repeat >= 3 {
		// short repeat
		if repeat <= 10 {
			if err := writeShortRepeat(repeat-3, e.values[0], out); err != nil {
				return errors.WithStack(err)
			}
			logger.Tracef("encoding, int run length v2 write %d Short Repeat values", repeat)

			e.values = e.values[repeat:]
			return nil
		}

		// fixed delta 0
		b := make([]byte, 8)
		var n int
		if e.signed {
			n = binary.PutVarint(b, UnZigzag(e.values[0]))
		} else {
			n = binary.PutUvarint(b, e.values[0])
		}
		if err := writeDelta(out, b[:n], 0, uint16(repeat), []uint64{}); err != nil {
			return err
		}
		e.values = e.values[repeat:]
		return nil
	}

	// delta, need width should be stable?
	dv := &deltaValues{}
	if tryDeltaEncoding(e.signed, e.values, dv) {
		b := make([]byte, 8)
		var n int
		if e.signed {
			n = binary.PutVarint(b, UnZigzag(e.values[0]))
		} else {
			n = binary.PutUvarint(b, e.values[0])
		}
		if err := writeDelta(out, b[:n], dv.base, dv.length, dv.deltas); err != nil {
			return err
		}

		e.values = e.values[dv.length:]
		return nil
	}

	var pvs patchedValues
	if e.signed {
		if tryPatch(e.values, &pvs) {
			if err := e.writePatch(&pvs, out); err != nil {
				return err
			}
			e.values = e.values[pvs.count:]
			return nil
		}
	}

	// rest all will be direct ?
	if toEnd {
		if err := e.writeDirect(out, true); err != nil {
			return err
		}
		e.values = e.values[:0]
	}

	return nil
}

func (e *IntRL2) writeDirect(out *bytes.Buffer, widthAlign bool) error {
	header := make([]byte, 2)
	header[0] = Encoding_DIRECT << 6

	var width int
	for _, v := range e.values {
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
	l := len(e.values) - 1
	header[0] |= byte(l>>8) & 0x01
	header[1] = byte(l)
	if _, err := out.Write(header); err != nil {
		return errors.WithStack(err)
	}

	e.forgetBits()
	for _, v := range e.values {
		if err := e.writeBits(v, width, out); err != nil {
			return err
		}
	}
	if err := e.writeLeftBits(out); err != nil {
		return err
	}

	logger.Tracef("encoding: int rl v2 Direct width %d values %d ", width, len(e.values))
	return nil
}

type deltaValues struct {
	base   int64
	length uint16
	deltas []uint64
}

// for monotonically increasing or decreasing sequences
// run length should be at least 3
func tryDeltaEncoding(signed bool, values []uint64, dv *deltaValues) bool {
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

	for i := 3; i < len(values); i++ {
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

// analyze bit widths, if return encoding Patch then fill the patchValues, else return encoding Direct
// todo: patch 0
// todo: try how many values to determine patch
func tryPatch(values []uint64, pvs *patchedValues) bool {
	base := UnZigzag(values[0])
	var count int
	//baseWidthHist := make([]byte, BITS_SLOTS) // slots for 0 to 64 bits widths

	for i := 1; i < len(values); i++ {
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
	logger.Tracef("encoding: int rl try patch, 0.90 width %d and 1.00 width %d", p90w, p100w)*/

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

	logger.Tracef("int rl v2 try patch, 90%% width %d and width %d of adjusted values", w, width)
	if width > w {
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
			panic("patches max 31")
		}

		logger.Tracef("int rl v2 will do PATCH %d values", pvs.count)
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
		return true
	}

	return false
}

func (e *IntRL2) writePatch(pvs *patchedValues, out *bytes.Buffer) error {
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
	if err := e.writeLeftBits(out); err != nil {
		return err
	}

	if pgw+patchWidth > 64 {
		return errors.New("pgw+pw must less than or equal to 64")
	}

	// write patch list, (PLL*(PGW+PW) bytes
	e.forgetBits()
	for i, p := range pvs.patches {
		patch := (uint64(pvs.gaps[i]) << patchWidth) | p
		if err := e.writeBits(patch, pgw+patchWidth, out); err != nil {
			return err
		}
	}
	if err := e.writeLeftBits(out); err != nil {
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
func writeDelta(out *bytes.Buffer, first []byte, deltaBase int64, length uint16, deltas []uint64) error {
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
	w, err := widthEncoding(int(width))
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
	logger.Tracef("encoding, int run length v2 write DELTA %d values with delta base %d", length, deltaBase)
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

// find repeats from start
func getRepeats(values []uint64) int {
	if len(values) == 0 {
		panic("find repeats 0 values")
	}
	if len(values) == 1 {
		return 1
	}
	i := 1
	for ; i < len(values); i++ {
		if values[i] != values[0] {
			break
		}
	}
	return i
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

func writeShortRepeat(count int, x uint64, out *bytes.Buffer) error {
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
