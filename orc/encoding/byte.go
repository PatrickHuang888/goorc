package encoding

import (
	"bytes"
	log "github.com/sirupsen/logrus"
	"io"
)

const MAX_BYTE_RL = 128
const MIN_REPEAT_SIZE = 3

type byteRunLength struct {
	// fixme: assuming indexStride >= MAX_BYTE_RL

	markedPosition int
	positions      []uint64

	offset int
	values []byte
	buf    *bytes.Buffer
}

func (e byteRunLength) BufferedSize() int {
	return e.buf.Len()
}

func NewByteEncoder() *byteRunLength {
	e := &byteRunLength{values: make([]byte, MAX_BYTE_RL), buf: &bytes.Buffer{}, offset: -1}
	e.Reset()
	return e
}

func (e *byteRunLength) MarkPosition() {
	if e.offset == -1 {
		log.Errorf("mark position error, offset -1")
		return
	}
	e.markedPosition = e.offset
}

func (e *byteRunLength) GetAndClearPositions() (ps []uint64) {
	ps = e.positions
	e.positions = e.positions[:0]
	return
}

func (e *byteRunLength) Encode(v interface{}) (err error) {
	value := v.(byte)

	e.offset++
	e.values[e.offset] = value

	if e.offset >= MAX_BYTE_RL-1 {
		e.encodeBytes(e.buf, e.values[:e.offset+1])
		e.offset = -1
	}

	return nil
}

func (e *byteRunLength) Flush() (data []byte, err error) {
	if e.offset != -1 {
		e.encodeBytes(e.buf, e.values[:e.offset+1])
	}
	data = e.buf.Bytes()
	e.Reset()
	return
}

func (e *byteRunLength) Reset() {
	e.offset = -1
	e.buf.Reset()
}

func (e *byteRunLength) encodeBytes(out *bytes.Buffer, vs []byte){

	for i := 0; i < len(vs); {
		mark := i

		l := len(vs) - i
		if l > MAX_BYTE_RL { // max 128
			l = MAX_BYTE_RL
		}

		values := vs[i : i+l]
		repeats := findRepeatsInBytes(values, 3)

		if repeats == nil {
			out.WriteByte(byte(-l))
			out.Write(values)

			i += l

			if e.markedPosition != -1 && i >= e.markedPosition {
				e.positions = append(e.positions, uint64(e.markedPosition-mark+1))
				e.markedPosition = -1
			}
			continue
		}

		if repeats[0].start != 0 {
			out.WriteByte(byte(-(repeats[0].start)))
			out.Write(values[:repeats[0].start])
		}
		for j := 0; j < len(repeats); j++ {

			out.WriteByte(byte(repeats[j].count - MIN_REPEAT_SIZE))
			out.WriteByte(values[repeats[j].start])

			if j+1 < len(repeats) {
				out.WriteByte(byte(-(repeats[j+1].start - (repeats[j].start + repeats[j].count))))
				out.Write(values[repeats[j].start+repeats[j].count : repeats[j+1].start])
			}
		}
		left := repeats[len(repeats)-1].start + repeats[len(repeats)-1].count // first not include in repeat
		if left < l {
			out.WriteByte(byte(-(l - left)))
			out.Write(values[left:l])
		}

		i += l

		if e.markedPosition != -1 && i >= e.markedPosition {
			e.positions = append(e.positions, uint64(e.markedPosition-mark+1))
			e.markedPosition = -1
		}
	}

}

func DecodeByteRL(in io.ByteReader, values []byte) ([]byte, error) {
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
	} else { // literals -1 ~ -128
		l := -int(int8(control))
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
