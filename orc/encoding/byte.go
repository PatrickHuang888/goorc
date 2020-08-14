package encoding

import (
	"bytes"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
)

const MAX_BYTE_RL = 128
const MIN_REPEAT_SIZE = 3

type ByteRunLength struct {
	// fixme: assuming indexStride >= MAX_BYTE_RL

	markedPosition int
	positions      []uint64

	offset int
	values []byte
	buf    *bytes.Buffer
}

func NewByteEncoder() *ByteRunLength {
	e := &ByteRunLength{values: make([]byte, MAX_BYTE_RL), buf: &bytes.Buffer{}}
	e.Reset()
	return e
}

func (e *ByteRunLength) MarkPosition() {
	if e.offset == -1 {
		log.Errorf("mark position error, offset -1")
		return
	}
	e.markedPosition = e.offset
}

func (e *ByteRunLength) GetAndClearPositions() (ps []uint64) {
	ps = e.positions
	e.positions = e.positions[:0]
	return
}

func (e *ByteRunLength) Encode(v interface{}) (data []byte, err error) {
	value := v.(byte)

	e.offset++
	e.values[e.offset] = value

	if e.offset >= MAX_BYTE_RL-1 {
		return e.Flush()
	}

	return
}

func (e *ByteRunLength) Flush() (data []byte, err error) {
	if e.offset != -1 {
		if err = e.encodeBytes(e.buf, e.values[:e.offset+1]); err != nil {
			return
		}
	}
	data = e.buf.Bytes()
	e.Reset()
	return
}

func (e *ByteRunLength) Reset() {
	e.offset = -1
	e.buf.Reset()
}

func (e *ByteRunLength) encodeBytes(out *bytes.Buffer, vs []byte) error {

	for i := 0; i < len(vs); {
		mark := i

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

			if e.markedPosition != -1 && i >= e.markedPosition {
				e.positions = append(e.positions, uint64(e.markedPosition-mark+1))
				e.markedPosition = -1
			}
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

		if e.markedPosition != -1 && i >= e.markedPosition {
			e.positions = append(e.positions, uint64(e.markedPosition-mark+1))
			e.markedPosition = -1
		}
	}

	return nil
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
