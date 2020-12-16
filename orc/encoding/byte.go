package encoding

import (
	"bytes"
	"io"
)

const MaxByteRunLength = 128
const MinRepeats = 3

type byteRunLength struct {
	position int
	values   []byte
}

func NewByteEncoder(resetPosition bool) *byteRunLength {
	if resetPosition {
		return &byteRunLength{values: make([]byte, 0, MaxByteRunLength), position: 0}
	}
	return &byteRunLength{values: make([]byte, 0, MaxByteRunLength), position: -1}
}

func (e *byteRunLength) GetPosition() []uint64 {
	return []uint64{uint64(e.position)}
}

func (e *byteRunLength) Encode(v interface{}, out *bytes.Buffer) error {
	value := v.(byte)
	e.values = append(e.values, value)

	if len(e.values) >= MaxByteRunLength {
		e.encodeBytes(out, false)
	}

	if e.position != -1 {
		e.position= len(e.values)
	}
	return nil
}

func (e *byteRunLength) Flush(out *bytes.Buffer) error {
	for len(e.values) != 0 {
		e.encodeBytes(out, true)
	}
	return nil
}

func (e *byteRunLength) Reset() {
	e.values = e.values[:0]
}

func (e *byteRunLength) encodeBytes(out *bytes.Buffer, toEnd bool) {
	// max len(values) is 128
	for ; ; {
		// find repeats from start, if no repeats index==len(values)
		index := findRepeats(e.values, MinRepeats)
		if index == 0 { //start from repeats, then find how many
			j := MinRepeats
			for ; j < len(e.values); j++ {
				if e.values[j] != e.values[0] {
					break
				}
			}
			index = j
			// write repeats
			out.WriteByte(byte(index - MinRepeats))
			out.WriteByte(e.values[0])
			logger.Tracef("byte encoder encoded %d repeated values", index)
		} else {
			//write direct
			out.WriteByte(byte(-index))
			out.Write(e.values[:index])
			logger.Tracef("byte encoder encoded %d directed values", index)
		}

		e.values = e.values[index:]

		if !toEnd || len(e.values) == 0 {
			break
		}
	}
}

func DecodeByteRL(in io.ByteReader, values []byte) ([]byte, error) {
	control, err := in.ReadByte()
	if err != nil {
		return values, err
	}
	if control < 0x80 { // run
		l := int(control) + MinRepeats
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

// find from start if values is repeats, if not find until at least min-repeats
func findRepeats(values []byte, minRepeats int) int {
	i := 0
	for ; i < len(values); i++ {
		count := 1
		for j := i + 1; j < len(values); j++ {
			if values[j] == values[i] {
				count++
				if count >= minRepeats {
					return i
				}
			} else {
				break
			}
		}
	}
	return i
}
