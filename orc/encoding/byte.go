package encoding

import (
	"bytes"
	log "github.com/sirupsen/logrus"
	"io"
)

const MaxByteRunLength = 128
const MinRepeats = 3

/*
	for simplicity, there should be 1 marked position in 1 encoded block which no less than MaxByteRunLength
 */
type byteRunLength struct {
	// fixme: only 1 marked here
	markedPosition int
	positions      []uint64
	values         []byte
}

func NewByteEncoder() *byteRunLength {
	e := &byteRunLength{values: make([]byte, 0, MaxByteRunLength), markedPosition: -1}
	return e
}

func (e *byteRunLength) MarkPosition() {
	if len(e.values) == 0 {
		log.Errorf("mark position error, no value")
		return
	}
	e.markedPosition = len(e.values)
}

func (e *byteRunLength) PopPositions() []uint64 {
	ps := e.positions
	e.positions = nil
	return ps
}

func (e *byteRunLength) Encode(v interface{}, out *bytes.Buffer) error {
	value := v.(byte)

	e.values = append(e.values, value)

	if len(e.values) >= MaxByteRunLength {
		e.encodeBytes(out, &e.values)
	}
	return nil
}

func (e *byteRunLength) Flush(out *bytes.Buffer) error {
	for len(e.values) != 0 {
		e.encodeBytes(out, &e.values)
	}
	e.Reset()
	return nil
}

func (e *byteRunLength) Reset() {
	//
}

func (e *byteRunLength) encodeBytes(out *bytes.Buffer, vector *[]byte) {
	// max len(values) is 128
	values := *vector

	// find repeats from start
	index := findRepeats(values, MinRepeats)
	if index == 0 { //start from repeats, then find how many
		j:= MinRepeats
		for; j < len(values); j++ {
			if values[j] != values[0] {
				break
			}
		}
		index = j
		// write repeats
		out.WriteByte(byte(index - MinRepeats))
		out.WriteByte(values[0])
	} else {
		//write direct
		out.WriteByte(byte(-index))
		out.Write(values[:index])
	}

	if e.markedPosition != -1 {
		if e.markedPosition < index {
			e.positions = append(e.positions, uint64(e.markedPosition))
			e.markedPosition = -1
		} else {
			e.markedPosition = e.markedPosition - index
		}
	}

	values = values[index:]
	*vector = values
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
