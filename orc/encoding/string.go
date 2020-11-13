package encoding

import (
	"github.com/pkg/errors"
	"io"
)

type stringContents struct {
	contents       []byte
	markedPosition int
	positions      []uint64
}

func (e *stringContents) Encode(v interface{}) error {
	bs, ok := v.([]byte)
	if !ok {
		return errors.New("string contents encoder need []byte to encoding")
	}
	e.contents = append(e.contents, bs...)
	e.markedPosition++
	return nil
}

func (e stringContents) BufferedSize() int {
	return len(e.contents)
}

func (e stringContents) Flush() (data []byte, err error) {
	return e.contents, nil
}

func (e *stringContents) MarkPosition() {
	e.positions = append(e.positions, uint64(e.markedPosition))
}

func (e stringContents) PopPositions() []uint64 {
	r := e.positions
	e.positions = nil
	return r
}

func (e stringContents) Reset() {
	e.contents = e.contents[:0]
	e.positions = nil
	e.markedPosition = 0
}

func NewStringContentsEncoder() Encoder {
	return &stringContents{}
}

func DecodeBytes(in io.Reader, length int) ([]byte, error) {
	value := make([]byte, length)
	if _, err := io.ReadFull(in, value); err != nil {
		return nil, err
	}
	return value, nil
}

