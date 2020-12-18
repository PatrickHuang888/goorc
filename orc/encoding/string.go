package encoding

import (
	"bytes"
	"io"

	"github.com/pkg/errors"
)

type StringContents struct {
}

func (e *StringContents) Encode(v interface{}, out *bytes.Buffer) error {
	bs, ok := v.([]byte)
	if !ok {
		return errors.New("string contents encoder need []byte to encoding")
	}
	if _, err := out.Write(bs); err != nil {
		return err
	}
	return nil
}

func (e *StringContents) Flush(out *bytes.Buffer) error {
	return nil
}

func (e *StringContents) GetPosition() []uint64 {
	// fixme: always 0 ??
	return []uint64{uint64(0)}
}

func (e *StringContents) Reset() {
}

func DecodeBytes(in io.Reader, length int) ([]byte, error) {
	value := make([]byte, length)
	if _, err := io.ReadFull(in, value); err != nil {
		return nil, err
	}
	return value, nil
}
