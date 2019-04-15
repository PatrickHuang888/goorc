package orc

import (
	"github.com/pkg/errors"
)

type readOp int8

// like Java ByteBuffer, limit set to len of buffer slice
type ByteBuffer struct {
	buf      []byte
	position int
	limit    int
}

// ready for read
func (bb *ByteBuffer) Flip() {
	bb.limit = bb.position
	bb.position = 0
}

// ready for write
func (bb *ByteBuffer) Clear() {
	bb.position = 0
	bb.limit = len(bb.buf)
}

func (bb *ByteBuffer) HasRemaining() bool {
	return bb.position < bb.limit
}

func (bb *ByteBuffer) Remaining() int {
	return bb.limit - bb.position
}

func (bb *ByteBuffer) ReadByte() (byte, error) {
	if bb.position >= bb.limit {
		return 0, errors.New("buffer under flow")
	}
	bb.position++
	return bb.buf[bb.position], nil
}

func (bb *ByteBuffer) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	n = copy(p, bb.buf[bb.position:bb.limit])
	bb.position += n
	return n, nil
}

func (bb *ByteBuffer) WriteByte(c byte) error {
	if bb.position >= bb.limit {
		return errors.New("buffer over flow")
	}
	bb.position++
	bb.buf[bb.position] = c
	return nil
}

func (bb *ByteBuffer) Write(p []byte) (n int, err error) {
	n= copy(bb.buf[bb.position:bb.limit], p)
	
}
