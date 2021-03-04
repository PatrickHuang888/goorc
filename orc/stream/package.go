package stream

import (
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/pb/pb"
	"io"
)

type readerItf interface {
	Finished() bool
	Close()
}

type DateReader interface {
	readerItf
	Next() (date api.Date, err error)
	Seek(chunk uint64, chunkOffset uint64, offset uint64) error
}

type Writer interface {
	Write(v interface{}) error
	Flush() error
	WriteOut(out io.Writer) (n int64, err error)
	GetPosition() []uint64
	Info() *pb.Stream
	Size() int
	Reset()
}
