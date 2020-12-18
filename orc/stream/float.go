package stream

import (
	"bytes"
	"github.com/patrickhuang888/goorc/orc/config"
	"github.com/patrickhuang888/goorc/orc/encoding"
	"github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
)

type FloatReader struct {
	is64 bool
	stream *reader
}

func NewFloatReader(opts *config.ReaderOptions, info *pb.Stream, start uint64, in io.File, is64 bool) (r *FloatReader) {
	r = &FloatReader{stream: &reader{opts: opts, start: start, info: info, buf: &bytes.Buffer{}, in: in}, is64: is64}
	return
}

func (r *FloatReader) NextFloat() (v float32, err error) {
	if r.is64 {
		return 0, errors.New("is 64 bit reader")
	}
	return encoding.DecodeFloat(r.stream)
}

func (r *FloatReader) NextDouble() (v float64, err error) {
	if !r.is64 {
		return 0, errors.New("is 32 bit reader")
	}
	return encoding.DecodeDouble(r.stream)
}

func (r FloatReader) Finished() bool {
	return r.stream.finished()
}

func (r *FloatReader) Seek(chunk uint64, chunkOffset uint64, offset uint64) error {
	// offset should be always zero
	if err := r.stream.seek(chunk, chunkOffset); err != nil {
		return err
	}
	return nil
}

func (r *FloatReader) Close() {
	r.stream.Close()
}
