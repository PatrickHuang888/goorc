package stream

import (
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
	r = &FloatReader{stream: newReader(opts, info, start, in), is64: is64}
	return
}

func (r *FloatReader) NextFloat() (v float32, err error) {
	if r.is64 {
		return 0, errors.Errorf("column %d is 64 bit reader", r.stream.info.GetColumn())
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
	if r.stream==nil {
		return true
	}
	return r.stream.finished()
}

func (r *FloatReader) Seek(chunk uint64, chunkOffset uint64) error {
	if r.stream==nil {
		return errors.New("stream not init")
	}

	// offset always zero
	if err := r.stream.seek(chunk, chunkOffset); err != nil {
		return err
	}
	return nil
}

func (r *FloatReader) Close() {
	if r.stream!=nil {
		r.stream.Close()
	}
}
