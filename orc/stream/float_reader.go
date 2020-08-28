package stream

import (
	"bytes"
	"github.com/patrickhuang888/goorc/orc/config"
	"github.com/patrickhuang888/goorc/orc/encoding"
	"github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/pb/pb"
)

type FloatReader struct {
	stream *reader
}

func NewFloatReader(opts *config.ReaderOptions, info *pb.Stream, start uint64, in io.File) (r *FloatReader) {
	r = &FloatReader{stream: &reader{opts: opts, start: start, info: info, buf: &bytes.Buffer{}, in: in}}
	return
}

func (r *FloatReader) Next() (v float32, err error) {

	return encoding.DecodeFloat(r.stream)
}

func (r *FloatReader) Finished() bool {
	return r.stream.finished()
}

func (r *FloatReader) Seek(chunkOffset uint64, uncompressionOffset uint64, decodingPos uint64) error {
	if err := r.stream.seek(chunkOffset, uncompressionOffset); err != nil {
		return err
	}
	for i := 0; i < int(decodingPos); i++ {
		if _, err := r.Next(); err != nil {
			return err
		}
	}
	return nil
}

func (r *FloatReader) Close() {
	r.stream.Close()
}

type DoubleReader struct {
	*FloatReader
}

func NewDoubleReader(opts *config.ReaderOptions, info *pb.Stream, start uint64, in io.File) *DoubleReader {
	fr := NewFloatReader(opts, info, start, in)
	return &DoubleReader{FloatReader: fr}
}

func (r *DoubleReader) Next() (v float64, err error) {
	return encoding.DecodeDouble(r.stream)
}
