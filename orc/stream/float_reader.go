package stream

import (
	"bytes"
	"github.com/patrickhuang888/goorc/orc"
	"github.com/patrickhuang888/goorc/orc/encoding"
	"github.com/patrickhuang888/goorc/pb/pb"
)

type FloatReader struct {
	stream  *reader
}

func NewFloatReader(opts *orc.ReaderOptions, info *pb.Stream, start uint64, path string) (r *FloatReader, err error) {
	var in orc.File
	if in, err = orc.Open(opts, path); err != nil {
		return
	}

	r= &FloatReader{stream: &reader{start: start, info: info, buf: &bytes.Buffer{}, in: in}}
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

func NewDoubleReader(opts *orc.ReaderOptions, info *pb.Stream, start uint64, path string) (r *DoubleReader, err error) {
	var fr *FloatReader
	if fr, err= NewFloatReader(opts, info, start, path);err!=nil {
		return
	}
	r = &DoubleReader{FloatReader:fr}
	return
}

func (r *DoubleReader) Next() (v float64, err error) {
	return encoding.DecodeDouble(r.stream)
}

