package stream

import (
	"bytes"
	"errors"
	"github.com/patrickhuang888/goorc/orc"
	"github.com/patrickhuang888/goorc/orc/encoding"
	"github.com/patrickhuang888/goorc/pb/pb"
)

type StringContentsReader struct {
	stream *reader
}

func NewStringContentsReader(opts *orc.ReaderOptions, info *pb.Stream, start uint64, path string) (r *StringContentsReader, err error) {
	var in orc.File
	if in, err = orc.Open(opts, path); err != nil {
		return
	}

	r = &StringContentsReader{stream: &reader{info: info, buf: &bytes.Buffer{}, in: in, start: start}}
	return
}

func (r *StringContentsReader) NextBytes(len uint64) (v []byte, err error) {
	v, err = encoding.DecodeBytes(r.stream, int(len))
	return
}

func (r *StringContentsReader) NextString(len uint64) (v string, err error) {
	var bb []byte
	bb, err = r.NextBytes(len)
	if err != nil {
		return
	}
	return string(bb), err
}

func (r *StringContentsReader) Finished() bool {
	return r.stream.finished()
}

// for read column using encoding like dict
func (r *StringContentsReader) getAllString(byteLengths []uint64) (vs []string, err error) {
	for !r.Finished() {
		// todo: data check
		for _, l := range byteLengths {
			var v string
			v, err = r.NextString(l)
			if err != nil {
				return
			}
			vs = append(vs, v)
		}
	}
	return
}

func (r *StringContentsReader) Seek(chunkOffset uint64, uncompressionOffset uint64, decodingPos uint64, lens []uint64) error {
	if len(lens) != int(decodingPos) {
		return errors.New("seek string length error")
	}

	if err := r.stream.seek(chunkOffset, uncompressionOffset); err != nil {
		return err
	}

	for i := 0; i < int(decodingPos); i++ {
		if _, err := r.NextString(lens[i]); err != nil {
			return err
		}
	}
	return nil
}

func (r *StringContentsReader) Close() error {
	return r.stream.Close()
}
