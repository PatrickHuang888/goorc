package stream

import (
	"bytes"
	"github.com/patrickhuang888/goorc/orc/config"
	"github.com/patrickhuang888/goorc/orc/encoding"
	"github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/pb/pb"
)

type StringContentsReader struct {
	stream *reader
}

func NewStringContentsReader(opts *config.ReaderOptions, info *pb.Stream, start uint64, in io.File) *StringContentsReader {
	return &StringContentsReader{stream: &reader{opts:opts, info: info, buf: &bytes.Buffer{}, in: in, start: start}}
}

func (r *StringContentsReader) NextBytes(len uint64) (v []byte, err error) {
	return encoding.DecodeBytes(r.stream, int(len))
}

func (r *StringContentsReader) NextString(len uint64) (string, error) {
	bb, err := r.NextBytes(len)
	if err != nil {
		return "", err
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

func (r *StringContentsReader) Seek(chunkOffset uint64, offset uint64, lens []uint64) error {
	if err := r.stream.seek(chunkOffset, offset); err != nil {
		return err
	}

	for i := 0; i < len(lens); i++ {
		if _, err := r.NextString(lens[i]); err != nil {
			return err
		}
	}
	return nil
}

func (r *StringContentsReader) Close() {
	r.stream.Close()
}
