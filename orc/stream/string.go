package stream

import (
	"bytes"

	"github.com/patrickhuang888/goorc/orc/encoding"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/pb/pb"
)

type StringContentsReader struct {
	stream *reader
}

func NewStringContentsReader(opts *config.ReaderOptions, info *pb.Stream, start uint64, in orcio.File) *StringContentsReader {
	return &StringContentsReader{stream: &reader{opts:opts, info: info, buf: &bytes.Buffer{}, f: in, start: start}}
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

func (r *StringContentsReader) Seek(chunk uint64, chunkOffset uint64) error {
	if err := r.stream.seek(chunk, chunkOffset); err != nil {
		return err
	}
	return nil
}

func (r *StringContentsReader) Close() {
	r.stream.Close()
}
