package stream

import (
	"bytes"
	"github.com/patrickhuang888/goorc/orc/config"
	"github.com/patrickhuang888/goorc/orc/io"

	"github.com/patrickhuang888/goorc/orc/encoding"
	"github.com/patrickhuang888/goorc/pb/pb"
)

type ByteReader struct {
	stream *reader

	values   []byte
	consumed int
}

func NewByteReader(opts *config.ReaderOptions, info *pb.Stream, start uint64, in io.File) *ByteReader{
	return &ByteReader{stream: &reader{opts: opts, info: info, start: start, in: in, buf: &bytes.Buffer{}}}
}

func (r *ByteReader) Next() (v byte, err error) {
	if r.consumed == len(r.values) {
		r.values = r.values[:0]
		r.consumed = 0

		if r.values, err = encoding.DecodeByteRL(r.stream, r.values); err != nil {
			return 0, err
		}
	}

	v = r.values[r.consumed]
	r.consumed++
	return
}

func (r *ByteReader) Seek(chunkOffset uint64, uncompressionOffset uint64, decodingPos uint64) error {
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

func (r *ByteReader) Finished() bool {
	return r.stream.finished() && (r.consumed == len(r.values))
}

func (r *ByteReader) Close() {
	r.stream.Close()
}
