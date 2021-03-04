package stream

import (
	"bytes"
	"github.com/patrickhuang888/goorc/orc/config"
	"github.com/patrickhuang888/goorc/orc/encoding"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/pb/pb"
)

type ByteReader struct {
	stream *reader

	values   []byte
	consumed int
}

func NewByteReader(opts *config.ReaderOptions, info *pb.Stream, start uint64, in orcio.File) *ByteReader{
	return &ByteReader{stream: &reader{opts: opts, info: info, start: start, f: in, buf: &bytes.Buffer{}}}
}

func (r *ByteReader) Next() (v byte, err error) {
	if r.consumed == len(r.values) {
		r.values = r.values[:0]
		r.consumed = 0

		if r.values, err = encoding.DecodeByteRL(r.stream, r.values); err != nil {
			return 0, err
		}
		logger.Tracef("byte stream %d decode %d values", r.stream.info.GetColumn(), len(r.values))
	}

	v = r.values[r.consumed]
	r.consumed++
	return
}

func (r *ByteReader) Seek(chunkOffset uint64, offset uint64, valueOffset uint64) error {
	if err := r.stream.seek(chunkOffset, offset); err != nil {
		return err
	}
	r.values = r.values[:0]
	r.consumed = 0
	for i := 0; i < int(valueOffset); i++ {
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
