package stream

import (
	"bytes"
	"github.com/patrickhuang888/goorc/orc/config"
	"github.com/patrickhuang888/goorc/orc/encoding"
	"github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/pb/pb"
	log "github.com/sirupsen/logrus"
)

type BoolReader struct {
	stream *reader

	values []bool
	pos    int
}

func NewBoolReader(opts *config.ReaderOptions, info *pb.Stream, start uint64, in io.File) *BoolReader {
	return &BoolReader{stream: &reader{opts: opts, info: info, start: start, in: in, buf: &bytes.Buffer{}}}
}

func (r *BoolReader) Next() (v bool, err error) {
	if r.pos >= len(r.values) {
		r.pos = 0
		r.values = r.values[:0]

		if r.values, err = encoding.DecodeBools(r.stream, r.values); err != nil {
			return
		}
		log.Tracef("bool stream has read %d values", len(r.values))
	}
	v = r.values[r.pos]
	r.pos++
	return
}

func (r *BoolReader) Seek(chunkOffset uint64, uncompressedOffset uint64, decodingPos uint64) error {
	if err := r.stream.seek(chunkOffset, uncompressedOffset); err != nil {
		return err
	}
	for i := 0; i < int(decodingPos); i++ {
		if _, err := r.Next(); err != nil {
			return err
		}
	}
	return nil
}

func (r *BoolReader) Finished() bool {
	return r.stream.finished() && (r.pos == len(r.values))
}

func (r *BoolReader) Close() {
	r.stream.Close()
}