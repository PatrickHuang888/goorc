package stream

import (
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	"github.com/patrickhuang888/goorc/orc/encoding"
	"github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/pb/pb"
)

func NewDateV2Reader(opts *config.ReaderOptions, info *pb.Stream, start uint64, in io.File) DateReader {
	return &dateV2Reader{stream: newReader(opts, info, start, in), decoder: encoding.NewDateV2DeCoder()}
}

type dateV2Reader struct {
	stream   *reader
	values   []api.Date
	consumed int
	decoder  encoding.DateDecoder
}

func (r dateV2Reader) Finished() bool {
	return r.stream.finished() && (r.consumed == len(r.values))
}

func (r *dateV2Reader) Close() {
	r.stream.Close()
}

func (r *dateV2Reader) Next() (date api.Date, err error) {
	if r.consumed == len(r.values) {
		r.consumed = 0
		if r.values, err = r.decoder.Decode(r.stream); err != nil {
			return
		}
		logger.Tracef("date stream %d read %d values", r.stream.info.GetColumn(), len(r.values))
	}
	date = r.values[r.consumed]
	r.consumed++
	return
}

func (r *dateV2Reader) Seek(chunk uint64, chunkOffset uint64, offset uint64) error {
	if err := r.stream.seek(chunk, chunkOffset); err != nil {
		return err
	}
	r.values = nil
	r.consumed = 0
	for i := 0; i < int(offset); i++ {
		if _, err := r.Next(); err != nil {
			return err
		}
	}
	return nil
}
