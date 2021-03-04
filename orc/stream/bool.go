package stream

import (
	"github.com/patrickhuang888/goorc/orc/config"
	"github.com/patrickhuang888/goorc/orc/encoding"
	"github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/pb/pb"
)

type BoolReader struct {
	reader *ByteReader
	values []bool
	pos    int
}

func NewBoolReader(opts *config.ReaderOptions, info *pb.Stream, start uint64, in io.File) *BoolReader {
	return &BoolReader{reader: NewByteReader(opts, info, start, in)}
}

func (r *BoolReader) Next() (v bool, err error) {
	if r.pos >= len(r.values) {
		r.pos = 0

		var b byte
		if b, err = r.reader.Next(); err != nil {
			return
		}
		r.values = encoding.DecodeBoolsFromByte(b)
	}
	v = r.values[r.pos]
	r.pos++
	return
}

func (r *BoolReader) Seek(chunkOffset uint64, offset uint64, byteRLOffset uint64, valueOffset uint64) error {
	if err := r.reader.Seek(chunkOffset, offset, byteRLOffset); err != nil {
		return err
	}
	r.pos = 0
	r.values = nil
	for i := 0; i < int(valueOffset); i++ {
		if _, err := r.Next(); err != nil {
			return err
		}
	}
	return nil
}

func (r *BoolReader) Finished() bool {
	return r.reader.Finished() && (r.pos == len(r.values))
}

func (r *BoolReader) Close() {
	r.reader.Close()
}
