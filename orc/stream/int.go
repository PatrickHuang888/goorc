package stream

import (
	"bytes"
	"github.com/patrickhuang888/goorc/orc/config"
	"github.com/patrickhuang888/goorc/orc/encoding"
	"github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type IntRLV2Reader struct {
	stream *reader

	signed  bool
	values  []int64
	uvalues []uint64
	pos     int

	decoder *encoding.IntRL2
}

func NewIntRLV2Reader(opts *config.ReaderOptions, info *pb.Stream, start uint64, signed bool, in io.File) *IntRLV2Reader {
	return &IntRLV2Reader{stream: &reader{opts: opts, info: info, start: start, buf: &bytes.Buffer{}, in: in}, decoder: encoding.NewIntRLV2(signed)}
}

func (r *IntRLV2Reader) NextInt64() (int64, error) {
	if !r.signed {
		return 0, errors.New("should be un-singed int")
	}

	if r.pos >= len(r.values) {
		r.pos = 0
		vector, err := r.decoder.Decode(r.stream)
		if err != nil {
			return 0, err
		}
		r.values = vector.([]int64)
		log.Tracef("stream long read column %d has read %d values", r.stream.info.GetColumn(), len(r.values))
	}

	v := r.values[r.pos]
	r.pos++
	return v, nil
}

func (r *IntRLV2Reader) NextUInt64() (uint64, error) {
	if r.signed {
		return 0, errors.New("should be signed int")
	}

	if r.pos >= len(r.values) {
		r.pos = 0
		vector, err := r.decoder.Decode(r.stream)
		if err != nil {
			return 0, err
		}
		r.uvalues = vector.([]uint64)
		log.Tracef("stream long read column %d has read %d values", r.stream.info.GetColumn(), len(r.values))
	}

	v := r.uvalues[r.pos]
	r.pos++
	return v, nil
}

// for small data like dict index, ignore stream.signed
func (r *IntRLV2Reader) GetAllUInts() ([]uint64, error) {
	if r.signed {
		return nil, errors.New("should be signed")
	}

	for !r.stream.finished() {
		vector, err := r.decoder.Decode(r.stream)
		if err != nil {
			return nil, err
		}
		r.uvalues = append(r.uvalues, vector.([]uint64)...)
	}
	return r.uvalues, nil
}

func (r *IntRLV2Reader) Finished() bool {
	return r.stream.finished() && (r.pos == len(r.values))
}

func (r *IntRLV2Reader) Seek(chunkOffset, uncompressedOffset, pos uint64) error {
	if err := r.stream.seek(chunkOffset, uncompressedOffset); err != nil {
		return err
	}
	for i := 0; i < int(pos); i++ {
		if r.signed {
			if _, err := r.NextInt64();err!=nil {
				return err
			}
		}else {
			if _, err := r.NextUInt64(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *IntRLV2Reader) Close() {
	r.stream.Close()
}

type VarIntReader struct {
	stream *reader
}

func NewVarIntReader(opts *config.ReaderOptions, info *pb.Stream, start uint64, in io.File) *VarIntReader {
	return &VarIntReader{stream: &reader{info: info, start: start, buf: &bytes.Buffer{}, in: in}}
}

func (r *VarIntReader) NextInt64() (v int64, err error) {
	v, err = encoding.DecodeVarInt64(r.stream)
	return
}

// todo: decode var 128

func (r *VarIntReader) Finished() bool {
	return r.stream.finished()
}

func (r *VarIntReader) Seek(offset, uncompressedOffset, pos uint64) error {
	if err := r.stream.seek(offset, uncompressedOffset); err != nil {
		return err
	}
	for i := 0; i < int(pos); i++ {
		if _, err := r.NextInt64(); err != nil {
			return err
		}
	}
	return nil
}

func (r *VarIntReader) Close() {
	r.stream.Close()
}
