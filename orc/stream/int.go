package stream

import (
	"bytes"
	"github.com/patrickhuang888/goorc/orc/config"
	"github.com/patrickhuang888/goorc/orc/encoding"
	"github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
)

type IntRLV2Reader struct {
	stream *reader

	signed  bool
	values  []int64
	uvalues []uint64
	pos     int

	decoder encoding.IntDecoder
}

func NewIntRLV2Reader(opts *config.ReaderOptions, info *pb.Stream, start uint64, signed bool, in io.File) *IntRLV2Reader {
	return &IntRLV2Reader{stream: &reader{opts: opts, info: info, start: start, buf: &bytes.Buffer{}, f: in},
		decoder: encoding.NewIntDecoder(signed), signed: signed}
}

func (r *IntRLV2Reader) NextInt64() (v int64, err error) {
	if !r.signed {
		return 0, errors.New("should be singed int")
	}

	if r.pos >= len(r.values) {
		r.pos = 0
		if r.values, err = r.decoder.DecodeInt(r.stream); err != nil {
			return 0, err
		}
		logger.Tracef("stream long read column %d has read %d values", r.stream.info.GetColumn(), len(r.values))
	}
	v = r.values[r.pos]
	r.pos++
	return v, nil
}

func (r *IntRLV2Reader) NextUInt64() (v uint64, err error) {
	if r.signed {
		return 0, errors.New("should be un-signed int")
	}

	if r.pos >= len(r.uvalues) {
		r.pos = 0
		if r.uvalues, err = r.decoder.DecodeUInt(r.stream); err != nil {
			return 0, err
		}
		if len(r.uvalues) == 0 {
			return 0, errors.New("no uint64 values decoded")
		}
		logger.Debugf("stream LONG read column %d has read %d unsiged values", r.stream.info.GetColumn(), len(r.uvalues))
	}
	v = r.uvalues[r.pos]
	r.pos++
	return v, nil
}

// for small data like dict index, ignore stream.signed
func (r *IntRLV2Reader) GetAllUInts() ([]uint64, error) {
	if r.signed {
		return nil, errors.New("should be signed")
	}

	for !r.stream.finished() {
		vector, err := r.decoder.DecodeUInt(r.stream)
		if err != nil {
			return nil, err
		}
		r.uvalues = append(r.uvalues, vector...)
	}
	return r.uvalues, nil
}

func (r *IntRLV2Reader) Finished() bool {
	if r.signed {
		return r.stream.finished() && (r.pos == len(r.values))
	} else {
		return r.stream.finished() && (r.pos == len(r.uvalues))
	}
}

func (r *IntRLV2Reader) Seek(chunkOffset, offset, pos uint64) error {
	if err := r.stream.seek(chunkOffset, offset); err != nil {
		return err
	}

	r.pos = 0
	if r.signed {
		r.values = r.values[:0]
	} else {
		r.uvalues = r.uvalues[:0]
	}
	for i := 0; i < int(pos); i++ {
		if r.signed {
			if _, err := r.NextInt64(); err != nil {
				return err
			}
		} else {
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

type Varint64Reader struct {
	stream *reader
}

func NewVarIntReader(opts *config.ReaderOptions, info *pb.Stream, start uint64, in io.File) *Varint64Reader {
	return &Varint64Reader{stream: &reader{opts: opts, info: info, start: start, buf: &bytes.Buffer{}, f: in}}
}

func (r *Varint64Reader) NextInt64() (v int64, err error) {
	v, err = encoding.DecodeVarInt64(r.stream)
	return
}

func (r *Varint64Reader) Finished() bool {
	return r.stream.finished()
}

func (r *Varint64Reader) Seek(offset, uncompressedOffset, pos uint64) error {
	if err := r.stream.seek(offset, uncompressedOffset); err != nil {
		return err
	}
	// always 1
	return nil
}

func (r *Varint64Reader) Close() {
	r.stream.Close()
}
