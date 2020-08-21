package stream

import (
	"bytes"
	"github.com/patrickhuang888/goorc/orc/config"
	"github.com/patrickhuang888/goorc/orc/encoding"
	"github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/pb/pb"
	log "github.com/sirupsen/logrus"
)

type IntRLV2Reader struct {
	stream *reader

	values []uint64
	pos    int

	decoder *encoding.IntRL2
}

func NewRLV2Reader(opts *config.ReaderOptions, info *pb.Stream, start uint64, signed bool, path string) (r *IntRLV2Reader, err error) {
	var in io.File
	if in, err = io.Open(opts, path); err != nil {
		return
	}
	r = &IntRLV2Reader{stream: &reader{opts: opts, info: info, start: start, buf: &bytes.Buffer{}, in: in}, decoder: encoding.NewIntRLV2(signed)}
	return
}

func (r *IntRLV2Reader) NextInt64() (v int64, err error) {
	var uv uint64
	uv, err = r.NextUInt()
	if err != nil {
		return
	}
	v = encoding.UnZigzag(uv)
	return
}

func (r *IntRLV2Reader) NextUInt() (v uint64, err error) {
	if r.pos >= len(r.values) {
		r.pos = 0
		r.values = r.values[:0]

		if r.values, err = r.decoder.Decode(r.stream, r.values); err != nil {
			return
		}

		log.Tracef("stream long read column %d has read %d values", r.stream.info.GetColumn(), len(r.values))
	}

	v = r.values[r.pos]
	r.pos++
	return
}

// for small data like dict index, ignore stream.signed
func (r *IntRLV2Reader) GetAllUInts() (vs []uint64, err error) {
	for !r.stream.finished() {
		if vs, err = r.decoder.Decode(r.stream, vs); err != nil {
			return
		}
	}
	return
}

func (r *IntRLV2Reader) Finished() bool {
	return r.stream.finished() && (r.pos == len(r.values))
}

func (r *IntRLV2Reader) Seek(offset, uncompressedOffset, pos uint64) error {
	if err := r.stream.seek(offset, uncompressedOffset); err != nil {
		return err
	}
	for i := 0; i < int(pos); i++ {
		if _, err := r.NextUInt(); err != nil {
			return err
		}
	}
	return nil
}

func (r *IntRLV2Reader) Close(){
	r.stream.Close()
}

type VarIntReader struct {
	stream *reader
}

func NewVarIntReader(opts *config.ReaderOptions, info *pb.Stream, start uint64, path string) (r *VarIntReader, err error) {
	var in io.File
	if in, err = io.Open(opts, path); err != nil {
		return
	}

	r = &VarIntReader{stream: &reader{info: info, start: start, buf: &bytes.Buffer{}, in: in}}
	return
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

func (r *VarIntReader) Close(){
	r.stream.Close()
}
