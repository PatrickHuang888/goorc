package stream

import (
	"bytes"
	"github.com/patrickhuang888/goorc/orc/common"
	"github.com/patrickhuang888/goorc/orc/config"
	"github.com/patrickhuang888/goorc/orc/encoding"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
)

type writer struct {
	info *pb.Stream

	buf           *bytes.Buffer
	compressedBuf *bytes.Buffer

	opts *config.WriterOptions

	positions [][]uint64

	count int

	encoder encoding.Encoder
}

func (w writer) GetPosition() []uint64 {
	if !w.opts.WriteIndex {
		return []uint64{}
	}

	var pp []uint64
	if w.opts.CompressionKind != pb.CompressionKind_NONE {
		pp = append(pp, uint64(w.compressedBuf.Len()))
	}
	pp = append(pp, uint64(w.buf.Len()))
	pp = append(pp, w.encoder.GetPosition()...)
	return pp
}

// info will update after flush
func (w writer) Info() *pb.Stream {
	return w.info
}

func (w *writer) Write(v interface{}) error {
	if err := w.encoder.Encode(v, w.buf); err != nil {
		return err
	}
	w.count++

	if w.opts.CompressionKind != pb.CompressionKind_NONE && w.buf.Len() > w.opts.ChunkSize {
		if err := common.CompressingChunks(w.opts.CompressionKind, w.opts.ChunkSize, w.compressedBuf, w.buf); err != nil {
			return err
		}
		logger.Debugf("stream writer column %d kind %s has written %d values, buffer over chunksize, do compressing...",
			w.info.GetColumn(), w.info.GetKind().String(), w.count)
		w.count = 0
	}
	return nil
}

// return compressed data + uncompressed data
func (w writer) Size() (size int) {
	// not include encoder not flushed values
	if w.opts.CompressionKind == pb.CompressionKind_NONE {
		size = w.buf.Len()
		return
	}
	size = w.compressedBuf.Len() + w.buf.Len()
	return
}

// Flush flush remaining data in encoder then compressing left encoded data
// will be the last operation before writeout
// update stream info.length
func (w *writer) Flush() error {
	if err := w.encoder.Flush(w.buf); err != nil {
		return err
	}
	if w.opts.CompressionKind == pb.CompressionKind_NONE {
		*w.info.Length = uint64(w.buf.Len())
	} else {
		if w.buf.Len() > 0 {
			logger.Debugf("stream writer column %d kind %s flush compressing %d values", w.info.GetColumn(), w.info.GetKind().String(), w.count)
			if err := common.CompressingAllInChunks(w.opts.CompressionKind, w.opts.ChunkSize, w.compressedBuf, w.buf); err != nil {
				return err
			}
		}
		*w.info.Length = uint64(w.compressedBuf.Len())
	}

	return nil
}

func (w *writer) Reset() {
	*w.info.Length = 0

	w.buf.Reset()
	w.compressedBuf.Reset()

	w.positions = w.positions[:0]
}

// used together with flush
func (w *writer) WriteOut(out io.Writer) (n int64, err error) {
	if w.opts.CompressionKind == pb.CompressionKind_NONE {
		if n, err = w.buf.WriteTo(out); err != nil {
			return 0, errors.WithStack(err)
		}
		log.Debugf("write out column %d stream %s length %d", w.info.GetColumn(), w.info.GetKind(), n)
		return
	}

	if n, err = w.compressedBuf.WriteTo(out); err != nil {
		return 0, errors.WithStack(err)
	}
	log.Debugf("write out column %d stream %s length %d", w.info.GetColumn(), w.info.GetKind(), n)
	return
}

func NewByteWriter(id uint32, kind pb.Stream_Kind, opts *config.WriterOptions) Writer {
	info := &pb.Stream{Kind: &kind, Column: &id, Length: new(uint64)}

	buf := bytes.NewBuffer(make([]byte, opts.ChunkSize))
	buf.Reset()

	var cbuf *bytes.Buffer
	if opts.CompressionKind != pb.CompressionKind_NONE {
		cbuf = bytes.NewBuffer(make([]byte, opts.ChunkSize))
		cbuf.Reset()
	}

	return &writer{buf: buf, compressedBuf: cbuf, info: info, opts: opts, encoder: encoding.NewByteEncoder(opts.WriteIndex)}
}

func NewBoolWriter(id uint32, kind pb.Stream_Kind, opts *config.WriterOptions) Writer {
	info := &pb.Stream{Kind: &kind, Column: &id, Length: new(uint64)}

	buf := bytes.NewBuffer(make([]byte, opts.ChunkSize))
	buf.Reset()

	var cbuf *bytes.Buffer
	if opts.CompressionKind != pb.CompressionKind_NONE {
		cbuf = bytes.NewBuffer(make([]byte, opts.ChunkSize))
		cbuf.Reset()
	}
	return &writer{info: info, buf: buf, compressedBuf: cbuf, opts: opts, encoder: encoding.NewBoolEncoder(opts.WriteIndex)}
}

func NewStringContentsWriter(id uint32, kind pb.Stream_Kind, opts *config.WriterOptions) Writer {
	info := &pb.Stream{Kind: &kind, Column: &id, Length: new(uint64)}

	buf := bytes.NewBuffer(make([]byte, opts.ChunkSize))
	buf.Reset()

	var cbuf *bytes.Buffer
	if opts.CompressionKind != pb.CompressionKind_NONE {
		cbuf = bytes.NewBuffer(make([]byte, opts.ChunkSize))
		cbuf.Reset()
	}

	return &writer{buf: buf, compressedBuf: cbuf, info: info, opts: opts, encoder: encoding.NewStringContentsEncoder()}
}

func NewIntRLV2Writer(id uint32, kind pb.Stream_Kind, opts *config.WriterOptions, signed bool) Writer {
	info := &pb.Stream{Kind: &kind, Column: &id, Length: new(uint64)}

	buf := bytes.NewBuffer(make([]byte, opts.ChunkSize))
	buf.Reset()

	var cbuf *bytes.Buffer
	if opts.CompressionKind != pb.CompressionKind_NONE {
		cbuf = bytes.NewBuffer(make([]byte, opts.ChunkSize))
		cbuf.Reset()
	}

	return &writer{info: info, buf: buf, compressedBuf: cbuf, opts: opts, encoder: encoding.NewIntRLV2(signed, opts.WriteIndex)}
}

func NewFloatWriter(id uint32, kind pb.Stream_Kind, opts *config.WriterOptions, is64 bool) Writer {
	info := &pb.Stream{Kind: &kind, Column: &id, Length: new(uint64)}

	buf := bytes.NewBuffer(make([]byte, opts.ChunkSize))
	buf.Reset()

	var cbuf *bytes.Buffer
	if opts.CompressionKind != pb.CompressionKind_NONE {
		cbuf = bytes.NewBuffer(make([]byte, opts.ChunkSize))
		cbuf.Reset()
	}

	if is64 {
		return &writer{info: info, buf: buf, compressedBuf: cbuf, opts: opts, encoder: encoding.NewDoubleEncoder()}
	} else {
		return &writer{info: info, buf: buf, compressedBuf: cbuf, opts: opts, encoder: encoding.NewFloatEncoder()}
	}
}

func NewDateV2Writer(id uint32, kind pb.Stream_Kind, opts *config.WriterOptions) Writer {
	info := &pb.Stream{Kind: &kind, Column: &id, Length: new(uint64)}

	buf := bytes.NewBuffer(make([]byte, opts.ChunkSize))
	buf.Reset()

	var cbuf *bytes.Buffer
	if opts.CompressionKind != pb.CompressionKind_NONE {
		cbuf = bytes.NewBuffer(make([]byte, opts.ChunkSize))
		cbuf.Reset()
	}

	return &writer{info: info, buf: buf, compressedBuf: cbuf, opts: opts, encoder: encoding.NewDateV2Encoder(opts.WriteIndex)}
}
