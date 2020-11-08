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

type Writer struct {
	*writer
	encoder encoding.Encoder
}

// mark position and collect stats, will not invoked when not write index
func (w *Writer) MarkPosition() {
	w.markPosition()
	w.encoder.MarkPosition()
}

func (w Writer) GetPositions() [][]uint64 {
	if !w.opts.WriteIndex {
		return [][]uint64{}
	}

	var pp [][]uint64
	pp = append(pp, w.positions...)
	pp = append(pp, w.encoder.GetPositions()...)

	w.positions = w.positions[:0]
	return pp
}

// info will update after flush
func (w Writer) Info() *pb.Stream {
	return w.info
}

func (w *Writer) Reset() {
	w.reset()
	w.encoder.Reset()
}

func (w *Writer) Write(v interface{}) (err error) {
	if err = w.encoder.Encode(v); err != nil {
		return
	}

	if w.encoder.BufferedSize() >= w.opts.EncoderBufferSize {
		data, err := w.encoder.Flush()
		if err != nil {
			return
		}
		if err := w.write(data); err != nil {
			return err
		}
		if w.opts.CompressionKind == pb.CompressionKind_NONE {
			*w.info.Length = uint64(w.buf.Len())
		} else {
			*w.info.Length = uint64(w.compressedBuf.Len())
		}
	}
	return
}

// return compressed data + uncompressed data
func (w *Writer) Size() (size int) {
	data, _ := w.encoder.Flush()
	// just write to buf here, no compressing
	// fix: direct operating on w.buf
	w.buf.Write(data)

	if w.opts.CompressionKind == pb.CompressionKind_NONE {
		size = w.buf.Len()
		return
	}

	size = w.compressedBuf.Len() + w.buf.Len()
	return
}

// Flush flush remaining data in encoder then compressing left encoded data
// will be the last operation before writeout
func (w *Writer) Flush() error {
	data, err := w.encoder.Flush()
	if err != nil {
		return err
	}
	if err := w.write(data); err != nil {
		return err
	}

	if w.opts.CompressionKind == pb.CompressionKind_NONE {
		*w.info.Length = uint64(w.buf.Len())
	} else {
		if err := common.CompressingLeft(w.opts.CompressionKind, w.opts.ChunkSize, w.compressedBuf, w.buf); err != nil {
			return err
		}
		*w.info.Length = uint64(w.compressedBuf.Len())
	}
	return nil
}

type writer struct {
	info *pb.Stream

	buf           *bytes.Buffer
	compressedBuf *bytes.Buffer

	opts *config.WriterOptions

	positions [][]uint64
}

// Write write p to stream buf, compressing if buf size larger than chunk size
func (w *writer) write(p []byte) error {
	if len(p) == 0 {
		return nil
	}

	w.buf.Write(p)

	if w.opts.CompressionKind == pb.CompressionKind_NONE || w.buf.Len() < w.opts.ChunkSize {
		return nil
	}

	if err := common.CompressingChunks(w.opts.CompressionKind, w.opts.ChunkSize, w.compressedBuf, w.buf); err != nil {
		return err
	}
	*w.info.Length = uint64(w.compressedBuf.Len())

	// compacting compressed buf
	w.buf = bytes.NewBuffer(w.buf.Bytes())
	return nil
}

func (w *writer) reset() {
	*w.info.Length = 0

	w.buf.Reset()
	w.compressedBuf.Reset()

	w.positions = w.positions[:0]
}

func (w *writer) markPosition() {
	if w.opts.CompressionKind == pb.CompressionKind_NONE {
		w.positions = append(w.positions, []uint64{uint64(w.buf.Len())})
		return
	}
	w.positions = append(w.positions, []uint64{uint64(w.compressedBuf.Len()), uint64(w.buf.Len())})
}

/*func (w writer) empty() bool {
	return w.compressedBuf.Len() == 0 && w.buf.Len() == 0
}*/

// compressing data in memory, update stream info length
func (w *writer) compressing() error {
	/*if w.opts.CompressionKind == pb.CompressionKind_NONE {
		*w.info.Length = uint64(w.buf.Len())
		return nil
	}*/

	if w.buf.Len() != 0 {
		if err := common.CompressingLeft(w.opts.CompressionKind, w.opts.ChunkSize, w.compressedBuf, w.buf); err != nil {
			return err
		}
	}
	//*w.info.Length = uint64(w.compressedBuf.Len())
	return nil
}

// used together with flush
func (w *Writer) WriteOut(out io.Writer) (n int64, err error) {
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

func NewByteWriter(id uint32, kind pb.Stream_Kind, opts *config.WriterOptions) *Writer {
	info := &pb.Stream{Kind: &kind, Column: &id, Length: new(uint64)}

	buf := bytes.NewBuffer(make([]byte, opts.ChunkSize))
	buf.Reset()

	var cbuf *bytes.Buffer
	if opts.CompressionKind != pb.CompressionKind_NONE {
		cbuf = bytes.NewBuffer(make([]byte, opts.ChunkSize))
		cbuf.Reset()
	}

	return &Writer{&writer{buf: buf, compressedBuf: cbuf, info: info, opts: opts}, encoding.NewByteEncoder()}
}

func NewBoolWriter(id uint32, kind pb.Stream_Kind, opts *config.WriterOptions) *Writer {
	info := &pb.Stream{Kind: &kind, Column: &id, Length: new(uint64)}

	buf := bytes.NewBuffer(make([]byte, opts.ChunkSize))
	buf.Reset()

	var cbuf *bytes.Buffer
	if opts.CompressionKind != pb.CompressionKind_NONE {
		cbuf = bytes.NewBuffer(make([]byte, opts.ChunkSize))
		cbuf.Reset()
	}

	return &Writer{&writer{info: info, buf: buf, compressedBuf: cbuf, opts: opts}, encoding.NewBoolEncoder()}
}

func NewIntRLV2Writer(id uint32, kind pb.Stream_Kind, opts *config.WriterOptions, signed bool) *Writer {
	info := &pb.Stream{Kind: &kind, Column: &id, Length: new(uint64)}

	buf := bytes.NewBuffer(make([]byte, opts.ChunkSize))
	buf.Reset()

	var cbuf *bytes.Buffer
	if opts.CompressionKind != pb.CompressionKind_NONE {
		cbuf = bytes.NewBuffer(make([]byte, opts.ChunkSize))
		cbuf.Reset()
	}

	return &Writer{&writer{info: info, buf: buf, compressedBuf: cbuf, opts: opts}, encoding.NewIntRLV2(signed)}
}
