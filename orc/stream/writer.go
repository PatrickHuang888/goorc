package stream

import (
	"bytes"
	"github.com/patrickhuang888/goorc/orc/common"
	"github.com/patrickhuang888/goorc/orc/config"
	"github.com/patrickhuang888/goorc/orc/encoding"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
	"io"
)

type Writer interface {
	//Reset()

	Write(v interface{}) error
	Flush() error

	WriteOut(out io.Writer) (n int64, err error)

	MarkPosition()
	GetAndClearPositions() [][]uint64

	Info() *pb.Stream
	//Size() int
}

type BoolWriter interface {
	Writer
	WriteBools(bb []bool) error
}

type ByteWriter interface {
	Writer
	WriteBytes(bb []byte) error
}

type IntWriter interface {
	Writer
	WriteInts(vec []int64) error
}

type encodingWriter struct {
	*writer
	encoder encoding.Encoder
}

// mark position and collect stats
func (w *encodingWriter) MarkPosition() {
	w.markPosition()
	w.encoder.MarkPosition()
}

func (w encodingWriter) GetAndClearPositions() [][]uint64 {
	var pp [][]uint64
	ep := w.encoder.GetAndClearPositions()
	for i, v := range w.positions {
		v = append(v, ep[i])
		pp = append(pp, v)
	}
	w.positions = w.positions[:0]
	return pp
}

// info will update after flush
func (w encodingWriter) Info() *pb.Stream {
	if !w.flushed {
		panic("not flushed")
	}
	return w.info
}

func (w *encodingWriter) Reset() {
	w.reset()
	w.encoder.Reset()
}

func (w *encodingWriter) Write(v interface{}) error {
	var err error
	var data []byte

	if data, err = w.encoder.Encode(v); err != nil {
		return err
	}

	return w.write(data)
}

func (w *encodingWriter) WriteBytes(bb []byte) error  {
	for _, v := range bb {
		if err:=w.Write(v);err!=nil {
			return err
		}
	}
	return nil
}

func (w *encodingWriter) WriteBools(bb []bool) error  {
	for _, v := range bb {
		if err:=w.Write(v);err!=nil {
			return err
		}
	}
	return nil
}

func (w *encodingWriter) Flush() error {
	var err error
	var data []byte

	if data, err = w.encoder.Flush(); err != nil {
		return err
	}

	if err = w.write(data); err != nil {
		return err
	}

	return w.flush()
}

type writer struct {
	info *pb.Stream
	flushed bool

	buf           *bytes.Buffer
	compressedBuf *bytes.Buffer

	opts *config.WriterOptions

	positions [][]uint64
}

// Write write p to stream buf
func (w *writer) write(p []byte) error {
	if len(p) == 0 {
		return nil
	}

	if _, err := w.buf.Write(p); err != nil {
		return err
	}

	if w.opts.CompressionKind == pb.CompressionKind_NONE || w.buf.Len() < w.opts.ChunkSize {
		return nil
	}

	if err := common.CompressingChunks(w.opts.CompressionKind, w.opts.ChunkSize, w.compressedBuf, w.buf); err != nil {
		return err
	}

	// compacting compressed buf
	w.buf = bytes.NewBuffer(w.buf.Bytes())
	return nil
}

func (w *writer) reset() {
	*w.info.Length = 0
	w.flushed= false

	w.buf.Reset()
	w.compressedBuf.Reset()

	w.positions = w.positions[:0]
}

func (w *writer) markPosition() {
	if w.empty() {
		return
	}

	if w.opts.CompressionKind == pb.CompressionKind_NONE {
		w.positions = append(w.positions, []uint64{uint64(w.buf.Len())})
		return
	}

	w.positions = append(w.positions, []uint64{uint64(w.compressedBuf.Len()), uint64(w.buf.Len())})
}

func (w writer) empty() bool {
	return w.compressedBuf.Len() == 0 && w.buf.Len() == 0
}

// flush data to buffer and update stream information when flush stripe
func (w *writer) flush() error {
	if w.opts.CompressionKind == pb.CompressionKind_NONE {
		*w.info.Length = uint64(w.buf.Len())
		w.flushed= true
		return nil
	}

	// compressing remaining
	if w.buf.Len() != 0 {
		if err := common.CompressingLeft(w.opts.CompressionKind, w.opts.ChunkSize, w.compressedBuf, w.buf); err != nil {
			return err
		}
	}
	*w.info.Length = uint64(w.compressedBuf.Len())
	w.flushed= true
	return nil
}

// used together with flush
func (w *writer) WriteOut(out io.Writer) (n int64, err error) {
	if w.opts.CompressionKind == pb.CompressionKind_NONE {
		if n, err = w.buf.WriteTo(out); err != nil {
			return 0, errors.WithStack(err)
		}
		return
	}

	if n, err = w.compressedBuf.WriteTo(out); err != nil {
		return 0, errors.WithStack(err)
	}
	return
}

// return compressed data + uncompressed data
/*func (w *writer) Size() int {
	if w.opts.CompressionKind == pb.CompressionKind_NONE {
		return w.buf.Len()
	}

	return w.compressedBuf.Len() + w.buf.Len()
}*/

func NewByteWriter(id uint32, kind pb.Stream_Kind, opts *config.WriterOptions) ByteWriter {
	info := &pb.Stream{Kind: &kind, Column: &id, Length: new(uint64)}

	buf := bytes.NewBuffer(make([]byte, opts.ChunkSize))
	buf.Reset()

	var cbuf *bytes.Buffer
	if opts.CompressionKind != pb.CompressionKind_NONE {
		cbuf = bytes.NewBuffer(make([]byte, opts.ChunkSize))
		cbuf.Reset()
	}

	return &encodingWriter{&writer{buf: buf, compressedBuf: cbuf, info: info, opts: opts}, encoding.NewByteEncoder()}
}

func NewBoolWriter(id uint32, kind pb.Stream_Kind, opts *config.WriterOptions) BoolWriter {
	info := &pb.Stream{Kind: &kind, Column: &id, Length: new(uint64)}

	buf := bytes.NewBuffer(make([]byte, opts.ChunkSize))
	buf.Reset()

	var cbuf *bytes.Buffer
	if opts.CompressionKind != pb.CompressionKind_NONE {
		cbuf = bytes.NewBuffer(make([]byte, opts.ChunkSize))
		cbuf.Reset()
	}

	return &encodingWriter{&writer{info: info, buf: buf, compressedBuf: cbuf, opts: opts}, encoding.NewBoolEncoder()}
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

	return &encodingWriter{&writer{info: info, buf: buf, compressedBuf: cbuf, opts: opts}, encoding.NewIntRLV2(signed)}
}
