package orc

import (
	"bytes"
	"compress/flate"
	"github.com/PatrickHuang888/goorc/pb/pb"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"os"
)

const (
	MIN_ROW_INDEX_STRIDE         = 1000
	STRIPE_LIMIT                 = 256 * 1024 * 1024
	DEFAULT_INDEX_SIZE           = 100 * 1024
	DEFAULT_PRESENT_SIZE         = 100 * 1024
	DEFAULT_DATA_SIZE            = 1 * 1024 * 1024
	DEFAULT_LENGTH_SIZE          = 100 * 1024
	DEFAULT_ENCODING_BUFFER_SIZE = 100 * 1024
	DEFAULT_CHUNK_SIZE           = 256 * 1024
	MAX_CHUNK_LENGTH             = uint64(32768) // 15 bit
)

type WriterOptions struct {
	chunkSize  uint64
	cmpKind    pb.CompressionKind
	stripeSize int
}

func DefaultWriterOptions() *WriterOptions {
	o := &WriterOptions{}
	o.cmpKind = pb.CompressionKind_ZLIB
	o.stripeSize = STRIPE_LIMIT
	o.chunkSize = DEFAULT_CHUNK_SIZE
	return o
}

type Writer interface {
	GetSchema() *TypeDescription

	Write(batch ColumnVector) error

	Close() error
}

// cannot used concurrently, not synchronized
// strip buffered in memory until the strip size
// write out by columns
type writer struct {
	path   string
	f      *os.File
	offset uint64

	ps *pb.PostScript

	// current stripe and info
	stripe *stripeWriter

	stripeInfos []*pb.StripeInformation
	columnStats []*pb.ColumnStatistics

	opts *WriterOptions

	schemas []*TypeDescription
}

type stripeWriter struct {
	schemas    []*TypeDescription
	opts *WriterOptions

	// streams <id, stream{present, data, length}>
	streams map[uint32][]*streamWriter

	idxBuf *bytes.Buffer // index area buffer
	//dataBuf *bytes.Buffer // data area buffer

	info *pb.StripeInformation
}

type streamWriter struct {
	info     *pb.Stream
	encoding *pb.ColumnEncoding
	enc      Encoder
	buf      *bytes.Buffer
	cmpBuf   *bytes.Buffer // accumulated compressed buffer
}

func NewWriter(path string, schema *TypeDescription, opts *WriterOptions) (Writer, error) {
	// fixme: create new one, error when exist
	log.Infof("open %s", path)
	f, err := os.Create(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	w := &writer{opts: opts, path: path, f: f, schemas:schema.normalize()}
	n, err := w.writeHeader()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	w.offset = n
	return w, nil
}

func newStripeWriter(offset uint64, schemas []*TypeDescription, opts *WriterOptions) *stripeWriter {
	idxBuf := bytes.NewBuffer(make([]byte, DEFAULT_INDEX_SIZE))
	idxBuf.Reset()
	ss := make(map[uint32][]*streamWriter)
	si := &pb.StripeInformation{}
	var r uint64
	si.NumberOfRows = &r
	o := offset
	si.Offset = &o
	stp := &stripeWriter{opts: opts, idxBuf: idxBuf, streams: ss, info: si, schemas:schemas}
	return stp
}

func (w *writer) Write(cv ColumnVector) error {
	// todo: verify cv type and column type
	if w.stripe == nil {
		w.stripe = newStripeWriter(w.offset, w.schemas, w.opts)
	}
	if err := w.stripe.write(cv); err != nil {
		return errors.WithStack(err)
	}

	if err := w.flushStripe(false); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (w *writer) flushStripe(force bool) error {
	// fixme: assuming 1 stripe all in memory
	// a stripe should contains whole row
	stp := w.stripe
	if stp.shouldFlush() || force {
		if err := stp.flush(w.f); err != nil {
			return errors.WithStack(err)
		}
		// todo: update column stats
		// reset current stripe
		w.offset += stp.info.GetOffset() + stp.info.GetIndexLength() + stp.info.GetDataLength()
		w.stripeInfos = append(w.stripeInfos, stp.info)
		log.Debugf("flushed stripe %v", stp.info)
		stp = newStripeWriter(w.offset, w.schemas, w.opts)
		w.stripe = stp
	}
	return nil
}

func (stp *stripeWriter) shouldFlush() bool {
	var l uint64
	for _, td := range stp.schemas {
		for _, s := range stp.streams[td.Id] {
			if s != nil {
				l += uint64(s.cmpBuf.Len())
			}
		}
	}
	return l >= stp.opts.chunkSize
}

func (stp *stripeWriter) write(cv ColumnVector) error {
	switch stp.schemas[cv.ColumnId()].Kind {
	case pb.Type_STRUCT:
		return errors.New("struct not impl")
	case pb.Type_LONG:
		lcv, ok := cv.(*BigIntColumn)
		if !ok {
			return errors.New("column type int should be vector long")
		}
		if _, ok := stp.streams[cv.ColumnId()]; !ok {
			stp.streams[cv.ColumnId()] = make([]*streamWriter, 2)
		}

		// todo: present stream

		// write data stream
		dtStm := stp.streams[cv.ColumnId()][1]
		if dtStm == nil {
			info := &pb.Stream{Kind: new(pb.Stream_Kind), Column: new(uint32), Length: new(uint64)}
			*info.Kind = pb.Stream_DATA
			*info.Column = cv.ColumnId()
			buf := bytes.NewBuffer(make([]byte, stp.opts.chunkSize))
			buf.Reset()
			cmpBuf := bytes.NewBuffer(make([]byte, stp.opts.chunkSize))
			cmpBuf.Reset()
			enc := &intRleV2{signed: true}
			dv2 := pb.ColumnEncoding_DIRECT_V2
			encoding := &pb.ColumnEncoding{Kind: &dv2}
			dtStm = &streamWriter{info: info, buf: buf, cmpBuf: cmpBuf, encoding: encoding, enc: enc}
			stp.streams[cv.ColumnId()][1] = dtStm
		}
		if err := dtStm.writeInts(lcv.GetVector()); err != nil {
			return errors.WithStack(err)
		}
		_, err := compressTo(stp.opts.cmpKind, stp.opts.chunkSize, dtStm.buf, dtStm.cmpBuf)
		if err != nil {
			return errors.Wrap(err, "compressing data stream error")
		}

	case pb.Type_STRING:
		scv, ok := cv.(*StringColumn)
		if !ok {
			return errors.New("column type stream should vector string")
		}

		var lghV []uint64 // length vector
		var cntV [][]byte  // content vector
		for _, s := range scv.GetVector() {
			lghV = append(lghV, uint64(len(s))) // len return bytes
			cntV = append(cntV, []byte(s))        // convert into bytes encoding utf-8
		}

		if _, ok := stp.streams[cv.ColumnId()]; !ok {
			stp.streams[cv.ColumnId()] = make([]*streamWriter, 3)
		}

		// todo: present stream

		// write data stream
		dtStm := stp.streams[cv.ColumnId()][1]
		if dtStm == nil {
			info := &pb.Stream{Kind: new(pb.Stream_Kind), Column: new(uint32), Length: new(uint64)}
			*info.Kind = pb.Stream_DATA
			*info.Column = cv.ColumnId()
			buf := bytes.NewBuffer(make([]byte, stp.opts.chunkSize))
			buf.Reset()
			cmpBuf := bytes.NewBuffer(make([]byte, stp.opts.chunkSize))
			cmpBuf.Reset()
			enc := &bytesDirectV2{}
			dv2 := pb.ColumnEncoding_DIRECT_V2
			encoding := &pb.ColumnEncoding{Kind: &dv2}
			dtStm = &streamWriter{info: info, buf: buf, cmpBuf: cmpBuf, encoding: encoding, enc: enc}
			stp.streams[cv.ColumnId()][1] = dtStm
		}

		if err := dtStm.writeBytesDirectV2(cntV); err != nil {
			return errors.WithStack(err)
		}
		_, err := compressTo(stp.opts.cmpKind, stp.opts.chunkSize, dtStm.buf, dtStm.cmpBuf)
		if err != nil {
			return errors.Wrap(err, "compressing data stream error")
		}

		// write length stream
		lghStm := stp.streams[cv.ColumnId()][2]
		if lghStm == nil {
			info := &pb.Stream{Kind: new(pb.Stream_Kind), Column: new(uint32), Length: new(uint64)}
			*info.Kind = pb.Stream_LENGTH
			*info.Column = cv.ColumnId()
			buf := bytes.NewBuffer(make([]byte, stp.opts.chunkSize))
			buf.Reset()
			cmpBuf := bytes.NewBuffer(make([]byte, stp.opts.chunkSize))
			cmpBuf.Reset()
			enc := &intRleV2{}
			dv2 := pb.ColumnEncoding_DIRECT_V2
			encoding := &pb.ColumnEncoding{Kind: &dv2}
			lghStm = &streamWriter{info: info, buf: buf, cmpBuf: cmpBuf, encoding: encoding, enc: enc}
			stp.streams[cv.ColumnId()][2] = lghStm
		}
		if err := lghStm.writeUints(lghV); err != nil {
			return errors.WithStack(err)
		}
		if _, err := compressTo(stp.opts.cmpKind, stp.opts.chunkSize, lghStm.buf, lghStm.cmpBuf); err != nil {
			return errors.Wrap(err, "compressing data stream error")
		}

	default:
		return errors.New("no impl")
	}

	*stp.info.NumberOfRows += uint64(cv.Rows())
	return nil
}

// 1 stripe should be self-contained
func (stp *stripeWriter) flush(f *os.File) error {
	// row number updated at write
	// write index
	idxL := uint64(stp.idxBuf.Len())
	// buf will be reset after writeTo
	_, err := stp.idxBuf.WriteTo(f)
	if err != nil {
		return errors.WithStack(err)
	}
	log.Debugf("flush index with %d", idxL)

	// write data streams
	var dataL uint64
	for _, td := range stp.schemas {
		for _, s := range stp.streams[td.Id] {
			if s != nil {
				*s.info.Length = uint64(s.cmpBuf.Len())
				log.Tracef("write stream %s of column %d length %d", s.info.GetKind().String(),
					s.info.GetColumn(), *s.info.Length)
				n, err := s.cmpBuf.WriteTo(f)
				if err != nil {
					return errors.WithStack(err)
				}
				dataL += uint64(n)
			}
		}
	}
	log.Debugf("flush data stream with %d", dataL)

	// write stripe footer
	sf := &pb.StripeFooter{}
	for i := 0; i < len(stp.schemas); i++ {
		ss := stp.streams[uint32(i)]
		for _, s := range ss {
			if s != nil {
				sf.Streams = append(sf.Streams, s.info)
				sf.Columns = append(sf.Columns, s.encoding)
			}
		}
	}
	sfm, err := proto.Marshal(sf)
	if err != nil {
		return errors.WithStack(err)
	}
	sfCmpBuf, err := compressByteSlice(stp.opts.cmpKind, stp.opts.chunkSize, sfm)
	if err != nil {
		return errors.WithStack(err)
	}
	ftLength := uint64(len(sfCmpBuf))
	if _, err := f.Write(sfCmpBuf); err != nil {
		return errors.WithStack(err)
	}
	log.Debugf("write stripe footer, length: %d", ftLength)

	stp.info.IndexLength = &idxL
	stp.info.DataLength = &dataL
	stp.info.FooterLength = &ftLength

	return nil
}

func (stm *streamWriter) writeUints(v []uint64) error {
	irl := stm.enc.(*intRleV2)
	irl.reset()
	irl.signed = false
	irl.uliterals = v
	stm.buf.Reset()
	if err := irl.writeValues(stm.buf); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
func (stm *streamWriter) writeInts(v []int64) error {
	irl := stm.enc.(*intRleV2)
	irl.reset()
	irl.signed = true
	irl.literals = v
	stm.buf.Reset()
	if err := irl.writeValues(stm.buf); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (stm *streamWriter) writeBytesDirectV2(bs [][]byte) error {
	// refactor: will not doing copy, just append to buffer ?
	enc := stm.enc.(*bytesDirectV2)
	enc.reset()
	enc.content= bs
	stm.buf.Reset()
	if err:= enc.writeValues(stm.buf); err!=nil {
		return errors.WithStack(err)
	}
	return nil
}

func (stm *streamWriter) reset() {
	*stm.info.Length = 0
	stm.buf.Reset()
}

func (w *writer) GetSchema() *TypeDescription {
	return w.schemas[0]
}

func (w *writer) Close() error {
	if err := w.flushStripe(true); err != nil {
		return errors.WithStack(err)
	}
	if err := w.writeFileTail(); err != nil {
		return errors.WithStack(err)
	}

	w.f.Close()
	return nil
}

func (w *writer) writeHeader() (uint64, error) {
	b := []byte(MAGIC)
	if _, err := w.f.Write(b); err != nil {
		return 0, errors.WithStack(err)
	}
	return uint64(len(b)), nil
}

func (w *writer) writeFileTail() error {
	// write footer
	// todo: rowsinstrde
	ft := &pb.Footer{HeaderLength: new(uint64), ContentLength: new(uint64), NumberOfRows: new(uint64)}
	*ft.HeaderLength = 3 // always 3
	for _, si := range w.stripeInfos {
		*ft.ContentLength += si.GetIndexLength() + si.GetDataLength() + si.GetFooterLength()
		*ft.NumberOfRows += si.GetNumberOfRows()
	}
	ft.Stripes = w.stripeInfos
	ft.Types = schemasToTypes(w.schemas)

	// metadata

	// statistics

	ftb, err := proto.Marshal(ft)
	if err != nil {
		return errors.Wrap(err, "marshall footer error")
	}
	ftCmpBuf, err := compressByteSlice(w.opts.cmpKind, w.opts.chunkSize, ftb)
	if err != nil {
		return errors.WithStack(err)
	}
	ftl := uint64(len(ftCmpBuf))
	if _, err := w.f.Write(ftCmpBuf); err != nil {
		return errors.WithStack(err)
	}
	log.Debugf("write footer with length: %d", ftl)

	// write postscript
	ps := &pb.PostScript{}
	ps.FooterLength = &ftl
	ps.Compression = &w.opts.cmpKind
	ps.CompressionBlockSize = &w.opts.chunkSize
	m := MAGIC
	ps.Magic = &m
	psb, err := proto.Marshal(ps)
	if err != nil {
		return errors.WithStack(err)
	}
	n, err := w.f.Write(psb)
	if err != nil {
		return errors.Wrap(err, "write PS error")
	}
	log.Debugf("write postscript with length %d", n)
	// last byte is ps length
	if _, err = w.f.Write([]byte{byte(n)}); err != nil {
		return errors.Wrap(err, "write PS length error")
	}

	return nil
}

// compressing src buf into dst, maybe to several chunks
func compressTo(kind pb.CompressionKind, chunkSize uint64, src *bytes.Buffer, dst *bytes.Buffer) (cmpLength int64, err error) {
	switch kind {
	case pb.CompressionKind_ZLIB:
		srcBytes := src.Bytes()
		chunkLength := MinUint64(MAX_CHUNK_LENGTH, chunkSize)
		buf := bytes.NewBuffer(make([]byte, chunkLength))
		buf.Reset()
		w, err := flate.NewWriter(buf, -1)
		if err != nil {
			return 0, errors.WithStack(err)
		}
		srcLen := src.Len()
		if uint64(srcLen) < chunkLength {
			if _, err := src.WriteTo(w); err != nil {
				return 0, errors.WithStack(err)
			}
			if err = w.Close(); err != nil {
				return 0, errors.WithStack(err)
			}
			var header []byte
			orig := buf.Len() >= srcLen
			if orig {
				header = encChunkHeader(srcLen, orig)
			} else {
				header = encChunkHeader(buf.Len(), orig)
			}
			if _, err = dst.Write(header); err != nil {
				return 0, err
			}
			if orig {
				if n, err := dst.Write(srcBytes); err != nil {
					return int64(n), errors.WithStack(err)
				}
			} else {
				if n, err := buf.WriteTo(dst); err != nil {
					return n, errors.WithStack(err)
				}
			}

		} else {
			// todo: several chunk
			return 0, errors.New("no impl")
		}

	default:
		return 0, errors.New("compression other than zlib not impl")
	}
	return
}

func encChunkHeader(l int, orig bool) (header []byte) {
	header = make([]byte, 3)
	if orig {
		header[0] = 0x01 | byte(l<<1)
	} else {
		header[0] = byte(l << 1)
	}
	header[1] = byte(l >> 7)
	header[2] = byte(l >> 15)
	return
}

func decChunkHeader(h []byte) (length int, orig bool) {
	_ = h[2]
	return int(h[2])<<15 | int(h[1])<<7 | int(h[0])>>1, h[0]&0x01 == 0x01
}

// compress byte slice into chunk slice, used in stripe footer, tail footer
// thinking should be smaller than chunksize
func compressByteSlice(kind pb.CompressionKind, chunkSize uint64, b []byte) (compressed []byte, err error) {
	switch kind {
	case pb.CompressionKind_ZLIB:
		src := bytes.NewBuffer(b)
		dst := bytes.NewBuffer(make([]byte, len(b)))
		dst.Reset()
		if _, err = compressTo(kind, chunkSize, src, dst); err != nil {
			return nil, err
		}
		return dst.Bytes(), nil

	default:
		return nil, errors.New("compression other than zlib not impl")
	}
	return
}
