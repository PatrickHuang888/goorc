package orc

import (
	"bytes"
	"compress/flate"
	"flag"
	"github.com/PatrickHuang888/goorc/pb/pb"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"os"
)

func init() {

}

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
	schemas    []*TypeDescription
	chunkSize  uint64
	cmpKind    pb.CompressionKind
	stripeSize int
	logLevel   log.Level
}

func NewWriterOptions(schema *TypeDescription) *WriterOptions {
	debug := flag.Bool("debug", false, "-debug")
	flag.Parse()
	o := &WriterOptions{logLevel: log.InfoLevel}
	o.cmpKind = pb.CompressionKind_ZLIB
	o.stripeSize = STRIPE_LIMIT
	o.chunkSize = DEFAULT_CHUNK_SIZE
	o.schemas = schema.normalize()
	if *debug {
		o.logLevel = log.DebugLevel
		log.SetLevel(o.logLevel)
	}
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
}

type stripeWriter struct {
	opts *WriterOptions

	// streams <id, stream{present, data, length}>
	streams map[uint32][3]*streamWriter

	idxBuf  *bytes.Buffer // index area buffer
	dataBuf *bytes.Buffer // data area buffer

	info *pb.StripeInformation
}

type streamWriter struct {
	info     *pb.Stream
	encoding *pb.ColumnEncoding
	enc      Encoder
	buf      *bytes.Buffer
	cmpBuf   *bytes.Buffer // accumulated compressed buffer
}

func NewWriter(path string, opts *WriterOptions) (Writer, error) {
	// fixme: create new one, error when exist
	log.Infof("open %s", path)
	f, err := os.Create(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	stp := newStripeWriter(opts)
	w := &writer{opts: opts, path: path, f: f, stripe: stp}
	n, err := w.writeHeader()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	w.offset = n
	return w, nil
}

func newStripeWriter(opts *WriterOptions) *stripeWriter {
	idxBuf := bytes.NewBuffer(make([]byte, DEFAULT_INDEX_SIZE))
	idxBuf.Reset()
	pstBuf := bytes.NewBuffer(make([]byte, DEFAULT_PRESENT_SIZE))
	pstBuf.Reset()
	dataBuf := bytes.NewBuffer(make([]byte, DEFAULT_DATA_SIZE))
	dataBuf.Reset()
	lghBuf := bytes.NewBuffer(make([]byte, DEFAULT_LENGTH_SIZE))
	lghBuf.Reset()
	ss := make(map[uint32][3]*streamWriter)
	si := &pb.StripeInformation{Offset: new(uint64), IndexLength: new(uint64), DataLength: new(uint64),
		FooterLength: new(uint64), NumberOfRows: new(uint64)}
	stp := &stripeWriter{opts: opts, idxBuf: idxBuf, pstBuf: pstBuf, dataBuf: dataBuf, lghBuf: lghBuf,
		streams: ss, info: si}
	return stp
}

func (w *writer) Write(cv ColumnVector) error {
	// todo: verify cv type and column type

	stp := w.stripe
	if err := stp.write(cv); err != nil {
		return errors.WithStack(err)
	}
	// refactor: update stripe info in 1 place?
	*stp.info.NumberOfRows += uint64(cv.Rows())

	if w.shouldFlush() {
		if err := w.flushStripe(); err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

func (w *writer) shouldFlush() bool {
	return w.stripe.idxBuf.Len()+w.stripe.dataBuf.Len()+w.stripe.lghBuf.Len() > w.opts.stripeSize
}

func (w *writer) flushStripe() error {
	log.Debugln("flush stripe")
	// fixme: assuming 1 stripe all in memory
	// a stripe should contains whole row
	stp := w.stripe
	if err := stp.flushStripe(w.f); err != nil {
		return errors.WithStack(err)
	}
	// todo: update column stats
	// reset current stripe
	w.offset += stp.info.GetOffset() + stp.info.GetIndexLength() + stp.info.GetDataLength()
	w.stripeInfos = append(w.stripeInfos, stp.info)
	stp.reset()
	*stp.info.Offset = w.offset
	return nil
}

func (stp *stripeWriter) reset() {
	stp.idxBuf.Reset()
	stp.dataBuf.Reset()

	*stp.info.Offset = 0
	*stp.info.FooterLength = 0
	*stp.info.DataLength = 0
	*stp.info.NumberOfRows = 0

	for _, v := range stp.streams {
		if v[0] != nil {
			v[0].reset()
		}
		v[1].reset()
		if v[2] != nil {
			v[2].reset()
		}
	}
}

func (stp *stripeWriter) write(cv ColumnVector) error {
	switch stp.opts.schemas[cv.ColumnId()].Kind {
	case pb.Type_STRUCT:
		return errors.New("struct not impl")
	case pb.Type_INT:
		if _, ok := stp.streams[cv.ColumnId()]; !ok {
			var ss [3]*streamWriter
			stp.streams[cv.ColumnId()] = ss
		}
		ss := stp.streams[cv.ColumnId()]
		// data stream
		if ss[1] == nil {
			info := &pb.Stream{Kind: new(pb.Stream_Kind), Column: new(uint32), Length: new(uint64)}
			*info.Kind = pb.Stream_DATA
			*info.Column = uint32(cv.ColumnId())
			buf := bytes.NewBuffer(make([]byte, stp.opts.chunkSize))
			buf.Reset()
			enc := &intRleV2{signed: true}
			dv2 := pb.ColumnEncoding_DIRECT_V2
			encoding := &pb.ColumnEncoding{Kind: &dv2}
			sw := &streamWriter{info: info, buf: buf, encoding: encoding, enc: enc}
			ss[1] = sw
			stp.streams[cv.ColumnId()] = ss
		}
		v, ok := cv.(*LongColumnVector)
		if !ok {
			return errors.New("column type int should be vector long")
		}
		stm := ss[1]
		// write data stream
		if err := stm.writeIrlV2(v); err != nil {
			return errors.WithStack(err)
		}
		_, err := compressTo(stp.opts.cmpKind, stp.opts.chunkSize, stm.buf, stm.cmpBuf)
		if err != nil {
			return errors.Wrap(err, "compressing data stream error")
		}
		//*stm.info.Length += uint64(n)

	default:
		return errors.New("no impl")
	}
	return nil
}

// 1 stripe should be self-contained
func (stp *stripeWriter) flushStripe(f *os.File) error {
	// row number updated at write
	idxL := uint64(stp.idxBuf.Len())
	// buf will be reset after writeTo
	_, err := stp.idxBuf.WriteTo(f)
	if err != nil {
		return errors.WithStack(err)
	}
	log.Debugf("flush index with %d", idxL)

	// write data area
	for _, td := range stp.opts.schemas {
		for _, stm := range stp.streams[td.Id] {
			stm.cmpBuf.WriteTo(stp.dataBuf)
		}
	}
	dataL := uint64(stp.dataBuf.Len())
	if _, err = stp.dataBuf.WriteTo(f); err != nil {
		return errors.WithStack(err)
	}
	log.Debugf("flush data stream with %d", dataL)
	stp.dataBuf.Reset()

	// write stripe footer
	sf := &pb.StripeFooter{}
	for i := 0; i < len(stp.opts.schemas); i++ {
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
	b, err := compressByteSlice(stp.opts.cmpKind, stp.opts.chunkSize, sfm)
	if err != nil {
		return errors.WithStack(err)
	}
	ftLength := uint64(len(b))
	if _, err := f.Write(b); err != nil {
		return errors.WithStack(err)
	}
	log.Debugf("write stripe footer, length: %d", ftLength)

	// refactor: stripe info field value setting scattering everywhere
	*stp.info.IndexLength = idxL
	*stp.info.DataLength = pstL + dataL + pstL
	*stp.info.FooterLength = ftLength

	return nil
}

func (stm *streamWriter) writeIrlV2(lcv *LongColumnVector) error {
	vector := lcv.GetVector()
	irl := stm.enc.(*intRleV2)
	irl.reset()
	irl.signed = true
	irl.literals = vector
	irl.numLiterals = len(vector)
	stm.buf.Reset()
	if err := irl.writeValues(stm.buf); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (stm *streamWriter) reset() {
	*stm.info.Length = 0
	stm.buf.Reset()
}

func (w *writer) GetSchema() *TypeDescription {
	return w.opts.schemas[0]
}

func (w *writer) Close() error {
	if err := w.flushStripe(); err != nil {
		return errors.WithStack(err)
	}
	if err := w.writeFileTail(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (w *writer) createRowIndexEntry() error {
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
	// todo: rowsinstrde
	ft := &pb.Footer{HeaderLength: new(uint64), ContentLength: new(uint64), NumberOfRows: new(uint64)}
	*ft.HeaderLength = 3 // always 3
	for _, v := range w.stripeInfos {
		*ft.ContentLength += v.GetIndexLength() + v.GetDataLength() + v.GetFooterLength()
		*ft.NumberOfRows += v.GetNumberOfRows()
	}
	ft.Stripes = w.stripeInfos
	ft.Types = schemasToTypes(w.opts.schemas)

	// metadata

	// statistics

	ftb, err := proto.Marshal(ft)
	if err != nil {
		return errors.Wrap(err, "marshall footer error")
	}
	b, err := compressByteSlice(w.opts.cmpKind, w.opts.chunkSize, ftb)
	if err != nil {
		return errors.WithStack(err)
	}
	ftl := uint64(len(b))
	if _, err := w.f.Write(b); err != nil {
		return errors.WithStack(err)
	}
	log.Debugf("write footer with length: %d", ftl)

	// write postscript
	ps := &pb.PostScript{FooterLength: new(uint64), Compression: new(pb.CompressionKind),
		CompressionBlockSize: new(uint64), Magic: new(string)}
	*ps.FooterLength = ftl
	*ps.Compression = w.opts.cmpKind
	*ps.CompressionBlockSize = w.opts.chunkSize
	*ps.Magic = "ORC"
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
			var header []byte
			orig := buf.Len() >= srcLen
			if orig {
				// todo:
				return 0, errors.New("no impl")
			} else {
				header = encChunkHeader(buf.Len(), orig)
			}
			if _, err = dst.Write(header); err != nil {
				return 0, err
			}
			if orig {
				// todo:
				return 0, errors.New("no impl")
			} else {
				if n, err := buf.WriteTo(dst); err != nil {
					return n, err
				}
			}

		} else {
			// todo:
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

// compress byte slice into chunks, used in stripe footer, tail footer
// thinking should be smaller than chunksize
func compressByteSlice(kind pb.CompressionKind, chunkSize uint64, b []byte) (compressed []byte, err error) {
	switch kind {
	case pb.CompressionKind_ZLIB:
		src := bytes.NewBuffer(b)
		dst := bytes.NewBuffer(make([]byte, len(b)))
		if _, err = compressTo(kind, chunkSize, src, dst); err != nil {
			return nil, err
		}
		return dst.Bytes(), nil

	default:
		return nil, errors.New("compression other than zlib not impl")
	}
	return
}

func compressProtoBuf(kind pb.CompressionKind, src *proto.Buffer, dst *bytes.Buffer) (uint64, error) {
	return 0, nil
}

type PhysicalWriter interface {
	WriteHeader() error
	WriteIndex(index pb.RowIndex) error
	WriteBloomFilter(bloom pb.BloomFilterIndex) error
	FinalizeStripe(footer pb.StripeFooter, dirEntry pb.StripeInformation) error
	WriteFileMetadata(metadate pb.Metadata) error
	WriteFileFooter(footer pb.Footer) error
	WritePostScript(ps pb.PostScript) error
	Close() error
	Flush() error
}

type physicalFsWriter struct {
	path string
	opts *WriterOptions
	f    *os.File
}

func (w *physicalFsWriter) WriteHeader() (err error) {
	if _, err = w.f.Write([]byte(MAGIC)); err != nil {
		return errors.Wrapf(err, "write header error")
	}
	return err
}

func (*physicalFsWriter) WriteIndex(index pb.RowIndex) error {
	panic("implement me")
}

func (*physicalFsWriter) WriteBloomFilter(bloom pb.BloomFilterIndex) error {
	panic("implement me")
}

func (*physicalFsWriter) FinalizeStripe(footer pb.StripeFooter, dirEntry pb.StripeInformation) error {
	panic("implement me")
}

func (*physicalFsWriter) WriteFileMetadata(metadate pb.Metadata) error {
	panic("implement me")
}

func (*physicalFsWriter) WriteFileFooter(footer pb.Footer) error {
	panic("implement me")
}

func (*physicalFsWriter) WritePostScript(ps pb.PostScript) error {
	panic("implement me")
}

func (*physicalFsWriter) Close() error {
	panic("implement me")
}

func (*physicalFsWriter) Flush() error {
	panic("implement me")
}
