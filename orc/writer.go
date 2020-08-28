package orc

import (
	"bytes"
	"github.com/golang/protobuf/proto"
	"github.com/patrickhuang888/goorc/orc/common"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
	"os"

	"github.com/patrickhuang888/goorc/orc/column"
	"github.com/patrickhuang888/goorc/pb/pb"
)


var VERSION = []uint32{0, 12}



type Writer interface {
	GetSchema() *TypeDescription

	Write(batch *ColumnVector) error

	Close() error
}

type fileWriter struct {
	path string
	f    *os.File

	*writer
}

// current version always write new file, no append
func NewFileWriter(path string, schema *TypeDescription, opts *WriterOptions) (writer Writer, err error) {
	// todo: overwrite exist file warning
	log.Infof("open %stream", path)
	f, err := os.Create(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	w, err := newWriter(schema, opts, f)
	if err != nil {
		return nil, err
	}

	writer = &fileWriter{f: f, path: path, writer: w}
	return writer, nil
}

func (w *fileWriter) Close() error {
	return w.close()
}

func newWriter(schema *TypeDescription, opts *WriterOptions, out io.WriteCloser) (w *writer, err error) {
	// normalize schema id from 0
	schemas := schema.normalize()

	w = &writer{opts: opts, out: out, schemas: schemas}

	var h uint64
	if h, err = w.writeHeader(); err != nil {
		return
	}

	w.offset = h
	if w.stripe, err = newStripeWriter(w.offset, schemas, w.opts); err != nil {
		return
	}

	for _, cw := range w.stripe.columnWriters {
		w.columnStats = append(w.columnStats, cw.GetStats())
	}

	return
}

// cannot used concurrently, not synchronized
// strip buffered in memory until the strip size
// Encode out by columns
type writer struct {
	schemas []*TypeDescription
	opts    *WriterOptions

	offset uint64

	stripe *stripeWriter

	stripeInfos []*pb.StripeInformation
	columnStats []*pb.ColumnStatistics

	ps *pb.PostScript

	out io.WriteCloser
}

// because stripe data in file is stream sequence, so every data should write to memory first
// before write to file
func (w *writer) Write(batch *ColumnVector) error {
	var err error

	if err = w.stripe.writeColumn(batch); err != nil {
		return err
	}

	if w.stripe.size() >= int(w.opts.StripeSize) {
		if err = w.flushStripe(); err != nil {
			return err
		}
	}

	return nil
}

func (w *writer) GetSchema() *TypeDescription {
	return w.schemas[0]
}

func (w *writer) flushStripe() error {
	var n int64
	var nf int
	var err error

	if n, err = w.stripe.flushOut(w.out); err != nil {
		return err
	}

	if nf, err = w.stripe.writeFooter(w.out); err != nil {
		return err
	}

	//next
	w.offset += uint64(n) + uint64(nf)
	w.stripeInfos = append(w.stripeInfos, w.stripe.info)
	w.stripe.reset(w.offset)

	return nil
}

func (w *writer) close() error {
	if w.stripe.size() != 0 {
		if err := w.flushStripe(); err != nil {
			return err
		}
	}

	if err := w.writeFileTail(); err != nil {
		return err
	}

	return w.out.Close()
}

func (w *writer) writeHeader() (uint64, error) {
	b := []byte(MAGIC)
	if _, err := w.out.Write(b); err != nil {
		return 0, errors.WithStack(err)
	}
	return uint64(len(b)), nil
}

var HEADER_LENGTH = uint64(3)
var MAGIC = "ORC"

func (w *writer) writeFileTail() error {
	var err error
	// Encode footer
	// todo: rowsinstride
	ft := &pb.Footer{HeaderLength: &HEADER_LENGTH, ContentLength: new(uint64), NumberOfRows: new(uint64)}

	for _, si := range w.stripeInfos {
		*ft.ContentLength += si.GetIndexLength() + si.GetDataLength() + si.GetFooterLength()
		*ft.NumberOfRows += si.GetNumberOfRows()
	}
	ft.Stripes = w.stripeInfos
	ft.Types = schemasToTypes(w.schemas)
	ft.Statistics = w.columnStats
	// metadata

	var ftb []byte
	if ftb, err = proto.Marshal(ft); err != nil {
		return errors.WithStack(err)
	}

	var ftCmpBuf []byte
	if ftCmpBuf, err = compressByteSlice(w.opts.CompressionKind, w.opts.ChunkSize, ftb); err != nil {
		return err
	}
	ftl := uint64(len(ftCmpBuf))

	if _, err := w.out.Write(ftCmpBuf); err != nil {
		return errors.WithStack(err)
	}
	log.Infof("write out file tail %s (length: %d)", ft.String(), ftl)

	// postscript
	ps := &pb.PostScript{}
	ps.FooterLength = &ftl
	ps.Compression = &w.opts.CompressionKind
	c := uint64(w.opts.ChunkSize)
	ps.CompressionBlockSize = &c
	ps.Version = VERSION
	ps.Magic = &MAGIC

	var psb []byte
	if psb, err = proto.Marshal(ps); err != nil {
		return errors.WithStack(err)
	}

	var n int
	if n, err = w.out.Write(psb); err != nil {
		return errors.WithStack(err)
	}

	log.Infof("write out postscript (length %d)", n)

	// last byte is ps length
	if _, err = w.out.Write([]byte{byte(n)}); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func newStripeWriter(offset uint64, schemas []*TypeDescription, opts *WriterOptions) (stripe *stripeWriter, err error) {
	// prepare streams
	var writers []column.Writer
	for _, schema := range schemas {
		var w column.Writer
		if w, err = column.CreateWriter(schema, opts); err != nil {
			return
		}
		writers = append(writers, w)
	}

	/*for _, schema := range schemas {
		switch schema.Kind {
		case pb.Type_STRUCT:
			for _, child := range schema.Children {
				w := writers[schema.Id].(*structWriter)
				w.children = append(w.children, writers[child.Id])
			}
		}
		// todo: case
	}*/

	o := offset
	info := &pb.StripeInformation{Offset: &o, IndexLength: new(uint64), DataLength: new(uint64), FooterLength: new(uint64),
		NumberOfRows: new(uint64)}
	stripe = &stripeWriter{columnWriters: writers, info: info, schemas: schemas, chunkSize: opts.ChunkSize, compressionKind: opts.CompressionKind}
	return
}

type stripeWriter struct {
	schemas []*TypeDescription

	columnWriters []column.Writer

	info *pb.StripeInformation // data in info not update every write,

	writeIndex bool

	compressionKind pb.CompressionKind
	chunkSize       int
}

// write to memory
func (stripe *stripeWriter) writeColumn(batch *ColumnVector) error {
	writer := stripe.columnWriters[batch.Id]
	rows, err := writer.Write(batch.Presents, false, batch.Vector)
	if err != nil {
		return err
	}

	*stripe.info.NumberOfRows += uint64(rows)
	return nil
}

func (stripe stripeWriter) size() int {
	var n int
	for _, c := range stripe.columnWriters {
		n += c.Size()
	}
	return n
}

func (stripe *stripeWriter) flushOut(out io.Writer) (n int64, err error) {
	for _, column := range stripe.columnWriters {
		if err = column.Flush(); err != nil {
			return
		}
	}

	if stripe.writeIndex {
		for _, column := range stripe.columnWriters {
			index := column.GetIndex()
			var indexData []byte
			if indexData, err = proto.Marshal(index); err != nil {
				return
			}

			var nd int
			if nd, err = out.Write(indexData); err != nil {
				return n, errors.WithStack(err)
			}

			*stripe.info.IndexLength += uint64(nd)
			n += int64(nd)
		}

		log.Tracef("flush index of length %d", stripe.info.GetIndexLength())
	}

	for _, column := range stripe.columnWriters {
		var ns int64
		if ns, err = column.WriteOut(out); err != nil {
			return
		}

		*stripe.info.DataLength += uint64(ns)
		n += ns
	}

	return
}

func (stripe *stripeWriter) writeFooter(out io.Writer) (n int, err error) {
	footer := &pb.StripeFooter{}

	for _, column := range stripe.columnWriters {
		// reassure: need make sure stream length != 0
		footer.Streams = append(footer.Streams, column.GetStreamInfos()...)
	}

	for _, schema := range stripe.schemas {
		footer.Columns = append(footer.Columns, &pb.ColumnEncoding{Kind: &schema.Encoding})
	}

	var footerBuf []byte
	footerBuf, err = proto.Marshal(footer)
	if err != nil {
		return n, errors.WithStack(err)
	}

	var compressedFooterBuf []byte
	if compressedFooterBuf, err = compressByteSlice(stripe.compressionKind, stripe.chunkSize, footerBuf); err != nil {
		return
	}

	if n, err = out.Write(compressedFooterBuf); err != nil {
		return n, errors.WithStack(err)
	}

	*stripe.info.FooterLength = uint64(n)

	log.Infof("write out stripe footer %s", footer.String())
	return
}

func (stripe *stripeWriter) reset(offset uint64) {
	o := offset
	stripe.info = &pb.StripeInformation{Offset: &o, IndexLength: new(uint64), DataLength: new(uint64), FooterLength: new(uint64), NumberOfRows: new(uint64)}

	for _, column := range stripe.columnWriters {
		column.Reset()
	}
}

// used in currentStripe footer, tail footer
func compressByteSlice(kind pb.CompressionKind, chunkSize int, b []byte) (compressed []byte, err error) {
	switch kind {
	case pb.CompressionKind_NONE:
		compressed = b
	case pb.CompressionKind_ZLIB:
		src := bytes.NewBuffer(b)
		dst := bytes.NewBuffer(make([]byte, len(b)))
		dst.Reset()
		if err = common.ZlibCompressing(chunkSize, dst, src); err != nil {
			return nil, err
		}
		return dst.Bytes(), nil

	default:
		return nil, errors.New("compression not impl")
	}
	return
}


