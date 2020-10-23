package orc

import (
	"bytes"
	"github.com/golang/protobuf/proto"
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/common"
	"github.com/patrickhuang888/goorc/orc/config"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
	"os"

	"github.com/patrickhuang888/goorc/orc/column"
	"github.com/patrickhuang888/goorc/pb/pb"
)

var VERSION = []uint32{0, 12}

type Writer interface {
	GetSchema() *api.TypeDescription

	Write(batch *api.ColumnVector) error

	Close() error
}

type fileWriter struct {
	path string
	f    *os.File

	*writer
}

// current version always write new file, no append
func NewFileWriter(path string, schema *api.TypeDescription, opts *config.WriterOptions) (writer Writer, err error) {
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

func newWriter(schema *api.TypeDescription, opts *config.WriterOptions, out io.WriteCloser) (w *writer, err error) {
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
	schemas []*api.TypeDescription
	opts    *config.WriterOptions

	offset uint64

	stripe *stripeWriter

	stripeInfos []*pb.StripeInformation
	columnStats []*pb.ColumnStatistics

	ps *pb.PostScript

	out io.WriteCloser
}

// because stripe data in file is stream sequence, so every data should write to memory first
// before write to file
func (w *writer) Write(batch *api.ColumnVector) error {
	var err error

	if err = w.stripe.write(batch); err != nil {
		return err
	}

	// fixme: calculate size after write out batch
	// todo: running in another goroutine
	if w.stripe.size() >= w.opts.StripeSize {
		if err = w.stripe.flushOut(w.out); err != nil {
			return err
		}

		w.stripeInfos = append(w.stripeInfos, w.stripe.info)
		w.stripe.forward()
	}

	return nil
}

func (w *writer) GetSchema() *api.TypeDescription {
	return w.schemas[0]
}

/*func (w *writer) flushStripe() error {
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
}*/

func (w *writer) close() error {
	var err error

	if w.stripe.size() != 0 {
		if err = w.stripe.flushOut(w.out); err != nil {
			return err
		}
		w.stripeInfos = append(w.stripeInfos, w.stripe.info)
	}

	if err := w.writeFileTail(); err != nil {
		return err
	}

	if err := w.out.Close(); err != nil {
		return errors.WithStack(err)
	}
	return nil
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
	ft.Types = api.schemasToTypes(w.schemas)
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

func newStripeWriter(offset uint64, schemas []*api.TypeDescription, opts *config.WriterOptions) (stripe *stripeWriter, err error) {
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
	stripe = &stripeWriter{columnWriters: writers, info: info, schemas: schemas, opts: opts}
	return
}

type stripeWriter struct {
	schemas []*api.TypeDescription

	columnWriters []column.Writer

	info *pb.StripeInformation // data in info not update every write,

	opts *config.WriterOptions
}

func (stripe *stripeWriter) write(batch *api.ColumnVector) error {
	var err error
	var count uint64

	writer := stripe.columnWriters[batch.Id]

	for i, v := range batch.Vector {
		// todo: verify null values on schema and batch
		if err = writer.Write(v); err != nil {
			return err
		}
		for _, child := range batch.Children {
			childWriter := stripe.columnWriters[child.Id]
			if stripe.schemas[batch.Id].HasNulls && !v.Null {
				// child Null ignored ？
				// and child schema has null will be false
				// child null value will equal to parent null
				if err = childWriter.WriteV(child.Vector[i].V); err != nil {
					return err
				}
			}
		}

		count++

		/*if stripe.size() >= stripe.opts.StripeSize {
			*stripe.info.NumberOfRows += count
			count = 0

			// todo: run in another go routine
			if err = stripe.flushOut(); err != nil {
				return err
			}

			stripe.forward()
		}*/
	}

	*stripe.info.NumberOfRows += count
	return nil
}

func (stripe stripeWriter) size() int {
	var n int
	for _, cw := range stripe.columnWriters {
		n += cw.Size()
	}
	return n
}

func (stripe *stripeWriter) flushOut(out io.Writer) error {
	var err error

	for _, column := range stripe.columnWriters {
		if err = column.Flush(); err != nil {
			return err
		}
	}

	if stripe.opts.WriteIndex {
		for _, column := range stripe.columnWriters {
			index := column.GetIndex()
			var indexData []byte
			if indexData, err = proto.Marshal(index); err != nil {
				return err
			}

			var nd int
			if nd, err = out.Write(indexData); err != nil {
				return errors.WithStack(err)
			}

			*stripe.info.IndexLength += uint64(nd)
		}

		log.Tracef("flush index of length %d", stripe.info.GetIndexLength())
	}

	for _, column := range stripe.columnWriters {
		var ns int64
		if ns, err = column.WriteOut(out); err != nil {
			return err
		}
		*stripe.info.DataLength += uint64(ns)
	}

	if err = stripe.writeFooter(out); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (stripe *stripeWriter) writeFooter(out io.Writer) error {
	var err error
	footer := &pb.StripeFooter{}

	for _, column := range stripe.columnWriters {
		footer.Streams = append(footer.Streams, column.GetStreamInfos()...)
	}

	for _, schema := range stripe.schemas {
		footer.Columns = append(footer.Columns, &pb.ColumnEncoding{Kind: &schema.Encoding})
	}

	var footerBuf []byte
	footerBuf, err = proto.Marshal(footer)
	if err != nil {
		return errors.WithStack(err)
	}

	compressedFooterBuf := bytes.NewBuffer(make([]byte, stripe.opts.ChunkSize))
	compressedFooterBuf.Reset()

	if err = common.CompressingChunks(stripe.opts.CompressionKind, stripe.opts.ChunkSize, compressedFooterBuf, bytes.NewBuffer(footerBuf)); err != nil {
		return err
	}

	var n int64
	if n, err = compressedFooterBuf.WriteTo(out); err != nil {
		return errors.WithStack(err)
	}

	*stripe.info.FooterLength = uint64(n)

	log.Infof("write out stripe footer %s", footer.String())
	return nil
}

func (stripe *stripeWriter) forward() {
	offset := stripe.info.GetOffset() + stripe.info.GetIndexLength() + stripe.info.GetDataLength() + stripe.info.GetFooterLength()
	stripe.info = &pb.StripeInformation{Offset: &offset, IndexLength: new(uint64), DataLength: new(uint64), FooterLength: new(uint64), NumberOfRows: new(uint64)}

	for _, column := range stripe.columnWriters {
		column.Reset()
	}
}
