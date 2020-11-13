package orc

import (
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
	"os"

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

func newWriter(schema *api.TypeDescription, opts *config.WriterOptions, out io.WriteCloser) (*writer, error) {
	// normalize schema id from 0
	schemas, err := schema.Normalize()
	if err!=nil {
		return nil, err
	}

	w := &writer{opts: opts, out: out, schemas: schemas}

	var h uint64
	if h, err = w.writeHeader(); err != nil {
		return nil, err
	}

	w.offset = h
	/*if w.stripe, err = newStripeWriter(w.offset, schemas, w.opts); err != nil {
		return nil, err
	}*/

	for _, cw := range w.stripe.columnWriters {
		w.columnStats = append(w.columnStats, cw.GetStats())
	}

	return w, nil
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

func (w *writer) Write(batch *api.ColumnVector) error {
	if err := w.stripe.write(batch); err != nil {
		return err
	}
	// rolled
	if w.stripe.previousInfo != nil {
		w.stripeInfos = append(w.stripeInfos, w.stripe.previousInfo)
		w.stripe.previousInfo = nil
	}
	return nil
}

func (w *writer) GetSchema() *api.TypeDescription {
	return w.schemas[0]
}

func (w *writer) close() error {
	if w.stripe.size() != 0 {
		if err := w.stripe.flushOut(); err != nil {
			return err
		}
		w.stripeInfos = append(w.stripeInfos, w.stripe.info)
	}

	/*if err := w.writeFileTail(); err != nil {
		return err
	}*/

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

/*func (w *writer) writeFileTail() error {
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
*/