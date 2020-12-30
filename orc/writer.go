package orc

import (
	"bytes"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/common"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/pb/pb"
)

var VERSION = []uint32{0, 12}

var logger = log.New()

type Writer interface {
	GetSchema() *api.TypeDescription

	Write(batch *api.ColumnVector) error

	Close() error
}

// current version always write new file, no append
func NewOSFileWriter(path string, schema *api.TypeDescription, opts config.WriterOptions) (Writer, error) {
	log.Infof("open file %s", path)
	f, err := orcio.OpenFileForWrite(path)
	if err != nil {
		return nil, err
	}
	w, err := newWriter(schema, &opts, f)
	if err != nil {
		return nil, err
	}
	return w, nil
}

func newWriter(schema *api.TypeDescription, opts *config.WriterOptions, f orcio.File) (*writer, error) {
	schemas, err := schema.Flat()
	if err != nil {
		return nil, err
	}

	w := &writer{opts: opts, f: f, schemas: schemas}

	var h uint64
	if h, err = w.writeHeader(); err != nil {
		return nil, err
	}
	w.offset = h
	if w.stripe, err = newStripeWriter(w.f, w.offset, schemas, w.opts); err != nil {
		return nil, err
	}
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

	columnStats []*pb.ColumnStatistics

	ps *pb.PostScript

	f orcio.File
}

func (w *writer) Write(batch *api.ColumnVector) error {
	if err := w.CheckBatch(batch); err != nil {
		return err
	}

	if err := w.stripe.write(batch); err != nil {
		return err
	}
	return nil
}

func (w *writer) CheckBatch(batch *api.ColumnVector) error {
	for _, v := range batch.Vector {
		if v.Null && !w.schemas[batch.Id].HasNulls {
			return errors.Errorf("schema %d %s has no nulls, but batch has null", batch.Id, w.schemas[batch.Id].Kind.String())
		}
	}
	for _, c := range batch.Children {
		if err := w.CheckBatch(&c); err != nil {
			return err
		}
	}
	return nil
}

func (w *writer) GetSchema() *api.TypeDescription {
	return w.schemas[0]
}

func (w *writer) Close() error {
	if w.stripe.size() != 0 {
		if err := w.stripe.flushOut(); err != nil {
			return err
		}
		// add current stripe info
		w.stripe.infos = append(w.stripe.infos, w.stripe.info)
	}

	if err := w.writeFileTail(); err != nil {
		return err
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

var HEADER_LENGTH = uint64(3)
var MAGIC = "ORC"

func (w *writer) writeFileTail() error {
	// Encode footer
	// todo: rowsinstride
	ft := &pb.Footer{HeaderLength: &HEADER_LENGTH, ContentLength: new(uint64), NumberOfRows: new(uint64)}

	for _, si := range w.stripe.infos {
		*ft.ContentLength += si.GetIndexLength() + si.GetDataLength() + si.GetFooterLength()
		*ft.NumberOfRows += si.GetNumberOfRows()
	}
	ft.Stripes = w.stripe.infos
	ft.Types = api.SchemasToTypes(w.schemas)
	ft.Statistics = w.columnStats

	// metadata

	footBytes, err := proto.Marshal(ft)
	if err != nil {
		return errors.WithStack(err)
	}

	var footLen uint64
	if w.opts.CompressionKind == pb.CompressionKind_NONE {
		footLen = uint64(len(footBytes))
		if _, err := w.f.Write(footBytes); err != nil {
			return err
		}
	} else {
		footCmpBuf := &bytes.Buffer{}
		if err = common.CompressingAllInChunks(w.opts.CompressionKind, w.opts.ChunkSize, footCmpBuf, bytes.NewBuffer(footBytes)); err != nil {
			return err
		}
		footLen = uint64(footCmpBuf.Len())
		if _, err := footCmpBuf.WriteTo(w.f); err != nil {
			return err
		}
	}
	logger.Infof("write out compressed file tail %s (length: %d)", ft.String(), footLen)

	// postscript
	ps := &pb.PostScript{}
	ps.FooterLength = &footLen
	ps.Compression = &w.opts.CompressionKind
	c := uint64(w.opts.ChunkSize)
	ps.CompressionBlockSize = &c
	ps.Version = VERSION
	ps.Magic = &MAGIC
	psb, err := proto.Marshal(ps)
	if err != nil {
		return errors.WithStack(err)
	}
	n, err := w.f.Write(psb)
	if err != nil {
		return err
	}
	logger.Debugf("write out postscript (length %d)", n)
	// last byte is ps length
	if _, err = w.f.Write([]byte{byte(n)}); err != nil {
		return err
	}
	return nil
}
