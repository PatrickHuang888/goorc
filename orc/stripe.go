package orc

import (
	"bytes"
	"github.com/golang/protobuf/proto"
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/column"
	"github.com/patrickhuang888/goorc/orc/common"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type stripeWriter struct {
	schemas []*api.TypeDescription

	columnWriters []column.Writer

	// rows update after every write
	// index length and data length update after flush
	info         *pb.StripeInformation
	previousInfo *pb.StripeInformation

	opts *config.WriterOptions

	out orcio.File
}

func (stripe *stripeWriter) write(batch *api.ColumnVector) error {
	col := stripe.columnWriters[batch.Id]

	count := uint64(0)
	for i, v := range batch.Vector {
		if err := col.Write(v); err != nil {
			return err
		}
		for _, child := range batch.Children {
			childCol := stripe.columnWriters[child.Id]
			if err := childCol.Write(child.Vector[i]); err != nil {
				return err
			}
		}

		if stripe.size() >= stripe.opts.StripeSize {
			*stripe.info.NumberOfRows += count
			count = 0
			// todo: run in another go routine
			if err := stripe.roll(); err != nil {
				return err
			}
		}
		count++
	}
	*stripe.info.NumberOfRows += count
	return nil
}

func (stripe *stripeWriter) roll() error {
	if err := stripe.flushOut(); err != nil {
		return err
	}
	stripe.forward()
	return nil
}

func (stripe stripeWriter) size() int {
	var n int
	for _, cw := range stripe.columnWriters {
		n += cw.Size()
	}
	return n
}

func (stripe *stripeWriter) flushOut() error {
	for _, column := range stripe.columnWriters {
		if err := column.Flush(); err != nil {
			return err
		}
	}

	if stripe.opts.WriteIndex {
		for _, column := range stripe.columnWriters {
			index := column.GetIndex()
			var indexData []byte
			indexData, err := proto.Marshal(index)
			if err != nil {
				return err
			}
			nd, err := stripe.out.Write(indexData)
			if err != nil {
				return errors.WithStack(err)
			}
			*stripe.info.IndexLength += uint64(nd)
		}
		log.Tracef("flush index of length %d", stripe.info.GetIndexLength())
	}

	for _, column := range stripe.columnWriters {
		ns, err := column.WriteOut(stripe.out)
		if err != nil {
			return err
		}
		*stripe.info.DataLength += uint64(ns)
	}

	if err := stripe.writeFooter(); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (stripe *stripeWriter) writeFooter() error {
	footer := &pb.StripeFooter{}

	for _, column := range stripe.columnWriters {
		footer.Streams = append(footer.Streams, column.GetStreamInfos()...)
	}

	for _, schema := range stripe.schemas {
		footer.Columns = append(footer.Columns, &pb.ColumnEncoding{Kind: &schema.Encoding})
	}

	footerBuf, err := proto.Marshal(footer)
	if err != nil {
		return errors.WithStack(err)
	}

	compressedFooterBuf := bytes.NewBuffer(make([]byte, stripe.opts.ChunkSize))
	compressedFooterBuf.Reset()

	if err = common.CompressingChunks(stripe.opts.CompressionKind, stripe.opts.ChunkSize, compressedFooterBuf, bytes.NewBuffer(footerBuf)); err != nil {
		return err
	}

	n, err := compressedFooterBuf.WriteTo(stripe.out)
	if err != nil {
		return errors.WithStack(err)
	}
	*stripe.info.FooterLength = uint64(n)

	log.Infof("write out stripe footer %s", footer.String())
	return nil
}

func (stripe *stripeWriter) forward() {
	stripe.previousInfo = stripe.info

	offset := stripe.info.GetOffset() + stripe.info.GetIndexLength() + stripe.info.GetDataLength() + stripe.info.GetFooterLength()
	stripe.info = &pb.StripeInformation{Offset: &offset, IndexLength: new(uint64), DataLength: new(uint64), FooterLength: new(uint64), NumberOfRows: new(uint64)}
	for _, column := range stripe.columnWriters {
		column.Reset()
	}
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

type stripeReader struct {
	f orcio.File

	schemas []*api.TypeDescription
	opts    *config.ReaderOptions

	columnReaders []column.Reader

	index int

	numberOfRows uint64
	cursor       uint64
}

func newStripeReader(f orcio.File, schemas []*api.TypeDescription, opts *config.ReaderOptions, idx int, info *pb.StripeInformation, footer *pb.StripeFooter) (reader *stripeReader, err error) {
	reader = &stripeReader{f: f, schemas: schemas, opts: opts, index: idx}
	err = reader.init(info, footer)
	return
}

// stripe {index{},column{[present],data,[length]},footer}
func (stripe *stripeReader) init(info *pb.StripeInformation, footer *pb.StripeFooter) error {
	var err error

	stripe.columnReaders = make([]column.Reader, len(stripe.schemas))
	for i, schema := range stripe.schemas {
		schema.Encoding = footer.GetColumns()[schema.Id].GetKind()
		if stripe.columnReaders[i], err = column.NewReader(schema, stripe.opts, stripe.f); err != nil {
			return err
		}
	}

	// build tree
	/*for _, schema := range s.schemas {
		if schema.Kind == pb.Type_STRUCT {
			var crs []column.Reader
			for _, childSchema := range schema.Children {
				crs = append(crs, s.columnReaders[childSchema.Id])
			}
			s.columnReaders[schema.Id].InitChildren(crs)
		}
	}*/

	// streams has sequence
	indexStart := info.GetOffset()
	dataStart := indexStart + info.GetIndexLength()
	for _, streamInfo := range footer.GetStreams() {
		id := streamInfo.GetColumn()
		streamKind := streamInfo.GetKind()
		length := streamInfo.GetLength()

		if streamKind == pb.Stream_ROW_INDEX {
			if err := stripe.columnReaders[id].InitIndex(indexStart, length); err != nil {
				return err
			}
			indexStart += length
			continue
		}

		if err := stripe.columnReaders[id].InitStream(streamInfo, dataStart); err != nil {
			return err
		}
		dataStart += length
	}

	return nil
}

// a stripe is typically  ~200MB
func (stripe *stripeReader) Next(batch *api.ColumnVector) error {
	if stripe.cursor+1>=stripe.numberOfRows {
		batch.Vector= batch.Vector[:0]
		for _, v := range batch.Children {
			v.Vector = v.Vector[:0]
		}
		return nil
	}

	page:= len(batch.Vector)
	if int(stripe.numberOfRows-stripe.cursor) < len(batch.Vector) {
		page = int(stripe.numberOfRows - stripe.cursor)
		batch.Vector = batch.Vector[:page]
	}

	if err := stripe.columnReaders[batch.Id].Next(batch.Vector); err != nil {
		return err
	}
	for _, v := range batch.Children {
		v.Vector = v.Vector[:page]
		if err := stripe.columnReaders[v.Id].Next(v.Vector); err != nil {
			return err
		}
	}

	stripe.cursor += uint64(len(batch.Vector))

	log.Debugf("stripe %d column %d has read %d", stripe.index, batch.Id, len(batch.Vector))
	return nil
}

// locate rows in this stripe
func (s *stripeReader) Seek(rowNumber uint64) error {
	// rethink: all column seek to row ?
	for _, r := range s.columnReaders {
		if err := r.Seek(rowNumber); err != nil {
			return err
		}
	}
	return nil
}

func (s *stripeReader) Close() error {
	for _, r := range s.columnReaders {
		r.Close()
	}
	return nil
}
