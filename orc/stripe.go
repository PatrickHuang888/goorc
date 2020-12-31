package orc

import (
	"bytes"
	"io"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/column"
	"github.com/patrickhuang888/goorc/orc/common"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/pb/pb"
)

type stripeWriter struct {
	schemas []*api.TypeDescription

	columnWriters []column.Writer

	// rows update after every write
	// index length and data length update after flush
	info  *pb.StripeInformation
	infos []*pb.StripeInformation

	opts *config.WriterOptions

	out io.Writer
}

func (stripe *stripeWriter) write(batch *api.ColumnVector) error {
	col := stripe.columnWriters[batch.Id]

	count := uint64(0)
	for i, v := range batch.Vector {
		count++

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
			if err := stripe.flushOut(); err != nil {
				return err
			}
			stripe.forward()
		}
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

	if err = common.CompressingAllInChunks(stripe.opts.CompressionKind, stripe.opts.ChunkSize, compressedFooterBuf, bytes.NewBuffer(footerBuf)); err != nil {
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
	stripe.infos = append(stripe.infos, stripe.info)

	offset := stripe.info.GetOffset() + stripe.info.GetIndexLength() + stripe.info.GetDataLength() + stripe.info.GetFooterLength()
	stripe.info = &pb.StripeInformation{Offset: &offset, IndexLength: new(uint64), DataLength: new(uint64), FooterLength: new(uint64), NumberOfRows: new(uint64)}
	for _, column := range stripe.columnWriters {
		column.Reset()
	}
}

func newStripeWriter(out io.Writer, offset uint64, schemas []*api.TypeDescription, opts *config.WriterOptions) (stripe *stripeWriter, err error) {
	// prepare streams
	var writers []column.Writer
	for _, schema := range schemas {
		var w column.Writer
		if w, err = column.NewWriter(schema, opts); err != nil {
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
	stripe = &stripeWriter{out: out, columnWriters: writers, info: info, schemas: schemas, opts: opts}
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

func newStripeReader(f orcio.File, schemas []*api.TypeDescription, opts *config.ReaderOptions, idx int, info *pb.StripeInformation) (*stripeReader, error) {
	reader := &stripeReader{f: f, schemas: schemas, opts: opts, index: idx, numberOfRows: info.GetNumberOfRows()}
	footer, err := reader.readFooter(info)
	if err != nil {
		return nil, err
	}
	if err := reader.init(info, footer); err != nil {
		return nil, err
	}
	return reader, nil
}

func (stripe *stripeReader) readFooter(info *pb.StripeInformation) (*pb.StripeFooter, error) {
	if _, err := stripe.f.Seek(int64(info.GetOffset()+info.GetIndexLength()+info.GetDataLength()), io.SeekStart); err != nil {
		return nil, err
	}

	footerBuf := make([]byte, info.GetFooterLength())
	if _, err := io.ReadFull(stripe.f, footerBuf); err != nil {
		return nil, errors.WithStack(err)
	}
	if stripe.opts.CompressionKind != pb.CompressionKind_NONE {
		fb := &bytes.Buffer{}
		if err := common.DecompressChunks(stripe.opts.CompressionKind, fb, bytes.NewBuffer(footerBuf)); err != nil {
			return nil, err
		}
		footerBuf = fb.Bytes()
	}
	footer := &pb.StripeFooter{}
	if err := proto.Unmarshal(footerBuf, footer); err != nil {
		return nil, err
	}
	log.Debugf("extracted stripe footer %d: %s", stripe.index, footer.String())
	return footer, nil
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
func (stripe *stripeReader) next(batch *api.ColumnVector) (end bool, err error) {
	i := 0

	page:= cap(batch.Vector)- len(batch.Vector)
	if int(stripe.numberOfRows- stripe.cursor) < page {
		page= int(stripe.numberOfRows- stripe.cursor)
	}

	for ; stripe.cursor+uint64(i) < stripe.numberOfRows && len(batch.Vector) < cap(batch.Vector); i++ {
		var v api.Value
		if v, err = stripe.columnReaders[batch.Id].Next(); err != nil {
			return
		}
		batch.Vector = append(batch.Vector, v)

		if err = stripe.readChildren(batch); err != nil {
			return
		}
	}
	stripe.cursor += uint64(i)
	logger.Debugf("stripe %d column %d has cursor %d", stripe.index, batch.Id, stripe.cursor)
	end = stripe.cursor+1 >= stripe.numberOfRows
	return
}

func (stripe *stripeReader) readChildren(batch *api.ColumnVector) error {
	for _, c := range batch.Children {
		for _, v := range batch.Vector {
			cv := api.Value{}
			if stripe.schemas[batch.Id].HasNulls {
				cv.Null = v.Null
			}
			if !cv.Null {
				v, err := stripe.columnReaders[c.Id].Next()
				if err != nil {
					return err
				}
				cv.V = v
			}
			c.Vector = append(c.Vector, cv)

			if err := stripe.readChildren(&c); err != nil {
				return err
			}
		}
	}
	return nil
}

// locate rows in this stripe
func (stripe *stripeReader) Seek(rowNumber uint64) error {
	for _, r := range stripe.columnReaders {
		if err := r.Seek(rowNumber); err != nil {
			return err
		}
	}
	return nil
}

func (stripe *stripeReader) Close() error {
	for _, r := range stripe.columnReaders {
		r.Close()
	}
	return nil
}
