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

	count uint64
	index int
}

func (stripe *stripeWriter) write(vec *api.ColumnVector) error {
	count := uint64(0)
	for i := 0; i < vec.Len(); i++ {
		if err := stripe.writeValue(i, vec); err != nil {
			return err
		}
		count++

		if stripe.size() >= stripe.opts.StripeSize {
			*stripe.info.NumberOfRows += count
			count = 0

			// improve:  concurrent flush?
			if err := stripe.flushOut(); err != nil {
				return err
			}
			stripe.forward()
		}
	}

	*stripe.info.NumberOfRows += count
	return nil
}

func (stripe *stripeWriter) writeValue(index int, vec *api.ColumnVector) error {
	if vec.Vector != nil { // maybe no present struct vector
		if err := stripe.columnWriters[vec.Id].Write(vec.Vector[index]); err != nil {
			return err
		}
	}

	for _, c := range vec.Children {
		if err := stripe.writeValue(index, c); err != nil {
			return err
		}
	}
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
	for _, c := range stripe.columnWriters {
		if err := c.Flush(); err != nil {
			return err
		}
	}

	if stripe.opts.WriteIndex {
		for _, c := range stripe.columnWriters {
			index := c.GetIndex()
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

	for _, c := range stripe.columnWriters {
		ns, err := c.WriteOut(stripe.out)
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

	for _, c := range stripe.columnWriters {
		footer.Streams = append(footer.Streams, c.GetStreamInfos()...)
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
	for _, c := range stripe.columnWriters {
		c.Reset()
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
	f    orcio.File
	ropts *config.ReaderOptions
	bopt  *api.BatchOption

	schemas       []*api.TypeDescription
	columnReader column.Reader

	index int

	numberOfRows uint64
	cursor       uint64
}

func newStripeReader(f orcio.File, schemas []*api.TypeDescription, ropts *config.ReaderOptions, bopt *api.BatchOption,
	idx int, info *pb.StripeInformation) (*stripeReader, error) {
	reader := &stripeReader{f: f, schemas: schemas, ropts: ropts, bopt:bopt, index: idx, numberOfRows: info.GetNumberOfRows()}
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
	if stripe.ropts.CompressionKind != pb.CompressionKind_NONE {
		fb := &bytes.Buffer{}
		if err := common.DecompressChunks(stripe.ropts.CompressionKind, fb, bytes.NewBuffer(footerBuf)); err != nil {
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
	// make sure len(columns)==len(all schemas)
	readers := make([]column.Reader, len(stripe.schemas))
	for _, schema := range stripe.schemas {
		if stripe.bopt.IsInclude(schema.Id) {
			schema.Encoding = footer.GetColumns()[schema.Id].GetKind()
			r, err := column.NewReader(schema, stripe.ropts, stripe.f)
			if err != nil {
				return err
			}
			readers[schema.Id] = r
			if stripe.columnReader==nil {
				stripe.columnReader= r
			}
		}
	}

	// streams has sequence
	indexStart := info.GetOffset()
	dataStart := indexStart + info.GetIndexLength()
	for _, streamInfo := range footer.GetStreams() {
		id := streamInfo.GetColumn()
		streamKind := streamInfo.GetKind()
		length := streamInfo.GetLength()

		if streamKind == pb.Stream_ROW_INDEX {
			if readers[id] != nil {
				if err := readers[id].InitIndex(indexStart, length); err != nil {
					return err
				}
			}
			indexStart += length
			continue
		}

		if readers[id] != nil {
			if err := readers[id].InitStream(streamInfo, dataStart); err != nil {
				return err
			}
		}
		dataStart += length
	}

	// build tree
	for _, schema := range stripe.schemas {
		for i, c := range schema.Children {
			if stripe.bopt.IsInclude(c.Id) { // todo: verify includes map's key and value
				switch schema.Kind {
				case pb.Type_STRUCT:
					readers[schema.Id].(*column.StructReader).AddChild(readers[c.Id])
				case pb.Type_LIST:
					readers[schema.Id].(*column.ListV2Reader).SetChild(readers[c.Id])
				case pb.Type_MAP:
					if i == 0 {
						readers[schema.Id].(*column.MapV2Reader).SetKey(readers[c.Id])
					} else if i == 1 {
						readers[schema.Id].(*column.MapV2Reader).SetValue(readers[c.Id])
					} else {
						return errors.New("map children > 2")
					}
				case pb.Type_UNION:
					return errors.New("not impl")
				default:
					return errors.New("should have no child")
				}
			}
		}
	}

	return nil
}

// a stripe is typically  ~200MB
func (stripe *stripeReader) next(vec *api.ColumnVector) (end bool, err error) {
	stripe.prepareVector(vec)

	if err = stripe.columnReader.NextBatch(vec); err != nil {
		return
	}

	stripe.cursor += uint64(vec.Len())
	logger.Debugf("stripe %d reading cursor %d", stripe.index, stripe.cursor)
	end = stripe.cursor+1 >= stripe.numberOfRows
	return
}

func (stripe *stripeReader) prepareVector(vec *api.ColumnVector) {
	page := vec.Cap() - vec.Len()
	if int(stripe.numberOfRows-stripe.cursor) < page {
		page = int(stripe.numberOfRows - stripe.cursor)
	}
	stripe.setVectorSize(page, vec)
}

func (stripe *stripeReader) setVectorSize(size int, vec *api.ColumnVector) {
	switch vec.Kind {
	case pb.Type_STRUCT:
		if stripe.schemas[vec.Id].HasNulls {
			vec.Vector = vec.Vector[:size]
		}
		for _, c := range vec.Children {
			stripe.setVectorSize(size, c)
		}

	case pb.Type_LIST:
		vec.Vector = vec.Vector[:size]
		// list's children maybe struct
		vec.Children[0].Vector = vec.Children[0].Vector[:0]

	case pb.Type_MAP:
		vec.Vector = vec.Vector[:size]
		// toClarify: will map's children be struct or just primary?
		vec.Children[0].Vector = vec.Children[0].Vector[:0]
		vec.Children[1].Vector = vec.Children[1].Vector[:0]

	case pb.Type_UNION:
		panic("not impl")
	default:
		vec.Vector = vec.Vector[:size]
	}
}

// locate rows in this stripe
func (stripe *stripeReader) Seek(rowNumber uint64) error {
	var offset uint64
	var stride uint64

	if rowNumber < uint64(stripe.ropts.IndexStride) {
		offset = rowNumber
	} else {
		stride := rowNumber / uint64(stripe.ropts.IndexStride)
		offset = rowNumber % (stride * uint64(stripe.ropts.IndexStride))
	}

	if err := stripe.columnReader.SeekStride(int(stride)); err != nil {
		return err
	}

	if err := stripe.columnReader.Skip(offset); err != nil {
		return err
	}
	return nil
}

func (stripe *stripeReader) Close() {
	stripe.columnReader.Close()
}
