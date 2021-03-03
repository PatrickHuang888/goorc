package orc

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io"
	"os"

	"github.com/golang/protobuf/proto"
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/common"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
)

const (
	MAGIC_LENGTH         = uint64(3)
	DIRECTORY_SIZE_GUESS = 16 * 1024
)

func NewOSFileReader(path string) (Reader, error) {
	f, err := orcio.OpenFileForRead(path)
	if err != nil {
		return nil, err
	}
	return newReader(f)
}

type reader struct {
	opts    *config.ReaderOptions
	schemas []*api.TypeDescription

	f            orcio.File
	numberOfRows uint64

	stats   []*pb.ColumnStatistics
	stripes []*pb.StripeInformation
}

func newReader(f orcio.File) (r *reader, err error) {
	opts := &config.ReaderOptions{}
	var tail *pb.FileTail
	if tail, err = extractFileTail(f); err != nil {
		return nil, err
	}

	opts.IndexStride = int(tail.Footer.GetRowIndexStride())

	rows := tail.GetFooter().GetNumberOfRows()
	if rows == 0 {
		return nil, errors.New("file footer error, number of rows 0")
	}

	schemas := unmarshallSchema(tail.Footer.Types)
	for i, stat := range tail.Footer.Statistics {
		schemas[i].HasNulls = stat.GetHasNull()
	}

	opts.CompressionKind = tail.Postscript.GetCompression()
	if opts.CompressionKind != pb.CompressionKind_NONE { // compression_none no block size
		opts.ChunkSize = tail.Postscript.GetCompressionBlockSize()
	} else {
		opts.ChunkSize = config.DefaultChunkSize
	}

	stats := tail.Footer.GetStatistics()
	stripes := tail.Footer.GetStripes()

	// todo: check opts chunksize !=0

	r = &reader{f: f, opts: opts, schemas: schemas, numberOfRows: rows, stats: stats, stripes: stripes}
	return
}

func (r *reader) CreateBatchReader(opts *api.BatchOption) (BatchReader, error) {
	if opts.RowSize == 0 {
		return nil, errors.New("Batch option RowSize == 0")
	}

	br := &batchReader{f: r.f, numberOfRows: r.numberOfRows, ropts: r.opts}

	if opts.Includes == nil {
		br.schema = r.schemas[0]
	} else {
		// todo: includes verification, make sure includes is a tree
		for i, id := range opts.Includes {
			if r.schemas[i].Id==id {
				br.schema= r.schemas[i]
				break
			}
		}
	}

	if err := br.initStripes(r.stripes); err != nil {
		return nil, err
	}
	return br, nil
}

func (r *reader) GetSchema() *api.TypeDescription {
	return r.schemas[0]
}

func (r *reader) GetReaderOptions() *config.ReaderOptions {
	return r.opts
}

func (r *reader) NumberOfRows() uint64 {
	return r.numberOfRows
}

func (r reader) GetStatistics() []*pb.ColumnStatistics {
	return r.stats
}

func (r *reader) Close() {
	if err := r.f.Close(); err != nil {
		logger.Warn(err)
	}
}

type batchReader struct {
	ropts   *config.ReaderOptions
	//opt    *api.BatchOption
	schema *api.TypeDescription

	stripeIndex int
	stripes     []*stripeReader

	f orcio.File

	numberOfRows uint64
	cursor       uint64
}

func (br *batchReader) initStripes(infos []*pb.StripeInformation) error {
	for i, stripeInfo := range infos {
		if stripeInfo.GetNumberOfRows() == 0 {
			return errors.Errorf("stripe number of rows 0 err, %s", stripeInfo.String())
		}
		sr, err := newStripeReader(br.f, br.schema, br.ropts, i, stripeInfo)
		if err != nil {
			return err
		}
		br.stripes = append(br.stripes, sr)
	}
	return nil
}

/*func (br *batchReader) CreateBatch() *api.Batch {
	batch := &api.Batch{}
	if len(br.opts.Includes) == 0 { //all
		for _, s := range br.schemas {
			vec := &api.ColumnVector{Id: s.Id, Kind: s.Kind, Vector: make([]api.Value, 0, br.opts.RowSize)}
			batch.Cols = append(batch.Cols, vec)
		}
	} else {
		for _, id := range br.opts.Includes {
			vec := &api.ColumnVector{Id: id, Kind: br.schemas[id].Kind, Vector: make([]api.Value, 0, br.opts.RowSize)}
			batch.Cols = append(batch.Cols, vec)
		}
	}

	for i := 0; i < len(batch.Cols); i++ {
		for _, child := range br.schemas[batch.Cols[i].Id].Children {
			if batch.Cols[child.Id] != nil {
				batch.Cols[i].Children = append(batch.Cols[i].Children, batch.Cols[child.Id])
			}
		}
	}
	return batch
}*/

func (br *batchReader) Close() {
	for _, s := range br.stripes {
		s.Close()
	}
}

func (br *batchReader) Next(batch *api.ColumnVector) error {
	batch.Clear()

	if br.cursor >= br.numberOfRows-1 {
		return nil
	}

	for br.stripeIndex < len(br.stripes) {
		end, err := br.stripes[br.stripeIndex].next(batch)
		if err != nil {
			return err
		}
		if end {
			br.stripeIndex++
			break
		}
		if batch.Len() >= batch.Cap() {
			break
		}
	}
	if br.stripeIndex == len(br.stripes) {
		br.stripeIndex--
	}
	br.cursor += uint64(batch.Len())
	return nil
}

func (br *batchReader) Seek(rowNumber uint64) error {
	if rowNumber > br.numberOfRows {
		return errors.New("row number larger than number of rows")
	}
	var rows uint64
	for i := 0; i < len(br.stripes); i++ {
		if rows+br.stripes[i].numberOfRows > rowNumber {
			return br.stripes[i].Seek(rowNumber - rows)
		}
		rows += br.stripes[i].numberOfRows
	}
	br.cursor = rowNumber
	return errors.New("no row found")
}

func unmarshallSchema(types []*pb.Type) (schemas []*api.TypeDescription) {
	schemas = make([]*api.TypeDescription, len(types))
	for i, t := range types {
		schemas[i] = &api.TypeDescription{Kind: t.GetKind(), Id: uint32(i)}
	}
	for i, t := range types {
		if len(t.Subtypes) != 0 {
			schemas[i].Children = make([]*api.TypeDescription, len(t.Subtypes))
			schemas[i].ChildrenNames = make([]string, len(t.Subtypes))
			for j, sub := range t.Subtypes {
				if t.GetKind() == pb.Type_STRUCT { // only struct has children names?
					schemas[i].ChildrenNames[j] = t.FieldNames[j]
				}
				schemas[i].Children[j] = schemas[sub]
			}
		}
	}
	return
}

func marshallSchema(schema *api.TypeDescription) (types []*pb.Type) {
	types = preOrderWalkSchema(schema)
	return
}

func preOrderWalkSchema(node *api.TypeDescription) (types []*pb.Type) {
	t := &pb.Type{}
	t.Kind = &node.Kind
	for i, name := range node.ChildrenNames {
		t.FieldNames = append(t.FieldNames, name)
		t.Subtypes = append(t.Subtypes, node.Children[i].Id)
	}
	types = append(types, t)
	for _, n := range node.Children {
		ts := preOrderWalkSchema(n)
		types = append(types, ts...)
	}
	return
}

func extractFileTail(f orcio.File) (tail *pb.FileTail, err error) {
	size, err := f.Size()
	if err != nil {
		return nil, err
	}

	if size == 0 {
		// Hive often creates empty files (including ORC) and has an
		// optimization to create a 0 byte file as an empty ORC file.
		// todo: empty tail, log
		fmt.Printf("file size 0")
		return
	}
	if size <= int64(len(Magic)) {
		return nil, errors.New("not a valid orc file")
	}

	// read last bytes into buffer to get PostScript
	// refactor: buffer 16k length or capacity
	readSize := Min(size, DIRECTORY_SIZE_GUESS)
	buf := make([]byte, readSize)
	if _, err := f.Seek(size-readSize, 0); err != nil {
		return nil, errors.WithStack(err)
	}
	if _, err := io.ReadFull(f, buf); err != nil {
		return nil, errors.WithStack(err)
	}

	// read postScript
	psLen := int64(buf[readSize-1])
	psOffset := readSize - 1 - psLen
	ps, err := extractPostScript(buf[psOffset : psOffset+psLen])
	if err != nil {
		return nil, err
	}
	footerSize := int64(ps.GetFooterLength()) // compressed footer length
	metaSize := int64(ps.GetMetadataLength())

	// check if extra bytes need to be read
	extra := Max(0, psLen+1+footerSize+metaSize-readSize)
	if extra > 0 {
		// more bytes need to be read, read extra bytes
		ebuf := make([]byte, extra)
		if _, err := f.Seek(size-readSize-extra, 0); err != nil {
			return nil, errors.WithStack(err)
		}
		if _, err = io.ReadFull(f, ebuf); err != nil {
			return nil, errors.WithStack(err)
		}
		// refactor: array allocated
		buf = append(buf, ebuf...)
	}

	// read file footer
	footerStart := psOffset - footerSize
	footerBuf := buf[footerStart : footerStart+footerSize]
	if ps.GetCompression() != pb.CompressionKind_NONE {
		fb := bytes.NewBuffer(make([]byte, ps.GetCompressionBlockSize()))
		fb.Reset()
		if err := common.DecompressChunks(ps.GetCompression(), fb, bytes.NewBuffer(footerBuf)); err != nil {
			return nil, errors.WithStack(err)
		}
		footerBuf = fb.Bytes()
	}
	footer := &pb.Footer{}
	if err = proto.Unmarshal(footerBuf, footer); err != nil {
		return nil, errors.Wrapf(err, "unmarshal footer error")
	}

	logger.Debugf("read file footer:\n")
	logger.Debugf("header length: %d\n", footer.GetHeaderLength())
	logger.Debugf("content length %d\n\n", footer.GetContentLength())
	for i, v := range footer.Stripes {
		logger.Debugf("stripe %d: %s\n", i, v.String())
	}
	logger.Debugf("\n")
	for i, v := range footer.Types {
		logger.Debugf("type %d: %s\n", i, v.String())
	}
	logger.Debugf("number of rows: %d\n\n", footer.GetNumberOfRows())
	logger.Debug("Statistics:")
	for i, v := range footer.Statistics {
		logger.Debugf("stats %d: %s", i, v.String())
	}

	fl := uint64(size)
	psl := uint64(psLen)
	ft := &pb.FileTail{Postscript: ps, Footer: footer, FileLength: &fl, PostscriptLength: &psl}
	return ft, nil
}

func extractPostScript(buf []byte) (ps *pb.PostScript, err error) {
	ps = &pb.PostScript{}
	if err = proto.Unmarshal(buf, ps); err != nil {
		return nil, errors.Wrapf(err, "unmarshall postscript err")
	}
	if err = checkOrcVersion(ps); err != nil {
		return nil, errors.Wrapf(err, "check orc version error")
	}

	logger.Debugf("read file postscript: %s", ps.String())

	return ps, err
}

func checkOrcVersion(ps *pb.PostScript) error {
	// todo：
	return nil
}

func ensureOrcFooter(f *os.File, psLen int, buf []byte) error {
	magicLength := len(Magic)
	fullLength := magicLength + 1
	if psLen < fullLength || len(buf) < fullLength {
		return errors.Errorf("malformed ORC file %stream, invalid postscript length %d", f.Name(), psLen)
	}
	// now look for the magic string at the end of the postscript.
	//if (!Text.decode(array, offset, magicLength).equals(OrcFile.Magic)) {
	offset := len(buf) - fullLength
	// fixme: encoding
	if string(buf[offset:]) != Magic {
		// If it isn't there, this may be the 0.11.0 version of ORC.
		// Read the first 3 bytes of the file to check for the header
		// todo:

		return errors.Errorf("malformed ORC file %stream, invalid postscript", f.Name())
	}
	return nil
}

func MinUint64(x, y uint64) uint64 {
	if x < y {
		return x
	}
	return y
}

func Min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

func Max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}

func ReadChunks(chunksBuf []byte, compressKind pb.CompressionKind, chunkBufferSize int) (decompressed []byte, err error) {
	for offset := 0; offset < len(chunksBuf); {
		// header
		original := (chunksBuf[offset] & 0x01) == 1
		chunkLength := int(chunksBuf[offset+2])<<15 | int(chunksBuf[offset+1])<<7 |
			int(chunksBuf[offset])>>1
		buf := make([]byte, chunkBufferSize)
		//fixme:
		if chunkLength > chunkBufferSize {
			return nil, errors.New("chunk length larger than compression block size")
		}
		offset += 3

		if original {
			//fixme:
			decompressed = append(decompressed, chunksBuf[offset:offset+chunkLength]...)
		} else {
			switch compressKind {
			case pb.CompressionKind_ZLIB:
				r := flate.NewReader(bytes.NewReader(chunksBuf[offset : offset+chunkLength]))
				n, err := r.Read(buf)
				r.Close()
				if err != nil && err != io.EOF {
					return nil, errors.Wrapf(err, "decompress chunk data error when read footer")
				}
				if n == 0 {
					return nil, errors.New("decompress 0 footer")
				}
				//fixme:
				decompressed = append(decompressed, buf[:n]...)
			default:
				//todo:
				return nil, errors.New("compress other than zlib not implemented")
			}
		}
		offset += chunkLength
	}
	return
}
