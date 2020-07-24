package orc

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/patrickhuang888/goorc/orc/encoding"
	"github.com/patrickhuang888/goorc/pb/pb"
)

const (
	MAGIC_LENGTH         = uint64(3)
	DIRECTORY_SIZE_GUESS = 16 * 1024
)

type Reader interface {
	GetSchema() *TypeDescription

	NumberOfRows() uint64

	Close() error

	Next(batch *ColumnVector) error

	GetStatistics() []*pb.ColumnStatistics
}

type ReaderOptions struct {
	CompressionKind pb.CompressionKind
	ChunkSize       uint64
	RowSize         int

	Loc *time.Location
}

func DefaultReaderOptions() *ReaderOptions {
	return &ReaderOptions{RowSize: DEFAULT_ROW_SIZE, ChunkSize: DEFAULT_CHUNK_SIZE,
		CompressionKind: pb.CompressionKind_ZLIB}
}

type File interface {
	io.ReadSeeker
	io.Closer
	Size() (int64, error)
}

type reader struct {
	opts    *ReaderOptions
	schemas []*TypeDescription

	stripes []*stripeReader

	stripeIndex int

	f File

	numberOfRows uint64
	stats        []*pb.ColumnStatistics
}

type fileFile struct {
	f *os.File
}

func (fr fileFile) Size() (size int64, err error) {
	fi, err := fr.f.Stat()
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return fi.Size(), nil
}

func (fr fileFile) Read(p []byte) (n int, err error) {
	n, err = fr.f.Read(p)
	if err != nil {
		return n, errors.WithStack(err)
	}
	return
}

func (fr fileFile) Seek(offset int64, whence int) (int64, error) {
	return fr.f.Seek(offset, whence)
}

func (fr fileFile) Close() error {
	if err := fr.f.Close(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func NewFileReader(path string, opts *ReaderOptions) (r Reader, err error) {
	var f *os.File
	f, err = os.Open(path)
	if err != nil {
		return nil, errors.Wrapf(err, "open file %s error", path)
	}
	log.Infof("open file %s", path)
	fr := &fileFile{f: f}

	r, err = newReader(opts, fr)
	if err != nil {
		return
	}

	return
}

func newReader(opts *ReaderOptions, f File) (r *reader, err error) {
	tail, err := extractFileTail(f)
	if err != nil {
		return nil, errors.Wrap(err, "read file tail error")
	}

	//check
	if tail.GetFooter().GetNumberOfRows() == 0 {
		return nil, errors.New("file footer error, number of rows 0")
	}

	schemas := unmarshallSchema(tail.Footer.Types)

	for i, stat := range tail.Footer.Statistics {
		schemas[i].HasNulls = stat.GetHasNull()
	}

	opts.CompressionKind = tail.Postscript.GetCompression()
	if opts.CompressionKind != pb.CompressionKind_NONE { // compression_none no block size
		opts.ChunkSize = tail.Postscript.GetCompressionBlockSize()
	}

	r = &reader{f: f, opts: opts, schemas: schemas, numberOfRows: tail.GetFooter().GetNumberOfRows(),
		stats: tail.Footer.GetStatistics()}

	r.initStripes(tail.Footer.GetStripes())

	return
}

func (r *reader) GetSchema() *TypeDescription {
	return r.schemas[0]
}

func (r *reader) initStripes(infos []*pb.StripeInformation) error {
	var err error

	for i, stripeInfo := range infos {

		//check
		if stripeInfo.GetNumberOfRows() == 0 {
			return errors.Errorf("stripe number of rows 0 err, %s", stripeInfo.String())
		}

		offset := stripeInfo.GetOffset()
		indexLength := stripeInfo.GetIndexLength()
		dataLength := stripeInfo.GetDataLength()

		// row index
		indexOffset := offset
		log.Tracef("seek index of stripe %d", i)
		if _, err = r.f.Seek(int64(indexOffset), 0); err != nil {
			return errors.WithStack(err)
		}
		indexBuf := make([]byte, indexLength)
		if _, err = io.ReadFull(r.f, indexBuf); err != nil {
			return errors.WithStack(err)
		}
		if r.opts.CompressionKind != pb.CompressionKind_NONE {
			ib := &bytes.Buffer{}
			if err = decompressBuffer(r.opts.CompressionKind, ib, bytes.NewBuffer(indexBuf)); err != nil {
				return err
			}
			indexBuf = ib.Bytes()
		}
		index := &pb.RowIndex{}
		if err = proto.Unmarshal(indexBuf, index); err != nil {
			return errors.Wrapf(err, "unmarshal strip index error")
		}

		// footer
		log.Tracef("seek stripe footer of %d", i)
		footerOffset := int64(offset + indexLength + dataLength)
		if _, err = r.f.Seek(footerOffset, 0); err != nil {
			return errors.WithStack(err)
		}
		footerBuf := make([]byte, stripeInfo.GetFooterLength())
		if _, err = io.ReadFull(r.f, footerBuf); err != nil {
			return errors.WithStack(err)
		}
		if r.opts.CompressionKind != pb.CompressionKind_NONE {
			fb := &bytes.Buffer{}
			if err = decompressBuffer(r.opts.CompressionKind, fb, bytes.NewBuffer(footerBuf)); err != nil {
				return err
			}
			footerBuf = fb.Bytes()
		}
		footer := &pb.StripeFooter{}
		if err = proto.Unmarshal(footerBuf, footer); err != nil {
			return errors.Wrapf(err, "unmarshal currentStripe footer error")
		}
		log.Debugf("extracted stripe footer %d: %s", i, footer.String())

		var sr *stripeReader
		if sr, err = newStripeReader(r.f, r.schemas, r.opts, i, stripeInfo, footer); err != nil {
			return err
		}

		r.stripes = append(r.stripes, sr)

	}

	return nil
}

func (r reader) GetStatistics() []*pb.ColumnStatistics {
	return r.stats
}

func (r *reader) Close() error {
	return r.f.Close()
}

func (r *reader) Next(batch *ColumnVector) (err error) {
	if err = r.stripes[r.stripeIndex].next(batch); err != nil {
		return
	}

	// next stripe
	if (r.stripeIndex < len(r.stripes)-1) && (batch.ReadRows == 0) {
		r.stripeIndex++
		if err = r.stripes[r.stripeIndex].next(batch); err != nil {
			return
		}
	}
	return
}

type stripeReader struct {
	in io.ReadSeeker

	schemas []*TypeDescription
	opts    *ReaderOptions

	columnReaders []columnReader

	idx int
}

func newStripeReader(in io.ReadSeeker, schemas []*TypeDescription, opts *ReaderOptions, idx int, info *pb.StripeInformation,
	footer *pb.StripeFooter) (reader *stripeReader, err error) {

	reader = &stripeReader{in: in, schemas: schemas, opts: opts, idx: idx}

	err = reader.prepare(info, footer)

	return
}

// stripe {index{},column{[present],data,[length]},footer}
func (s *stripeReader) prepare(info *pb.StripeInformation, footer *pb.StripeFooter) error {

	//prepare column reader
	s.columnReaders = make([]columnReader, len(s.schemas))
	columns := make([]*treeReader, len(s.schemas))
	// id==i
	for _, schema := range s.schemas {
		encoding := footer.GetColumns()[schema.Id].GetKind()

		c := &treeReader{schema: schema, numberOfRows: info.GetNumberOfRows()}
		columns[schema.Id] = c

		switch schema.Kind {
		case pb.Type_SHORT:
			fallthrough
		case pb.Type_INT:
			fallthrough
		case pb.Type_LONG:
			if encoding == pb.ColumnEncoding_DIRECT_V2 {
				s.columnReaders[schema.Id] = &longV2Reader{treeReader: c}
				break
			}
			return errors.New("not impl")

		case pb.Type_FLOAT:
		// todo:

		case pb.Type_DOUBLE:
			if encoding != pb.ColumnEncoding_DIRECT {
				return errors.New("column encoding error")
			}
			s.columnReaders[schema.Id] = &doubleReader{treeReader: c}

		case pb.Type_STRING:
			if encoding == pb.ColumnEncoding_DIRECT_V2 {
				s.columnReaders[schema.Id] = &stringDirectV2Reader{treeReader: c}
				break
			}
			if encoding == pb.ColumnEncoding_DICTIONARY_V2 {
				s.columnReaders[schema.Id] = &stringDictV2Reader{treeReader: c}
				break
			}
			return errors.New("column encoding error")

		case pb.Type_BOOLEAN:
			if encoding != pb.ColumnEncoding_DIRECT {
				return errors.New("bool column encoding error")
			}
			s.columnReaders[schema.Id] = &boolReader{treeReader: c}

		case pb.Type_BYTE: // tinyint
			if encoding != pb.ColumnEncoding_DIRECT {
				return errors.New("tinyint column encoding error")
			}
			s.columnReaders[schema.Id] = &byteReader{treeReader: c}

		case pb.Type_BINARY:
			if encoding == pb.ColumnEncoding_DIRECT {
				// todo:
				return errors.New("not impl")
				break
			}
			if encoding == pb.ColumnEncoding_DIRECT_V2 {
				s.columnReaders[schema.Id] = &binaryV2Reader{treeReader: c}
				break
			}
			return errors.New("binary column encoding error")

		case pb.Type_DECIMAL:
			if encoding == pb.ColumnEncoding_DIRECT {
				// todo:
				return errors.New("not impl")
				break
			}
			if encoding == pb.ColumnEncoding_DIRECT_V2 {
				s.columnReaders[schema.Id] = &decimal64DirectV2Reader{treeReader: c}
				break
			}
			return errors.New("column encoding error")

		case pb.Type_DATE:
			if encoding == pb.ColumnEncoding_DIRECT {
				// todo:
				return errors.New("not impl")
				break
			}
			if encoding == pb.ColumnEncoding_DIRECT_V2 {
				s.columnReaders[schema.Id] = &dateV2Reader{treeReader: c}
				break
			}
			return errors.New("column encoding error")

		case pb.Type_TIMESTAMP:
			if encoding == pb.ColumnEncoding_DIRECT {
				// todo:
				return errors.New("not impl")
				break
			}

			if encoding == pb.ColumnEncoding_DIRECT_V2 {
				s.columnReaders[schema.Id] = &timestampV2Reader{treeReader: c}
				break
			}
			return errors.New("column encoding error")

		case pb.Type_STRUCT:
			if encoding != pb.ColumnEncoding_DIRECT {
				return errors.New("encoding error")
			}
			s.columnReaders[schema.Id] = &structReader{treeReader: c}

		case pb.Type_LIST:
			if encoding == pb.ColumnEncoding_DIRECT {
				// todo:
				return errors.New("not impl")
				break
			}
			if encoding == pb.ColumnEncoding_DIRECT_V2 {
				// todo:
				break
			}
			return errors.New("encoding error")

		case pb.Type_MAP:
			if encoding == pb.ColumnEncoding_DIRECT {
				// todo:
				return errors.New("not impl")
				break
			}
			if encoding == pb.ColumnEncoding_DIRECT_V2 {
				// todo:
				break
			}
			return errors.New("encoding error")

		case pb.Type_UNION:
			if encoding != pb.ColumnEncoding_DIRECT {
				return errors.New("column encoding error")
			}
		//todo:
		// fixme: pb.Stream_DIRECT

		default:
			return errors.New("type unwkone")
		}
	}

	// build tree
	for _, schema := range s.schemas {
		if schema.Kind == pb.Type_STRUCT {
			var crs []columnReader
			for _, childSchema := range schema.Children {
				crs = append(crs, s.columnReaders[childSchema.Id])
			}
			s.columnReaders[schema.Id].(*structReader).children = crs
		}
	}

	// setup streams
	// streams has sequence
	indexStart := info.GetOffset()
	dataStart := indexStart + info.GetIndexLength()

	for _, streamInfo := range footer.GetStreams() {

		id := streamInfo.GetColumn()
		schema := s.schemas[id]
		streamKind := streamInfo.GetKind()
		length := streamInfo.GetLength()

		if streamKind == pb.Stream_ROW_INDEX {
			// todo: init index streamReader
			indexStart += length
			continue
		}

		if streamKind == pb.Stream_PRESENT {
			if !schema.HasNulls {
				return errors.New("reader column %d has !HasNulls, why has present stream?")
			}
			columns[id].present = newBoolStreamReader(s.opts, streamInfo, dataStart, s.in)
			dataStart += length
			continue
		}

		encoding := footer.GetColumns()[id].GetKind()
		switch schema.Kind {
		case pb.Type_SHORT:
			fallthrough
		case pb.Type_INT:
			fallthrough
		case pb.Type_LONG:
			if encoding == pb.ColumnEncoding_DIRECT_V2 {
				reader := s.columnReaders[id].(*longV2Reader)
				if streamKind == pb.Stream_DATA {
					reader.data = newLongV2StreamReader(s.opts, streamInfo, dataStart, s.in, true)
					break
				}
				return errors.New("stream kind error")
			}

		case pb.Type_FLOAT:
			reader := s.columnReaders[id].(*floatReader)
			if streamKind == pb.Stream_DATA {
				reader.data = newFloatStreamReader(s.opts, streamInfo, dataStart, s.in)
				break
			}
			return errors.New("stream kind error")

		case pb.Type_DOUBLE:
			reader := s.columnReaders[id].(*doubleReader)
			if streamKind == pb.Stream_DATA {
				reader.data = newDoubleStreamReader(s.opts, streamInfo, dataStart, s.in)
				break
			}
			return errors.New("stream kind error")

		case pb.Type_STRING:
			if encoding == pb.ColumnEncoding_DIRECT_V2 {
				reader := s.columnReaders[id].(*stringDirectV2Reader)

				if streamKind == pb.Stream_DATA {
					reader.data = newStringContentsStreamReader(s.opts, streamInfo, dataStart, s.in)
					break
				}
				if streamKind == pb.Stream_LENGTH {
					reader.length = newLongV2StreamReader(s.opts, streamInfo, dataStart, s.in, false)
					break
				}
				return errors.New("stream kind error")
			}

			if encoding == pb.ColumnEncoding_DICTIONARY_V2 {
				reader := s.columnReaders[id].(*stringDictV2Reader)
				if streamKind == pb.Stream_DATA {
					data := newLongV2StreamReader(s.opts, streamInfo, dataStart, s.in, false)
					reader.data = data
					break
				}
				if streamKind == pb.Stream_DICTIONARY_DATA {
					dictData := newStringContentsStreamReader(s.opts, streamInfo, dataStart, s.in)
					reader.dictData = dictData
					break
				}
				if streamKind == pb.Stream_LENGTH {
					dictLength := newLongV2StreamReader(s.opts, streamInfo, dataStart, s.in, false)
					reader.dictLength = dictLength
					break
				}
				return errors.New("stream kind error")
			}

		case pb.Type_BOOLEAN:
			reader := s.columnReaders[id].(*boolReader)
			if streamKind == pb.Stream_DATA {
				reader.data = newBoolStreamReader(s.opts, streamInfo, dataStart, s.in)
				break
			}
			return errors.New("stream kind error")

		case pb.Type_BYTE: // tinyint
			reader := s.columnReaders[id].(*byteReader)
			if streamKind == pb.Stream_DATA {
				reader.data = newByteStreamReader(s.opts, streamInfo, dataStart, s.in)
				break
			}
			return errors.New("stream kind error")

		case pb.Type_BINARY:
			if encoding == pb.ColumnEncoding_DIRECT {
				// todo:
				return errors.New("not impl")
				break
			}
			if encoding == pb.ColumnEncoding_DIRECT_V2 {
				reader := s.columnReaders[id].(*binaryV2Reader)
				if streamKind == pb.Stream_DATA {
					reader.data = newStringContentsStreamReader(s.opts, streamInfo, dataStart, s.in)
					break
				}
				if streamKind == pb.Stream_LENGTH {
					reader.length = newLongV2StreamReader(s.opts, streamInfo, dataStart, s.in, false)
					break
				}
				return errors.New("stream kind error")
			}

		case pb.Type_DECIMAL:
			if encoding == pb.ColumnEncoding_DIRECT {
				// todo:
				return errors.New("not impl")
				break
			}
			if encoding == pb.ColumnEncoding_DIRECT_V2 {
				reader := s.columnReaders[id].(*decimal64DirectV2Reader)
				if streamKind == pb.Stream_DATA {
					reader.data = newVarIntStreamReader(s.opts, streamInfo, dataStart, s.in)
					break
				}
				if streamKind == pb.Stream_SECONDARY {
					reader.secondary = newLongV2StreamReader(s.opts, streamInfo, dataStart, s.in, false)
					break
				}
				return errors.New("stream kind error")
			}

		case pb.Type_DATE:
			if encoding == pb.ColumnEncoding_DIRECT {
				// todo:
				return errors.New("not impl")
				break
			}
			if encoding == pb.ColumnEncoding_DIRECT_V2 {
				reader := s.columnReaders[id].(*dateV2Reader)
				if streamKind == pb.Stream_DATA {
					reader.data = newLongV2StreamReader(s.opts, streamInfo, dataStart, s.in, false)
					break
				}
				return errors.New("stream kind error")
			}

		case pb.Type_TIMESTAMP:
			if encoding == pb.ColumnEncoding_DIRECT {
				// todo:
				return errors.New("not impl")
				break
			}

			if encoding == pb.ColumnEncoding_DIRECT_V2 {
				reader := s.columnReaders[id].(*timestampV2Reader)
				if streamKind == pb.Stream_DATA {
					reader.data = newLongV2StreamReader(s.opts, streamInfo, dataStart, s.in, true)
					break
				}
				if streamKind == pb.Stream_SECONDARY {
					reader.secondary = newLongV2StreamReader(s.opts, streamInfo, dataStart, s.in, false)
					break
				}
				return errors.New("stream kind error")
			}

		case pb.Type_STRUCT:
			// no data

		case pb.Type_LIST:
			if encoding == pb.ColumnEncoding_DIRECT {
				// todo:
				return errors.New("not impl")
				break
			}
			if encoding == pb.ColumnEncoding_DIRECT_V2 {
				if streamKind == pb.Stream_LENGTH {
					// todo:
					//decoder := &encoding.IntRleV2{}
					//length := &longStream{r: r, decoder: decoder}

				}
				break
			}
			return errors.New("encoding error")

		case pb.Type_MAP:
			if encoding == pb.ColumnEncoding_DIRECT {
				// todo:
				return errors.New("not impl")
				break
			}
			if encoding == pb.ColumnEncoding_DIRECT_V2 {
				if streamKind == pb.Stream_LENGTH {
					//decoder := &encoding.IntRleV2{}
					//length := &longStream{r: r, decoder: decoder}
					// todo:
				}
			}

		case pb.Type_UNION:
			if encoding != pb.ColumnEncoding_DIRECT {
				return errors.New("column encoding error")
			}

			// fixme: pb.Stream_DIRECT

		}

		dataStart += length
	}

	// todo: streamreader needed existing check

	/*for _, v := range crs {
		v.Print()
	}*/
	return nil
}

// entry of stripeR reader
// a stripeR is typically  ~200MB
func (s *stripeReader) next(batch *ColumnVector) error {

	err := s.columnReaders[batch.Id].next(batch)
	if err != nil {
		return err
	}

	log.Debugf("stripe %d column %d has read %d", s.idx, batch.Id, batch.ReadRows)

	return nil
}

type columnReader interface {
	next(batch *ColumnVector) error
}


type dateV2Reader struct {
	*treeReader
	data *longV2StreamReader
}

func (c *dateV2Reader) next(batch *ColumnVector) error {
	vector := batch.Vector.([]Date)
	vector = vector[:0]

	if err := c.nextPresents(batch); err != nil {
		return err
	}

	for i := 0; i < cap(vector) && c.cursor < c.numberOfRows; i++ {

		if len(batch.Presents) == 0 || (len(batch.Presents) != 0 && batch.Presents[i]) {
			v, err := c.data.nextInt64()
			if err != nil {
				return err
			}
			vector = append(vector, fromDays(v))

		} else {
			vector = append(vector, Date{})
		}

		c.cursor++
	}

	batch.Vector = vector
	batch.ReadRows = len(vector)
	return nil
}

type timestampV2Reader struct {
	*treeReader

	data      *longV2StreamReader
	secondary *longV2StreamReader
}

func (c *timestampV2Reader) next(batch *ColumnVector) error {
	vector := batch.Vector.([]Timestamp)
	vector = vector[:0]

	if err := c.nextPresents(batch); err != nil {
		return err
	}

	for i := 0; i < cap(vector) && c.cursor < c.numberOfRows; i++ {

		if len(batch.Presents) == 0 || (len(batch.Presents) != 0 && batch.Presents[i]) {
			seconds, err := c.data.nextInt64()
			if err != nil {
				return err
			}
			nanos, err := c.secondary.nextUInt()
			if err != nil {
				return err
			}
			vector = append(vector, Timestamp{seconds, uint32(encoding.DecodingNano(nanos))})

		} else {
			vector = append(vector, Timestamp{})
		}

		c.cursor++
	}

	if (c.data.finished() && !c.secondary.finished()) || (c.secondary.finished() && !c.data.finished()) {
		return errors.New("read error")
	}

	batch.Vector = vector
	batch.ReadRows = len(vector)
	return nil
}

type boolReader struct {
	*treeReader
	data *boolStreamReader
}

func (c *boolReader) next(batch *ColumnVector) error {
	vector := batch.Vector.([]bool)
	vector = vector[:0]

	if err := c.nextPresents(batch); err != nil {
		return err
	}

	for i := 0; i < cap(vector) && c.cursor < c.numberOfRows; i++ {

		if len(batch.Presents) == 0 || (len(batch.Presents) != 0 && batch.Presents[i]) {
			v, err := c.data.next()
			if err != nil {
				return err
			}
			vector = append(vector, v)
		} else {
			vector = append(vector, false)
		}

		c.cursor++
	}

	batch.Vector = vector
	batch.ReadRows = len(vector)
	return nil
}

type binaryV2Reader struct {
	*treeReader
	length *longV2StreamReader
	data   *stringContentsStreamReader
}

func (c *binaryV2Reader) next(batch *ColumnVector) error {
	vector := batch.Vector.([][]byte)
	vector = vector[:0]

	if err := c.nextPresents(batch); err != nil {
		return err
	}

	for i := 0; i < cap(vector) && c.cursor < c.numberOfRows; i++ {
		if len(batch.Presents) == 0 || (len(batch.Presents) != 0 && batch.Presents[i]) {
			l, err := c.length.nextUInt()
			if err != nil {
				return err
			}
			v, err := c.data.nextBytes(l)
			if err != nil {
				return err
			}
			// default utf-8
			vector = append(vector, v)

		} else {
			vector = append(vector, nil)
		}

		c.cursor++
	}

	if (c.length.finished() && !c.data.finished()) || (c.data.finished() && !c.length.finished()) {
		return errors.New("read error")
	}

	batch.Vector = vector
	batch.ReadRows = len(vector)
	return nil
}

type stringDirectV2Reader struct {
	*treeReader
	data   *stringContentsStreamReader
	length *longV2StreamReader
}

func (c *stringDirectV2Reader) next(batch *ColumnVector) error {
	vector := batch.Vector.([]string)
	vector = vector[:0]

	if err := c.nextPresents(batch); err != nil {
		return err
	}

	for i := 0; i < cap(vector) && c.cursor < c.numberOfRows; i++ {
		if len(batch.Presents) == 0 || (len(batch.Presents) != 0 && batch.Presents[i]) {
			l, err := c.length.nextUInt()
			if err != nil {
				return err
			}
			v, err := c.data.next(l)
			if err != nil {
				return err
			}
			// default utf-8
			vector = append(vector, string(v))

		} else {
			vector = append(vector, "")
		}

		c.cursor++
	}

	if (c.length.finished() && !c.data.finished()) || (c.data.finished() && !c.length.finished()) {
		return errors.New("read error")
	}

	batch.Vector = vector
	batch.ReadRows = len(vector)
	return nil
}

type stringDictV2Reader struct {
	*treeReader

	data       *longV2StreamReader
	dictData   *stringContentsStreamReader
	dictLength *longV2StreamReader

	dict    []string
	lengths []uint64
}

func (c *stringDictV2Reader) next(batch *ColumnVector) error {
	var err error
	vector := batch.Vector.([]string)
	vector = vector[:0]

	// rethink: len(lengths)==0
	if len(c.lengths) == 0 {
		c.lengths, err = c.dictLength.getAllUInts()
		if err != nil {
			return err
		}
	}

	// rethink: len(dict)==0
	if len(c.dict) == 0 {
		c.dict, err = c.dictData.getAll(c.lengths)
		if err != nil {
			return err
		}
	}

	if err = c.nextPresents(batch); err != nil {
		return err
	}

	for i := 0; i < cap(vector) && c.cursor < c.numberOfRows; i++ {

		if len(batch.Presents) == 0 || (len(batch.Presents) != 0 && batch.Presents[i]) {
			v, err := c.data.nextUInt()
			if err != nil {
				return err
			}
			if v >= uint64(len(c.dict)) {
				return errors.New("dict index error")
			}
			vector = append(vector, c.dict[v])

		} else {
			vector = append(vector, "")
		}

		c.cursor++
	}

	batch.Vector = vector
	batch.ReadRows = len(vector)
	return nil
}

type longV2Reader struct {
	*treeReader
	data *longV2StreamReader
}

func (c *longV2Reader) next(batch *ColumnVector) error {
	vector := batch.Vector.([]int64)
	vector = vector[:0]

	if err := c.nextPresents(batch); err != nil {
		return err
	}

	for i := 0; i < cap(vector) && c.cursor < c.numberOfRows; i++ {

		if len(batch.Presents) == 0 || (len(batch.Presents) != 0 && batch.Presents[i]) {
			v, err := c.data.nextInt64()
			if err != nil {
				return err
			}
			vector = append(vector, v)

		} else {
			vector = append(vector, 0)
		}

		c.cursor++
	}

	batch.Vector = vector
	batch.ReadRows = len(vector)
	return nil
}

type decimal64DirectV2Reader struct {
	*treeReader
	data      *varIntStreamReader
	secondary *longV2StreamReader
}

func (c *decimal64DirectV2Reader) next(batch *ColumnVector) error {
	vector := batch.Vector.([]Decimal64)
	vector = vector[:0]

	if err := c.nextPresents(batch); err != nil {
		return err
	}

	for i := 0; i < cap(vector) && c.cursor < c.numberOfRows; i++ {

		if len(batch.Presents) == 0 || (len(batch.Presents) != 0 && batch.Presents[i]) {
			precision, err := c.data.next()
			if err != nil {
				return err
			}
			scala, err := c.secondary.nextInt64()
			if err != nil {
				return err
			}
			vector = append(vector, Decimal64{precision, int(scala)})

		} else {
			vector = append(vector, Decimal64{})
		}

		c.cursor++
	}

	if (c.data.finished() && !c.secondary.finished()) || (c.secondary.finished() && !c.data.finished()) {
		return errors.New("read error")
	}

	batch.Vector = vector
	batch.ReadRows = len(vector)
	return nil
}

type floatReader struct {
	*treeReader
	data *floatStreamReader
}

func (c *floatReader) next(batch *ColumnVector) error {
	vector := batch.Vector.([]float32)
	vector = vector[:0]

	if err := c.nextPresents(batch); err != nil {
		return err
	}

	for i := 0; i < cap(vector) && c.cursor < c.numberOfRows; i++ {

		if len(batch.Presents) == 0 || (len(batch.Presents) != 0 && batch.Presents[i]) {
			v, err := c.data.next()
			if err != nil {
				return err
			}
			vector = append(vector, v)

		} else {
			vector = append(vector, 0)
		}

		c.cursor++
	}

	batch.Vector = vector
	batch.ReadRows = len(vector)
	return nil
}

type doubleReader struct {
	*treeReader
	data *doubleStreamReader
}

func (c *doubleReader) next(batch *ColumnVector) error {
	vector := batch.Vector.([]float64)
	vector = vector[:0]

	if err := c.nextPresents(batch); err != nil {
		return err
	}

	for i := 0; i < cap(vector) && c.cursor < c.numberOfRows; i++ {

		if len(batch.Presents) == 0 || (len(batch.Presents) != 0 && batch.Presents[i]) {
			v, err := c.data.next()
			if err != nil {
				return err
			}
			vector = append(vector, v)

		} else {
			vector = append(vector, 0)
		}

		c.cursor++
	}

	batch.Vector = vector
	batch.ReadRows = len(vector)
	return nil
}

type structReader struct {
	*treeReader
	children []columnReader
}

func (c *structReader) next(batch *ColumnVector) error {

	if err := c.nextPresents(batch); err != nil {
		return err
	}

	vector := batch.Vector.([]*ColumnVector)

	for i, child := range c.children {
		if len(batch.Presents) != 0 {
			// todo: it should check in prepare that
			// there will be no presents is children while parent has presents

			// reassure: if parent has presents, children use it
			vector[i].Presents = batch.Presents
		}

		if err := child.next(vector[i]); err != nil {
			return err
		}
	}

	// reassure: no present, so readrows same as children readrows?
	if len(batch.Presents) == 0 {
		batch.ReadRows = vector[0].ReadRows
	} else {
		batch.ReadRows = len(batch.Presents)
	}

	c.cursor += uint64(batch.ReadRows)

	return nil
}


type stringContentsStreamReader struct {
	stream  *streamReader
	decoder *encoding.BytesContent
}

func newStringContentsStreamReader(opts *ReaderOptions, info *pb.Stream, start uint64, in io.ReadSeeker) *stringContentsStreamReader {
	sr := &streamReader{info: info, buf: &bytes.Buffer{}, in: in, start: start, compressionKind: opts.CompressionKind,
		chunkSize: opts.ChunkSize}
	return &stringContentsStreamReader{stream: sr, decoder: &encoding.BytesContent{}}
}

func (r *stringContentsStreamReader) nextBytes(byteLength uint64) (v []byte, err error) {
	v, err = r.decoder.DecodeNext(r.stream, int(byteLength))
	return
}

func (r *stringContentsStreamReader) next(byteLength uint64) (v string, err error) {
	var bb []byte
	bb, err = r.nextBytes(byteLength)
	if err != nil {
		return
	}
	return string(bb), err
}

func (r *stringContentsStreamReader) finished() bool {
	return r.stream.finished()
}

// for read column using encoding like dict
func (r *stringContentsStreamReader) getAll(byteLengths []uint64) (vs []string, err error) {
	for !r.finished() {
		// todo: data check
		for _, l := range byteLengths {
			var v string
			v, err = r.next(l)
			if err != nil {
				return
			}
			vs = append(vs, v)
		}
	}
	return
}

type doubleStreamReader struct {
	stream  *streamReader
	decoder *encoding.Ieee754Double
}

func newDoubleStreamReader(opts *ReaderOptions, info *pb.Stream, start uint64,
	in io.ReadSeeker) *doubleStreamReader {
	sr := &streamReader{start: start, info: info, buf: &bytes.Buffer{}, in: in, compressionKind: opts.CompressionKind,
		chunkSize: opts.ChunkSize}
	return &doubleStreamReader{stream: sr, decoder: &encoding.Ieee754Double{}}
}

func (r *doubleStreamReader) next() (v float64, err error) {
	return r.decoder.Decode(r.stream)
}

func (r *doubleStreamReader) finished() bool {
	return r.stream.finished()
}

type floatStreamReader struct {
	stream  *streamReader
	decoder *encoding.Ieee754Float
}

func newFloatStreamReader(opts *ReaderOptions, info *pb.Stream, start uint64, in io.ReadSeeker) *floatStreamReader {
	sr := &streamReader{start: start, info: info, buf: &bytes.Buffer{}, in: in, compressionKind: opts.CompressionKind,
		chunkSize: opts.ChunkSize}
	return &floatStreamReader{stream: sr, decoder: &encoding.Ieee754Float{}}
}

func (r *floatStreamReader) next() (v float32, err error) {
	return r.decoder.Decode(r.stream)
}

func (r *floatStreamReader) finished() bool {
	return r.stream.finished()
}

type varIntStreamReader struct {
	stream *streamReader

	decoder *encoding.Base128VarInt
}

func newVarIntStreamReader(opts *ReaderOptions, info *pb.Stream, start uint64, in io.ReadSeeker) *varIntStreamReader {
	sr := &streamReader{info: info, start: start, buf: &bytes.Buffer{}, in: in, compressionKind: opts.CompressionKind,
		chunkSize: opts.ChunkSize}
	return &varIntStreamReader{stream: sr, decoder: &encoding.Base128VarInt{}}
}

func (r *varIntStreamReader) next() (v int64, err error) {
	v, err = r.decoder.DecodeNext(r.stream)
	return
}

func (r *varIntStreamReader) finished() bool {
	return r.stream.finished()
}

type longV2StreamReader struct {
	stream *streamReader

	values []uint64
	pos    int

	decoder *encoding.IntRleV2
}

func newLongV2StreamReader(opts *ReaderOptions, info *pb.Stream, start uint64, in io.ReadSeeker, signed bool) *longV2StreamReader {
	sr := &streamReader{info: info, start: start, buf: &bytes.Buffer{}, in: in, compressionKind: opts.CompressionKind,
		chunkSize: opts.ChunkSize}
	return &longV2StreamReader{stream: sr, decoder: &encoding.IntRleV2{Signed: signed}}
}

func (r *longV2StreamReader) nextInt64() (v int64, err error) {
	var uv uint64
	uv, err = r.nextUInt()
	if err != nil {
		return
	}
	v = encoding.UnZigzag(uv)
	return
}

func (r *longV2StreamReader) nextUInt() (v uint64, err error) {
	if r.pos >= len(r.values) {
		r.pos = 0
		r.values = r.values[:0]

		if r.values, err = r.decoder.Decode(r.stream, r.values); err != nil {
			return
		}

		log.Tracef("stream long read column %d has read %d values", r.stream.info.GetColumn(), len(r.values))
	}

	v = r.values[r.pos]
	r.pos++
	return
}

// for small data like dict index, ignore stream.signed
func (r *longV2StreamReader) getAllUInts() (vs []uint64, err error) {
	for !r.stream.finished() {
		if vs, err = r.decoder.Decode(r.stream, vs); err != nil {
			return
		}
	}
	return
}

func (r *longV2StreamReader) finished() bool {
	return r.stream.finished() && (r.pos == len(r.values))
}



func (r *reader) NumberOfRows() uint64 {
	return r.numberOfRows
}

func unmarshallSchema(types []*pb.Type) (schemas []*TypeDescription) {
	schemas = make([]*TypeDescription, len(types))
	for i, t := range types {
		node := &TypeDescription{Kind: t.GetKind(), Id: uint32(i)}
		schemas[i] = node
	}
	for i, t := range types {
		schemas[i].Children = make([]*TypeDescription, len(t.Subtypes))
		schemas[i].ChildrenNames = make([]string, len(t.Subtypes))
		for j, v := range t.Subtypes {
			schemas[i].ChildrenNames[j] = t.FieldNames[j]
			schemas[i].Children[j] = schemas[v]
		}
	}
	return
}

func marshallSchema(schema *TypeDescription) (types []*pb.Type) {
	types = preOrderWalkSchema(schema)
	return
}

func preOrderWalkSchema(node *TypeDescription) (types []*pb.Type) {
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

func extractFileTail(f File) (tail *pb.FileTail, err error) {

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
	if size <= int64(len(MAGIC)) {
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
		if err := decompressBuffer(ps.GetCompression(), fb, bytes.NewBuffer(footerBuf)); err != nil {
			return nil, errors.WithStack(err)
		}
		footerBuf = fb.Bytes()
	}
	footer := &pb.Footer{}
	if err = proto.Unmarshal(footerBuf, footer); err != nil {
		return nil, errors.Wrapf(err, "unmarshal footer error")
	}

	log.Debugf("read file footer: %s", footer.String())

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

	log.Debugf("read file postscript: %s", ps.String())

	return ps, err
}

func checkOrcVersion(ps *pb.PostScript) error {
	// todo：
	return nil
}

func ensureOrcFooter(f *os.File, psLen int, buf []byte) error {
	magicLength := len(MAGIC)
	fullLength := magicLength + 1
	if psLen < fullLength || len(buf) < fullLength {
		return errors.Errorf("malformed ORC file %stream, invalid postscript length %d", f.Name(), psLen)
	}
	// now look for the magic string at the end of the postscript.
	//if (!Text.decode(array, offset, magicLength).equals(OrcFile.MAGIC)) {
	offset := len(buf) - fullLength
	// fixme: encoding
	if string(buf[offset:]) != MAGIC {
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

func assertx(condition bool) {
	if !condition {
		panic("assert error")
	}
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


