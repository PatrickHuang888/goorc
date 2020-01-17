package orc

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io"
	"os"
	"strings"

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
	Stripes() ([]StripeReader, error)
	Close() error
}

type ReaderOptions struct {
	CompressionKind pb.CompressionKind
	ChunkSize       uint64
	RowSize         int
	HasNulls        bool
}

func DefaultReaderOptions() *ReaderOptions {
	return &ReaderOptions{RowSize: DEFAULT_ROW_SIZE, ChunkSize: DEFAULT_CHUNK_SIZE}
}

type File interface {
	io.ReadSeeker
	io.Closer
	Size() (int64, error)
}

type reader struct {
	f File

	schemas []*TypeDescription
	opts    *ReaderOptions

	tail    *pb.FileTail
	stripes []StripeReader
}

type fileReader struct {
	f *os.File
}

func (fr fileReader) Size() (size int64, err error) {
	fi, err := fr.f.Stat()
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return fi.Size(), nil
}

func (fr fileReader) Read(p []byte) (n int, err error) {
	n, err = fr.f.Read(p)
	if err != nil {
		return n, errors.WithStack(err)
	}
	return
}

func (fr fileReader) Seek(offset int64, whence int) (int64, error) {
	return fr.f.Seek(offset, whence)
}

func (fr fileReader) Close() error {
	if err := fr.f.Close(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func NewFileReader(path string, opts *ReaderOptions) (r Reader, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrapf(err, "open file %stream error", path)
	}
	fr := &fileReader{f: f}
	return newReader(opts, fr)
}

func newReader(opts *ReaderOptions, f File) (r *reader, err error) {
	tail, err := extractFileTail(f)
	if err != nil {
		return nil, errors.Wrap(err, "read file tail error")
	}

	schemas := unmarshallSchema(tail.Footer.Types)
	opts.ChunkSize = tail.Postscript.GetCompressionBlockSize()
	opts.CompressionKind = tail.Postscript.GetCompression()
	r = &reader{f: f, tail: tail, opts: opts, schemas: schemas}
	return
}

func (r *reader) GetSchema() *TypeDescription {
	return r.schemas[0]
}

func (r *reader) Stripes() (ss []StripeReader, err error) {
	for i, stripeInfo := range r.tail.Footer.Stripes {
		offset := stripeInfo.GetOffset()
		indexLength := stripeInfo.GetIndexLength()
		dataLength := stripeInfo.GetDataLength()
		ps := r.tail.GetPostscript()

		// row index
		indexOffset := offset
		if _, err = r.f.Seek(int64(indexOffset), 0); err != nil {
			return nil, errors.WithStack(err)
		}
		indexBuf := make([]byte, indexLength)
		if _, err = io.ReadFull(r.f, indexBuf); err != nil {
			return nil, errors.WithStack(err)
		}
		if ps.GetCompression() != pb.CompressionKind_NONE {
			ib := &bytes.Buffer{}
			if err = decompressBuffer(ps.GetCompression(), ib, bytes.NewBuffer(indexBuf)); err != nil {
				return nil, err
			}
			indexBuf = ib.Bytes()
		}
		index := &pb.RowIndex{}
		if err = proto.Unmarshal(indexBuf, index); err != nil {
			return nil, errors.Wrapf(err, "unmarshal strip index error")
		}

		// footer
		footerOffset := int64(offset + indexLength + dataLength)
		if _, err := r.f.Seek(footerOffset, 0); err != nil {
			return nil, errors.WithStack(err)
		}
		footerBuf := make([]byte, stripeInfo.GetFooterLength())
		if _, err = io.ReadFull(r.f, footerBuf); err != nil {
			return nil, errors.WithStack(err)
		}
		if ps.GetCompression() != pb.CompressionKind_NONE {
			fb := &bytes.Buffer{}
			if err = decompressBuffer(r.tail.GetPostscript().GetCompression(), fb, bytes.NewBuffer(footerBuf)); err != nil {
				return nil, err
			}
			footerBuf = fb.Bytes()
		}
		footer := &pb.StripeFooter{}
		if err = proto.Unmarshal(footerBuf, footer); err != nil {
			return nil, errors.Wrapf(err, "unmarshal currentStripe footer error")
		}

		sr := &stripeReader{f: r.f, opts: r.opts, footer: footer, schemas: r.schemas, info: stripeInfo, idx: i}
		if err := sr.prepare(); err != nil {
			return ss, errors.WithStack(err)
		}
		log.Debugf("get stripeR %d : %stream", sr.idx, sr.info.String())
		ss = append(ss, sr)

	}

	return
}

func (r *reader) Close() error {
	return r.f.Close()
}

type StripeReader interface {
	Next(batch *ColumnVector) error
}

type stripeReader struct {
	f File

	schemas []*TypeDescription
	opts    *ReaderOptions

	info   *pb.StripeInformation
	footer *pb.StripeFooter

	columnReaders []columnReader
	idx           int
}

// stripe {index{},column{[present],data,[length]},footer}
func (s *stripeReader) prepare() error {

	//prepare column reader
	s.columnReaders = make([]columnReader, len(s.schemas))
	columns := make([]*cr, len(s.schemas))
	// id==i
	for _, schema := range s.schemas {
		c := &cr{}
		c.id = schema.Id
		c.schema = schema
		c.encoding = s.footer.GetColumns()[schema.Id]
		c.numberOfRows = s.info.GetNumberOfRows()
		c.opts = s.opts
		columns[schema.Id] = c
	}

	// streams has sequence
	indexStart := s.info.GetOffset()
	dataStart := indexStart + s.info.GetIndexLength()
	for _, streamInfo := range s.footer.GetStreams() {

		id := streamInfo.GetColumn()
		streamKind := streamInfo.GetKind()
		length := streamInfo.GetLength()
		buf := bytes.NewBuffer(make([]byte, s.opts.ChunkSize))
		buf.Reset()

		c := columns[id]

		if streamKind == pb.Stream_ROW_INDEX {
			// todo: init index streamReader
			indexStart += length
			continue
		}

		sr := &streamReader{start: dataStart, info: streamInfo, buf: buf, in: s.f, opts: s.opts}

		if streamKind == pb.Stream_PRESENT {
			decoder := &encoding.ByteRunLength{}
			c.present = &boolValuesReader{stream: sr, decoder: decoder}
			continue
		}

		switch s.schemas[id].Kind {
		case pb.Type_SHORT:
			fallthrough
		case pb.Type_INT:
			fallthrough
		case pb.Type_LONG:
			if c.encoding.GetKind() == pb.ColumnEncoding_DIRECT_V2 {
				if s.columnReaders[id] == nil {
					s.columnReaders[id] = &longV2Reader{cr: c}
				}
				cr := s.columnReaders[id].(*longV2Reader)

				if streamKind == pb.Stream_DATA {
					decoder := &encoding.IntRleV2{Signed: true}
					data := &longV2ValuesReader{stream: sr, decoder: decoder}
					cr.data = data
				}
				break
			}

			return errors.New("not impl")

		case pb.Type_FLOAT:
		// todo:

		case pb.Type_DOUBLE:
			if c.encoding.GetKind() != pb.ColumnEncoding_DIRECT {
				return errors.New("column encoding error")
			}

			if s.columnReaders[id] == nil {
				s.columnReaders[id] = &doubleReader{cr: c}
			}
			cr := s.columnReaders[id].(*doubleReader)

			if streamKind == pb.Stream_DATA {
				decoder := &encoding.Ieee754Double{}
				data := &ieeeFloatValuesReader{stream: sr, decoder: decoder}
				cr.data = data
			}

		case pb.Type_STRING:
			if c.encoding.GetKind() == pb.ColumnEncoding_DIRECT_V2 {
				if s.columnReaders[id] == nil {
					s.columnReaders[id] = &stringDirectV2Reader{cr: c}
				}
				cr := s.columnReaders[id].(*stringDirectV2Reader)

				if streamKind == pb.Stream_DATA {
					data := &bytesContentValuesReader{stream: sr}
					cr.data = data
				}
				if streamKind == pb.Stream_LENGTH {
					decoder := &encoding.IntRleV2{Signed: false}
					length := &longV2ValuesReader{stream: sr, decoder: decoder}
					cr.length = length
				}
				break
			}

			if c.encoding.GetKind() == pb.ColumnEncoding_DICTIONARY_V2 {
				if s.columnReaders[id] == nil {
					s.columnReaders[id] = &stringDictV2Reader{cr: c}
				}
				cr := s.columnReaders[id].(*stringDictV2Reader)

				if streamKind == pb.Stream_DATA {
					data := &longV2ValuesReader{stream: sr, decoder: &encoding.IntRleV2{Signed: false}}
					cr.data = data
				}
				if streamKind == pb.Stream_DICTIONARY_DATA {
					// todo: encoder
					dictData := &bytesContentValuesReader{stream: sr}
					cr.dictData = dictData
				}
				if streamKind == pb.Stream_LENGTH {
					decoder := &encoding.IntRleV2{Signed: false}
					dictLength := &longV2ValuesReader{stream: sr, decoder: decoder}
					cr.dictLength = dictLength
				}
				break
			}
			return errors.New("column encoding error")

		case pb.Type_BOOLEAN:
			if c.encoding.GetKind() != pb.ColumnEncoding_DIRECT {
				return errors.New("bool column encoding error")
			}

			if s.columnReaders[id] == nil {
				s.columnReaders[id] = &boolReader{cr: c}
			}
			cr := s.columnReaders[id].(*boolReader)

			if streamKind == pb.Stream_DATA {
				decoder := &encoding.ByteRunLength{}
				data := &boolValuesReader{stream: sr, decoder: decoder}
				cr.data = data
			}

		case pb.Type_BYTE: // tinyint
			if c.encoding.GetKind() != pb.ColumnEncoding_DIRECT {
				return errors.New("tinyint column encoding error")
			}

			if s.columnReaders[id] == nil {
				s.columnReaders[id] = &byteReader{cr: c}
			}
			cr := s.columnReaders[id].(*byteReader)

			if streamKind == pb.Stream_DATA {
				decoder := &encoding.ByteRunLength{}
				data := &byteValuesReader{stream: sr, decoder: decoder}
				cr.data = data
			} else {
				return errors.New("stream kind error")
			}

		case pb.Type_BINARY:
			if c.encoding.GetKind() == pb.ColumnEncoding_DIRECT {
				// todo:
				return errors.New("not impl")
				break
			}
			if c.encoding.GetKind() == pb.ColumnEncoding_DIRECT_V2 {
				if s.columnReaders[id] == nil {
					s.columnReaders[id] = &binaryV2Reader{cr: c}
				}
				cr := s.columnReaders[id].(*binaryV2Reader)

				if streamKind == pb.Stream_DATA {
					data := &bytesContentValuesReader{stream: sr}
					cr.data = data
				} else if streamKind == pb.Stream_LENGTH {
					decoder := &encoding.IntRleV2{Signed: false}
					length := &longV2ValuesReader{stream: sr, decoder: decoder}
					cr.length = length
				} else {
					return errors.New("stream kind error")
				}
				break
			}
			return errors.New("binary column encoding error")

		case pb.Type_DECIMAL:
			if c.encoding.GetKind() == pb.ColumnEncoding_DIRECT {
				// todo:
				return errors.New("not impl")
				break
			}
			if c.encoding.GetKind() == pb.ColumnEncoding_DIRECT_V2 {
				if s.columnReaders[id] == nil {
					s.columnReaders[id] = &decimal64DirectV2Reader{cr: c}
				}
				cr := s.columnReaders[id].(*decimal64DirectV2Reader)

				if streamKind == pb.Stream_DATA {
					decoder := &encoding.Base128VarInt{}
					data := &int64VarIntValuesReader{stream: sr, decoder: decoder}
					cr.data = data
				} else if streamKind == pb.Stream_SECONDARY {
					decoder := &encoding.IntRleV2{Signed: false}
					secondary := &longV2ValuesReader{stream: sr, decoder: decoder}
					cr.secondary = secondary
				} else {
					errors.New("stream kind error")
				}
				break
			}
			return errors.New("column encoding error")

		case pb.Type_DATE:
			if c.encoding.GetKind() == pb.ColumnEncoding_DIRECT {
				// todo:
				return errors.New("not impl")
				break
			}
			if c.encoding.GetKind() == pb.ColumnEncoding_DIRECT_V2 {
				if s.columnReaders[id] == nil {
					s.columnReaders[id] = &dateReader{cr: c}
				}
				cr := s.columnReaders[id].(*dateReader)

				if streamKind == pb.Stream_DATA {
					decoder := &encoding.IntRleV2{Signed: true}
					data := &longV2ValuesReader{stream: sr, decoder: decoder}
					cr.data = data
				} else {
					errors.New("stream kind error")
				}
				break
			}
			return errors.New("column encoding error")

		case pb.Type_TIMESTAMP:
			if c.encoding.GetKind() == pb.ColumnEncoding_DIRECT {
				// todo:
				return errors.New("not impl")
				break
			}
			if c.encoding.GetKind() == pb.ColumnEncoding_DIRECT_V2 {
				if s.columnReaders[id] == nil {
					s.columnReaders[id] = &timestampV2Reader{cr: c}
				}
				cr := s.columnReaders[id].(*timestampV2Reader)

				if streamKind == pb.Stream_DATA {
					decoder := &encoding.IntRleV2{Signed: true}
					data := &longV2ValuesReader{stream: sr, decoder: decoder}
					cr.data = data
				}
				if streamKind == pb.Stream_SECONDARY {
					decoder := &encoding.IntRleV2{Signed: false}
					secondary := &longV2ValuesReader{stream: sr, decoder: decoder}
					cr.secondary = secondary
				} else {
					return errors.New("stream kind error")
				}
				break
			}
			return errors.New("column encoding error")

		case pb.Type_STRUCT:
			if c.encoding.GetKind() == pb.ColumnEncoding_DIRECT_V2 {
				if s.columnReaders[id] == nil {
					s.columnReaders[id] = &structReader{cr: c}

					// connecting children below
				}
				break
			}
			return errors.New("encoding error")

		case pb.Type_LIST:
			if c.encoding.GetKind() == pb.ColumnEncoding_DIRECT {
				// todo:
				return errors.New("not impl")
				break
			}
			if c.encoding.GetKind() == pb.ColumnEncoding_DIRECT_V2 {
				if streamKind == pb.Stream_LENGTH {
					// todo:
					//decoder := &encoding.IntRleV2{}
					//length := &longStream{r: r, decoder: decoder}

				}
				break
			}
			return errors.New("encoding error")

		case pb.Type_MAP:
			if c.encoding.GetKind() == pb.ColumnEncoding_DIRECT {
				// todo:
				return errors.New("not impl")
				break
			}
			if c.encoding.GetKind() == pb.ColumnEncoding_DIRECT_V2 {
				if streamKind == pb.Stream_LENGTH {
					//decoder := &encoding.IntRleV2{}
					//length := &longStream{r: r, decoder: decoder}
					// todo:
				}
			}

		case pb.Type_UNION:
			if c.encoding.GetKind() != pb.ColumnEncoding_DIRECT {
				return errors.New("column encoding error")
			}

			// fixme: pb.Stream_DIRECT

		}

		dataStart += length
	}

	for _, schema := range s.schemas {
		if schema.Kind == pb.Type_STRUCT {
			crs := s.columnReaders[schema.Id].(*structReader).children
			for _, childSchema := range schema.Children {
				crs = append(crs, s.columnReaders[childSchema.Id])
			}
			s.columnReaders[schema.Id].(*structReader).children = crs
		}
	}

	// todo: streamreader needed checking

	/*for _, v := range crs {
		v.Print()
	}*/
	return nil
}

// entry of stripeR reader
// a stripeR is typically  ~200MB
func (s *stripeReader) Next(batch *ColumnVector) error {

	c := s.columnReaders[batch.Id]

	return c.next(batch)
}

type columnReader interface {
	next(batch *ColumnVector) error
}

type cr struct {
	id       uint32
	schema   *TypeDescription
	encoding *pb.ColumnEncoding
	//f            *os.File
	numberOfRows uint64
	opts         *ReaderOptions

	present *boolValuesReader

	cursor uint64
}

func (c *cr) String() string {
	sb := strings.Builder{}
	fmt.Fprintf(&sb, "id %d, ", c.id)
	fmt.Fprintf(&sb, "kind %stream, ", c.schema.Kind.String())
	fmt.Fprintf(&sb, "encoding %stream, ", c.encoding.String())
	fmt.Fprintf(&sb, "read cursor %d", c.cursor)
	return sb.String()
}

func (c *cr) nextPresents(batch *ColumnVector) (err error) {
	if c.present != nil {
		batch.Presents = batch.Presents[:0]

		for i := 0; !c.present.finished() && i < cap(batch.Presents); i++ {
			v, err := c.present.next()
			if err != nil {
				return err
			}
			batch.Presents = append(batch.Presents, v)
		}
	}
	return nil
}

type byteReader struct {
	*cr

	data *byteValuesReader
}

func (c *byteReader) next(batch *ColumnVector) error {
	if err := c.nextPresents(batch); err != nil {
		return err
	}

	vector := batch.Vector.([]byte)
	vector = vector[:0]

	i := 0
	for ; i < cap(vector) && !c.data.finished(); i++ {
		if len(batch.Presents) == 0 || (len(batch.Presents) != 0 && batch.Presents[i]) {
			v, err := c.data.next()
			if err != nil {
				return err
			}
			vector = append(vector, v)
		} else {
			vector = append(vector, 0)
		}
	}

	c.cursor += uint64(i)
	batch.Vector = vector
	return nil
}

type dateReader struct {
	*cr
	data *longV2ValuesReader
}

func (c *dateReader) next(batch *ColumnVector) error {
	if err := c.nextPresents(batch); err != nil {
		return err
	}

	vector := batch.Vector.([]Date)
	vector = vector[:0]

	i := 0
	for ; i < cap(vector) && !c.data.finished(); i++ {
		if len(batch.Presents) == 0 || (len(batch.Presents) != 0 && batch.Presents[i]) {
			v, err := c.data.nextInt()
			if err != nil {
				return err
			}
			// opti:
			batch.Vector = append(vector, fromDays(v))
		} else {
			batch.Vector = append(vector, Date{})
		}
	}

	c.cursor += uint64(i)
	batch.Vector = vector
	return nil
}

type timestampV2Reader struct {
	*cr

	data      *longV2ValuesReader
	secondary *longV2ValuesReader
}

func (c *timestampV2Reader) next(batch *ColumnVector) error {
	if err := c.nextPresents(batch); err != nil {
		return err
	}

	vector := batch.Vector.([]Timestamp)
	vector = vector[:0]

	i := 0
	for ; i < cap(vector) && !c.data.finished() && !c.secondary.finished(); i++ {
		if len(batch.Presents) == 0 || (len(batch.Presents) != 0 && batch.Presents[i]) {
			seconds, err := c.data.nextInt()
			if err != nil {
				return err
			}
			nanos, err := c.data.nextUInt()
			if err != nil {
				return err
			}
			vector = append(vector, Timestamp{seconds, uint32(nanos)})
		} else {
			vector = append(vector, Timestamp{})
		}
	}

	if (c.data.finished() && !c.secondary.finished()) || (c.secondary.finished() && !c.data.finished()) {
		return errors.New("read error")
	}

	c.cursor += uint64(i)
	batch.Vector = vector
	return nil
}

type boolReader struct {
	*cr
	data *boolValuesReader
}

func (c *boolReader) next(batch *ColumnVector) error {

	vector := batch.Vector.([]bool)
	vector = vector[:0]

	// ??
	// because bools extend to byte, may not know the real rows from read,
	// so using number of rows
	for i := 0; c.cursor < c.numberOfRows && i < cap(vector); i++ {
		if len(batch.Presents) != 0 {
			assertx(i <= len(batch.Presents))
		}

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
	return nil
}

type binaryV2Reader struct {
	*cr
	length *longV2ValuesReader
	data   *bytesContentValuesReader
}

func (c *binaryV2Reader) next(batch *ColumnVector) error {
	if err := c.nextPresents(batch); err != nil {
		return err
	}

	vector := batch.Vector.([][]byte)
	vector = vector[:0]

	i := 0
	for ; i < cap(vector) && !c.data.finished(); i++ {
		if len(batch.Presents) != 0 {
			assertx(i < len(batch.Presents))
		}

		if len(batch.Presents) == 0 || (len(batch.Presents) != 0 && batch.Presents[i]) {
			l, err := c.length.nextUInt()
			if err != nil {
				return err
			}
			v, err := c.data.next(l)
			if err != nil {
				return err
			}
			vector = append(vector, v)
		} else {
			vector = append(vector, []byte{})
		}
	}

	c.cursor = c.cursor + uint64(i)
	batch.Vector = vector
	return nil
}

type stringDirectV2Reader struct {
	*cr

	length *longV2ValuesReader
	data   *bytesContentValuesReader
}

func (c *stringDirectV2Reader) next(batch *ColumnVector) error {
	if err := c.nextPresents(batch); err != nil {
		return err
	}

	vector := batch.Vector.([]string)
	vector = vector[:0]

	i := 0
	for ; i < cap(vector) && !c.data.finished() && !c.length.finished(); i++ {
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
	}

	if (c.length.finished() && !c.data.finished()) || (c.data.finished() && !c.length.finished()) {
		return errors.New("read error")
	}

	c.cursor += uint64(i)
	batch.Vector = vector
	return nil
}

type stringDictV2Reader struct {
	*cr

	data       *longV2ValuesReader
	dictData   *bytesContentValuesReader
	dictLength *longV2ValuesReader
}

func (c *stringDictV2Reader) next(batch *ColumnVector) error {
	if err := c.nextPresents(batch); err != nil {
		return err
	}

	vector := batch.Vector.([]string)
	vector = vector[:0]

	ls, err := c.dictLength.getAllUInts()
	if err != nil {
		return err
	}

	dict, err := c.dictData.getAll(ls)
	if err != nil {
		return err
	}

	i := 0
	for ; i < cap(vector) && !c.data.finished(); i++ {
		if len(batch.Presents) == 0 || (len(batch.Presents) != 0 && batch.Presents[i]) {
			v, err := c.data.nextUInt()
			if err != nil {
				return err
			}
			if v >= uint64(len(dict)) {
				return errors.New("dict index error")
			}
			vector = append(vector, string(dict[v]))
		} else {
			vector = append(vector, "")
		}
	}

	c.cursor += uint64(i)
	batch.Vector = vector
	return nil
}

type longV2Reader struct {
	*cr

	data *longV2ValuesReader
}

func (c *longV2Reader) next(batch *ColumnVector) error {
	if err := c.nextPresents(batch); err != nil {
		return err
	}

	vector := batch.Vector.([]int64)
	vector = vector[:0]

	i := 0
	for ; !c.data.finished() && i < cap(vector); i++ {
		// rethink: i and present index check

		if len(batch.Presents) == 0 || (len(batch.Presents) != 0 && batch.Presents[i]) {
			v, err := c.data.nextInt()
			if err != nil {
				return err
			}
			vector = append(vector, v)
		} else {
			vector = append(vector, 0)
		}
	}

	c.cursor += uint64(i)
	batch.Vector = vector
	return nil
}

type decimal64DirectV2Reader struct {
	*cr

	data      *int64VarIntValuesReader
	secondary *longV2ValuesReader
}

func (c *decimal64DirectV2Reader) next(batch *ColumnVector) error {
	if err := c.nextPresents(batch); err != nil {
		return err
	}

	vector := batch.Vector.([]Decimal64)
	vector = vector[:0]

	i := 0
	for ; i < cap(vector) && !c.data.finished() && !c.secondary.finished(); i++ {
		if len(batch.Presents) == 0 || (len(batch.Presents) != 0 && batch.Presents[i]) {
			precision, err := c.data.next()
			if err != nil {
				return err
			}
			scala, err := c.secondary.nextUInt()
			if err != nil {
				return err
			}
			vector = append(vector, Decimal64{precision, uint16(scala)})
		} else {
			vector = append(vector, Decimal64{})
		}
	}

	if (c.data.finished() && !c.secondary.finished()) || (c.secondary.finished() && !c.data.finished()) {
		return errors.New("read error")
	}

	c.cursor += uint64(i)
	batch.Vector = vector
	return nil
}

type doubleReader struct {
	*cr

	data *ieeeFloatValuesReader
}

func (c *doubleReader) next(batch *ColumnVector) error {
	if err := c.nextPresents(batch); err != nil {
		return err
	}

	vector := batch.Vector.([]float64)
	vector = vector[:0]

	i := 0
	for ; i < cap(vector) && !c.data.finished(); i++ {
		if len(batch.Presents) == 0 || (len(batch.Presents) != 0 && batch.Presents[i]) {
			v, err := c.data.next()
			if err != nil {
				return err
			}
			vector = append(vector, v)
		} else {
			vector = append(vector, 0)
		}
	}

	c.cursor += uint64(i)
	batch.Vector= vector
	return nil
}

type structReader struct {
	*cr

	children []columnReader
}

func (c *structReader) next(batch *ColumnVector) error {
	if err := c.nextPresents(batch); err != nil {
		return err
	}

	vector := batch.Vector.([]*ColumnVector)

	for i, child := range c.children {
		// fixme: how to handle present ?
		// todo: cursor
		if err := child.next(vector[i]); err != nil {
			return err
		}
	}

	return nil
}

type byteValuesReader struct {
	stream *streamReader

	values   []byte
	consumed int

	decoder *encoding.ByteRunLength
}

func (r *byteValuesReader) next() (v byte, err error) {
	if r.consumed == len(r.values) {
		r.values = r.values[:0]
		r.consumed = 0

		if r.values, err = r.decoder.ReadValues(r.stream, r.values); err != nil {
			return 0, err
		}
	}

	v = r.values[r.consumed]
	r.consumed++
	return
}

func (r *byteValuesReader) finished() bool {
	return r.stream.finished() && (r.consumed == len(r.values))
}

// rethink: not using decoder, just read directly using streamReader
type bytesContentValuesReader struct {
	stream *streamReader

	decoder *encoding.BytesContent
}

func (r *bytesContentValuesReader) next(length uint64) (v []byte, err error) {
	// todo
	/*if s.r.buf.Len() < int(length) && !s.r.finished() {
		s.r.buf.Truncate(s.r.buf.Len())
		if err := s.r.readAChunk(); err != nil {
			return nil, err
		}
	}

	v = make([]byte, length)
	n, err := s.r.buf.Read(v)
	if err != nil {
		return nil, err
	}
	if n < int(length) {
		return nil, errors.New("no enough bytes")
	}*/
	return nil, errors.New("not impl")
}

func (r *bytesContentValuesReader) finished() bool {
	// todo
	//return stream.r.readFinished() && (stream.r.buf.Len() == 0)
	return false
}

// for read column using encoding like dict
func (r *bytesContentValuesReader) getAll(lengthAll []uint64) (vs [][]byte, err error) {
	// todo
	/*for !stream.r.readFinished() {
		if err = stream.r.readAChunk(); err != nil {
			return nil, err
		}
	}

	for _, l := range lengthAll {
		v := make([]byte, l)
		n, err := stream.r.buf.Read(v)
		if err != nil {
			return vs, err
		}
		if n < int(l) {
			return vs, errors.New("no enough bytes")
		}
		vs = append(vs, v)
	}*/

	return nil, errors.New("not impl")
}

type ieeeFloatValuesReader struct {
	stream       *streamReader
	decoder *encoding.Ieee754Double
}

func (r *ieeeFloatValuesReader) next() (v float64, err error) {
	return r.decoder.ReadValue(r.stream)
}

func (r *ieeeFloatValuesReader) finished() bool {
	return r.stream.finished()
}

type int64VarIntValuesReader struct {
	stream *streamReader

	values []int64
	pos    int

	decoder *encoding.Base128VarInt
}

func (r *int64VarIntValuesReader) next() (v int64, err error) {
	if r.pos >= len(r.values) {
		r.pos = 0
		r.values = r.values[:0]

		if err = r.decoder.ReadValues(r.stream, r.values); err != nil {
			return 0, err
		}
	}

	v = r.values[r.pos]
	r.pos++
	return
}

func (r *int64VarIntValuesReader) finished() bool {
	return r.stream.finished() && (r.pos == len(r.values))
}

type longV2ValuesReader struct {
	stream *streamReader

	values []uint64
	pos    int

	decoder *encoding.IntRleV2
}

func (r *longV2ValuesReader) nextInt() (v int64, err error) {
	x, err := r.nextUInt()
	if err != nil {
		return
	}
	v = encoding.UnZigzag(x)
	return
}

func (r *longV2ValuesReader) nextUInt() (v uint64, err error) {
	if r.pos >= len(r.values) {
		r.pos = 0
		r.values = r.values[:0]

		if r.values, err = r.decoder.ReadValues(r.stream, r.values); err != nil {
			return
		}

		log.Debugf("decoded %d long values from chunk", len(r.values))
	}

	v = r.values[r.pos]
	r.pos++
	return
}

// for small data like dict index, ignore stream.signed
func (r *longV2ValuesReader) getAllUInts() (vs []uint64, err error) {
	for !r.stream.finished() {
		if r.values, err = r.decoder.ReadValues(r.stream, r.values); err != nil {
			return
		}
	}
	return
}

func (r *longV2ValuesReader) finished() bool {
	return r.stream.finished() && (r.pos == len(r.values))
}

type boolValuesReader struct {
	stream *streamReader

	values  []byte
	pos     int
	bytePos int

	decoder *encoding.ByteRunLength
}

func (r *boolValuesReader) next() (v bool, err error) {
	if r.pos >= len(r.values) {
		r.pos = 0
		r.values = r.values[:0]

		if r.values, err = r.decoder.ReadValues(r.stream, r.values); err != nil {
			return
		}
	}

	v = r.values[r.pos]>>byte(7-r.bytePos) == 0x01
	r.bytePos++
	if r.bytePos == 8 {
		r.bytePos = 0
		r.pos++
	}
	return
}

func (r *boolValuesReader) finished() bool {
	// fixme:
	return r.stream.finished() && (r.pos == len(r.values))
}

/*type streamReader interface {
	io.Reader
	io.ByteReader
	finished() bool
}*/

type streamReader struct {
	info *pb.Stream

	start      uint64
	readLength uint64

	buf *bytes.Buffer

	opts *ReaderOptions

	in io.ReadSeeker
}

/*func (stream streamReader) String() string {
	return fmt.Sprintf("start %d, length %d, kind %stream, already read %d", stream.start, stream.length,
		stream.kind.String(), stream.readLength)
}*/

func (s *streamReader) ReadByte() (b byte, err error) {
	if s.buf.Len() >= 1 {
		b, err = s.buf.ReadByte()
		if err != nil {
			return b, errors.WithStack(err)
		}
		return
	}

	for s.buf.Len() < 1 && s.readLength < s.info.GetLength() {
		if err = s.readAChunk(); err != nil {
			return 0, err
		}
	}

	b, err = s.buf.ReadByte()
	if err != nil {
		return b, errors.WithStack(err)
	}
	return
}

func (s *streamReader) Read(p []byte) (n int, err error) {
	if s.buf.Len() >= len(p) {
		n, err = s.buf.Read(p)
		if err != nil {
			return n, errors.WithStack(err)
		}
		return
	}

	for s.buf.Len() < len(p) && s.readLength < s.info.GetLength() {
		if err = s.readAChunk(); err != nil {
			return 0, err
		}
	}

	n, err = s.buf.Read(p)
	if err != nil {
		return n, errors.WithStack(err)
	}
	return
}

// read a chunk to s.buf
func (s *streamReader) readAChunk() error {

	if _, err := s.in.Seek(int64(s.start+s.readLength), 0); err != nil {
		return errors.WithStack(err)
	}

	if s.opts.CompressionKind == pb.CompressionKind_NONE { // no header
		l := int64(DEFAULT_CHUNK_SIZE)
		if int64(s.info.GetLength()) < l {
			l = int64(s.info.GetLength())
		}
		log.Tracef("no compression copy %d from stream", l)
		_, err := io.CopyN(s.buf, s.in, l)
		if err != nil {
			return errors.WithStack(err)
		}
		s.readLength += uint64(l)
		return nil
	}

	head := make([]byte, 3)
	if _, err := io.ReadFull(s.in, head); err != nil {
		return errors.WithStack(err)
	}
	s.readLength += 3
	original := (head[0] & 0x01) == 1
	chunkLength := uint64(head[2])<<15 | uint64(head[1])<<7 | uint64(head[0])>>1

	if chunkLength > s.opts.ChunkSize {
		return errors.Errorf("chunk length %d larger than chunk size %d", chunkLength, s.opts.ChunkSize)
	}

	if original {
		log.Tracef("stream read original chunkLength %d", chunkLength)
		if _, err := io.CopyN(s.buf, s.in, int64(chunkLength)); err != nil {
			return errors.WithStack(err)
		}
		s.readLength += chunkLength
		return nil
	}

	log.Tracef("compressing %s read chunkLength %d", s.opts.CompressionKind, chunkLength)
	readBuf := bytes.NewBuffer(make([]byte, chunkLength))
	readBuf.Reset()
	if _, err := io.CopyN(readBuf, s.in, int64(chunkLength)); err != nil {
		return errors.WithStack(err)
	}

	if _, err := decompressChunkData(s.opts.CompressionKind, s.buf, readBuf); err != nil {
		return err
	}

	s.readLength += chunkLength

	return nil
}

/*func (r *streamReader) ReadAChunk(valueBuf *bytes.Buffer) (n int, err error) {
	if _, err := r.f.Seek(int64(r.start+r.readLength), 0); err != nil {
		return 0, errors.WithStack(err)
	}

	l, err := readAChunk()
	log.Debugf("read %d", n)
	if err != nil {
		return int(l), err
	}
	r.readLength += l
	n = int(l)

	return
}*/

/*// read whole streamR into memory
func (stream *streamReader) readWhole(opts *ReaderOptions, f *os.File) (err error) {

	for stream.readLength < stream.length {
		err = stream.readAChunk()
		if err != nil {
			return err
		}
	}
	return nil
}*/

func (s streamReader) finished() bool {
	return s.readLength >= s.info.GetLength() && s.buf.Len() == 0
}

func (r *reader) NumberOfRows() uint64 {
	return r.tail.Footer.GetNumberOfRows()
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

	/*fi, err := f.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "get file status error")
	}
	size := fi.Size()*/
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

	log.Debugf("Footer: %stream\n", footer.String())

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

	// Check compression codec.
	/*switch ps.GetCompression() {
	  default:
	  	return nil, errors.New("unknown compression")
	  }*/
	fmt.Printf("Postscript: %stream\n", ps.String())
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

// data should be compressed
func decompressChunkData(kind pb.CompressionKind, dst *bytes.Buffer, src *bytes.Buffer) (n int64, err error) {
	switch kind {
	case pb.CompressionKind_ZLIB:
		r := flate.NewReader(src)
		n, err = dst.ReadFrom(r)
		r.Close()
		if err != nil {
			return 0, errors.WithStack(err)
		}
		return n, nil
	default:
		return 0, errors.New("compression kind other than zlib not impl")
	}

	return
}

// buffer should be compressed, maybe contains several chunks
func decompressBuffer(kind pb.CompressionKind, dst *bytes.Buffer, src *bytes.Buffer) (err error) {
	assertx(kind != pb.CompressionKind_NONE)
	switch kind {
	case pb.CompressionKind_ZLIB:
		for src.Len() > 0 {
			header := make([]byte, 3)
			if _, err = src.Read(header); err != nil {
				return errors.WithStack(err)
			}
			original := header[0]&0x01 == 1
			chunkLength := int64(header[2])<<15 | int64(header[1])<<7 | int64(header[0])>>1
			if original {
				if _, err = io.CopyN(dst, src, chunkLength); err != nil {
					return errors.WithStack(err)
				}
			} else {
				buf := bytes.NewBuffer(make([]byte, chunkLength))
				buf.Reset()
				if _, err = io.CopyN(buf, src, chunkLength); err != nil {
					return errors.WithStack(err)
				}
				r := flate.NewReader(buf)
				if _, err = io.Copy(dst, r); err != nil {
					return errors.WithStack(err)
				}
				if err = r.Close(); err != nil {
					return errors.WithStack(err)
				}
			}
		}
	default:
		return errors.New("decompression other than zlib not impl")
	}
	return
}
