package orc

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io"
	"os"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/common"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/pb/pb"
)

const (
	MAGIC_LENGTH         = uint64(3)
	DIRECTORY_SIZE_GUESS = 16 * 1024
)

type Reader interface {
	GetSchema() *api.TypeDescription

	NumberOfRows() uint64

	Close() error

	Next(batch *api.ColumnVector) error

	Seek(rowNumber uint64) error

	GetStatistics() []*pb.ColumnStatistics
}

type reader struct {
	opts    *config.ReaderOptions
	schemas []*api.TypeDescription

	stripes []*stripeReader

	stripeIndex int

	f orcio.File

	numberOfRows uint64
	cursor       uint64

	stats []*pb.ColumnStatistics
}

func NewFileReader(path string, opts config.ReaderOptions) (Reader, error) {
	f, err := orcio.OpenFileForRead(path)
	if err != nil {
		return nil, err
	}
	return newReader(&opts, f)
}

func newReader(opts *config.ReaderOptions, f orcio.File) (r *reader, err error) {
	var tail *pb.FileTail
	if tail, err = extractFileTail(f); err != nil {
		return nil, errors.Wrap(err, "read file tail error")
	}

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
	}
	opts.ChunkSize = tail.Postscript.GetCompressionBlockSize()

	r = &reader{f: f, opts: opts, schemas: schemas, numberOfRows: rows, stats: tail.Footer.GetStatistics()}
	r.initStripes(f, tail.Footer.GetStripes())
	return
}

func (r *reader) GetSchema() *api.TypeDescription {
	return r.schemas[0]
}

func (r *reader) NumberOfRows() uint64 {
	return r.numberOfRows
}

func (r *reader) initStripes(f orcio.File, infos []*pb.StripeInformation) error {
	for i, stripeInfo := range infos {
		if stripeInfo.GetNumberOfRows() == 0 {
			return errors.Errorf("stripe number of rows 0 err, %s", stripeInfo.String())
		}
		sr, err := newStripeReader(f, r.schemas, r.opts, i, stripeInfo)
		if err != nil {
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
	return r.stripes[r.stripeIndex].Close()
}

func (r *reader) Next(batch *api.ColumnVector) error {
	batch.Clear()

	if r.cursor >= r.numberOfRows-1 {
		return nil
	}

	for r.stripeIndex < len(r.stripes) {
		end, err := r.stripes[r.stripeIndex].next(batch)
		if err != nil {
			return err
		}
		if end {
			r.stripeIndex++
			if len(batch.Vector) >= cap(batch.Vector) {
				break
			}
		} else {
			break
		}
	}
	if r.stripeIndex == len(r.stripes) {
		r.stripeIndex--
	}
	r.cursor += uint64(len(batch.Vector))
	return nil
}

func (r *reader) Seek(rowNumber uint64) error {
	if rowNumber > r.numberOfRows {
		return errors.New("row number larger than number of rows")
	}
	var rows uint64
	for i := 0; i < len(r.stripes); i++ {
		if rows+r.stripes[i].numberOfRows > rowNumber {
			return r.stripes[i].Seek(rowNumber - rows)
		}
		rows += r.stripes[i].numberOfRows
	}
	r.cursor = rowNumber
	return errors.New("no row found")
}

/*
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


*/

func unmarshallSchema(types []*pb.Type) (schemas []*api.TypeDescription) {
	schemas = make([]*api.TypeDescription, len(types))
	for i, t := range types {
		node := &api.TypeDescription{Kind: t.GetKind(), Id: uint32(i)}
		schemas[i] = node
	}
	for i, t := range types {
		schemas[i].Children = make([]*api.TypeDescription, len(t.Subtypes))
		schemas[i].ChildrenNames = make([]string, len(t.Subtypes))
		for j, v := range t.Subtypes {
			schemas[i].ChildrenNames[j] = t.FieldNames[j]
			schemas[i].Children[j] = schemas[v]
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
		if err := common.DecompressBuffer(ps.GetCompression(), fb, bytes.NewBuffer(footerBuf)); err != nil {
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
