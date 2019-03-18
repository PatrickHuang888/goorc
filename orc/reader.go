package orc

import (
	"bytes"
	"compress/flate"
	"fmt"
	"github.com/PatrickHuang888/goorc/hive"
	"github.com/PatrickHuang888/goorc/pb/pb"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"io"
	"os"
)

const (
	MAGIC                = "ORC"
	DIRECTORY_SIZE_GUESS = 16 * 1024
)

type Reader interface {
	GetSchema() *TypeDescription
	NumberOfRows() uint64
	Stripes() (StripeReader, error)
}

type reader struct {
	f    *os.File
	tail *pb.FileTail
	tds  []*TypeDescription
}

func (r *reader) GetSchema() *TypeDescription {
	return r.tds[0]
}

func (r *reader) Stripes() (rr StripeReader, err error) {
	var sfs []*pb.StripeFooter

	//fixme: strips are large typically  ~200MB
	for _, si := range r.tail.Footer.Stripes {
		offSet := int64(si.GetOffset())
		indexLength := int64(si.GetIndexLength())
		dataLength := int64(si.GetDataLength())
		bufferSize := int(r.tail.Postscript.GetCompressionBlockSize())

		// row index
		indexOffset := offSet
		if _, err = r.f.Seek(indexOffset, 0); err != nil {
			return nil, errors.WithStack(err)
		}
		indexBuf := make([]byte, indexLength)
		if _, err = io.ReadFull(r.f, indexBuf); err != nil {
			return nil, errors.WithStack(err)
		}
		decompressed, err := ReadChunks(indexBuf, r.tail.GetPostscript().GetCompression(), bufferSize)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		index := &pb.RowIndex{}
		if err = proto.Unmarshal(decompressed, index); err != nil {
			return nil, errors.Wrapf(err, "unmarshal strip index error")
		}
		//fmt.Printf("Stripe index: %s\n", index.String())
		/*for _, entry := range index.Entry {
			fmt.Printf("strip index entry: %s\n", entry.String())
		}*/

		// footer
		footerOffset := int64(offSet + indexLength + dataLength)
		if _, err := r.f.Seek(footerOffset, 0); err != nil {
			return nil, errors.WithStack(err)
		}
		stripeFooterBuf := make([]byte, si.GetFooterLength())
		if _, err = io.ReadFull(r.f, stripeFooterBuf); err != nil {
			return nil, errors.WithStack(err)
		}
		decompressed, err = ReadChunks(stripeFooterBuf, r.tail.GetPostscript().GetCompression(), bufferSize)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		stripeFooter := &pb.StripeFooter{}
		if err = proto.Unmarshal(decompressed, stripeFooter); err != nil {
			return nil, errors.Wrapf(err, "unmarshal stripe footer error")
		}
		//fmt.Printf("Stripe %d footer: %s\n", i, stripeFooter.String())

		sfs = append(sfs, stripeFooter)
	}

	rr = &stripeReader{f: r.f, stripeFooters: sfs, stripeInfos: r.tail.Footer.Stripes, tds: r.tds}
	return
}

func ReadChunks(chunksBuf []byte, compressKind pb.CompressionKind, chunkBufferSize int) (decompressed []byte, err error) {
	for offset := 0; offset < len(chunksBuf); {
		// header
		original := (chunksBuf[offset] & 0x01) == 1
		chunkLength := int((chunksBuf[offset+2] << 15) | (chunksBuf[offset+1] << 7) |
			(chunksBuf[offset] >> 1))
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

type StripeReader interface {
	NextStripe() bool
	Err() error
	NextBatch(batch *hive.VectorizedRowBatch) bool
	Close()
}

type stripeReader struct {
	f                 *os.File
	stripeFooters     []*pb.StripeFooter
	stripeInfos       []*pb.StripeInformation
	footer            *pb.Footer
	tds               []*TypeDescription
	currentStripe     int
	currentCrs        []*columnReader
	currentDataOffset uint64
	currentDataLength uint64
	err               error
}

type columnReader struct {
	id       int
	td       *TypeDescription
	encoding *pb.ColumnEncoding
	streams  []*pb.Stream
}

func (cr *columnReader) Print() {
	fmt.Printf("   Column Reader: %d\n", cr.id)
	fmt.Printf("td: %s\n", cr.td.Kind.String())
	//cr.td.Print()
	fmt.Printf("encoding: %s\n", cr.encoding.String())
	fmt.Printf("streams: \n")
	for _, s := range cr.streams {
		fmt.Printf("%s ", s.String())
	}
	fmt.Println()
}

func (sr *stripeReader) NextStripe() bool {
	currentStripe := sr.currentStripe
	sf := sr.stripeFooters[currentStripe]
	fmt.Printf("Stripe number %d\n", sr.currentStripe)

	crs := make([]*columnReader, len(sr.tds))

	for i := 0; i < len(crs); i++ {
		crs[i] = &columnReader{id: i, td: sr.tds[i], encoding: sf.GetColumns()[i]}
	}
	for _, stream := range sf.GetStreams() {
		c := stream.GetColumn()
		crs[c].streams = append(crs[c].streams, stream)
	}

	si := sr.stripeInfos[currentStripe]
	sr.currentDataOffset = si.GetOffset() + si.GetIndexLength()
	sr.currentDataLength = si.GetDataLength()

	sr.currentCrs = crs
	for _, v := range crs {
		v.Print()
	}

	sr.currentStripe++
	if sr.currentStripe <= len(sr.stripeInfos) {
		return true
	} else {
		return false
	}
}

func (sr *stripeReader) NextBatch(batch *hive.VectorizedRowBatch) bool {
	return false
}

func (sr *stripeReader) Err() error {
	return sr.err
}

func (sr *stripeReader) Close() {
	sr.f.Close()
}

func (r *reader) NumberOfRows() uint64 {
	return r.tail.Footer.GetNumberOfRows()
}

type ReaderOptions struct {
}

func CreateReader(path string) (r Reader, err error) {

	/*tail := opts.OrcTail
	if tail == nil {
		tail, err: = extractFileTail(path, )
	} else {
		// checkOrcVersion(path, tail.PostScript)
	}
	ri.tail = tail*/

	f, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "open file error")
	}

	tail, err := extractFileTail(f)
	if err != nil {
		return nil, errors.Wrap(err, "extract tail error")
	}

	tds := unmarshallSchema2(tail.Footer.Types)

	r = &reader{f: f, tail: tail, tds: tds}
	return
}

func unmarshallSchema2(types []*pb.Type) (tds []*TypeDescription) {
	tds = make([]*TypeDescription, len(types))
	for i, t := range types {
		node := &TypeDescription{Kind: t.GetKind(), Id: uint32(i)}
		tds[i] = node
	}
	for i, t := range types {
		tds[i].Children = make([]*TypeDescription, len(t.Subtypes))
		tds[i].ChildrenNames = make([]string, len(t.Subtypes))
		for j, v := range t.Subtypes {
			tds[i].ChildrenNames[j] = t.FieldNames[j]
			tds[i].Children[j] = tds[v]
		}
	}
	return
}

func unmarshallSchema(types []*pb.Type) (schema *TypeDescription) {
	tds := make([]*TypeDescription, len(types))
	for i, t := range types {
		node := &TypeDescription{Kind: t.GetKind(), Id: uint32(i)}
		tds[i] = node
	}
	if len(tds) > 0 {
		schema = tds[0]
	}
	for i, t := range types {
		tds[i].Children = make([]*TypeDescription, len(t.Subtypes))
		tds[i].ChildrenNames = make([]string, len(t.Subtypes))
		for j, v := range t.Subtypes {
			tds[i].ChildrenNames[j] = t.FieldNames[j]
			tds[i].Children[j] = tds[v]
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

func extractFileTail(f *os.File) (tail *pb.FileTail, err error) {

	fi, err := f.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "get file status error")
	}
	size := fi.Size()
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

	// read the PostScript
	psLen := int64(buf[readSize-1])
	psOffset := readSize - 1 - psLen
	ps, err := extractPostScript(buf[psOffset : psOffset+psLen])
	if err != nil {
		return nil, errors.Wrapf(err, "extract postscript error %s", f.Name())
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
	footerStart := psOffset - footerSize
	bufferSize := int64(ps.GetCompressionBlockSize())
	decompressed := make([]byte, bufferSize, bufferSize)
	decompressedPos := 0
	compress := ps.GetCompression()
	//data := buf[footerStart : footerStart+footerSize]
	offset := int64(0)
	footerBuf := buf[footerStart : footerStart+footerSize]
	for r := int64(0); r < footerSize; {
		// header
		original := (footerBuf[offset] & 0x01) == 1
		chunkLength := int64((footerBuf[offset+2] << 15) | (footerBuf[offset+1] << 7) | (footerBuf[offset] >> 1))
		//fixme:
		if chunkLength > bufferSize {
			return nil, errors.New("chunk length larger than compression block size")
		}
		offset += 3
		r = offset + chunkLength

		if original {
			// todo:
			return nil, errors.New("chunk original not implemented!")
		} else {
			switch compress {
			case pb.CompressionKind_ZLIB:
				b := bytes.NewBuffer(footerBuf[offset : offset+chunkLength])
				r := flate.NewReader(b)
				decompressedPos, err = r.Read(decompressed)
				r.Close()
				if err != nil && err != io.EOF {
					return nil, errors.Wrapf(err, "decompress chunk data error when read footer")
				}
				if decompressedPos == 0 {
					return nil, errors.New("decompress 0 footer")
				}
			}

		}
		offset += chunkLength
	}

	footer := &pb.Footer{}
	if err = proto.Unmarshal(decompressed[:decompressedPos], footer); err != nil {
		return nil, errors.Wrapf(err, "unmarshal footer error")
	}
	fmt.Printf("Footer: %s\n", footer.String())

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
	fmt.Printf("Postscript: %s\n", ps.String())
	return ps, err
}

func checkOrcVersion(ps *pb.PostScript) error {
	// todo：
	return nil
}

func ensureOrcFooter(f *os.File, psLen int, buf []byte) error {
	magicLength := len(MAGIC)
	fullLength := magicLength + 1;
	if psLen < fullLength || len(buf) < fullLength {
		return errors.Errorf("malformed ORC file %s, invalid postscript length %d", f.Name(), psLen)
	}
	// now look for the magic string at the end of the postscript.
	//if (!Text.decode(array, offset, magicLength).equals(OrcFile.MAGIC)) {
	offset := len(buf) - fullLength
	// fixme: encoding
	if string(buf[offset:]) != MAGIC {
		// If it isn't there, this may be the 0.11.0 version of ORC.
		// Read the first 3 bytes of the file to check for the header
		// todo:

		return errors.Errorf("malformed ORC file %s, invalid postscript", f.Name())
	}
	return nil
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
