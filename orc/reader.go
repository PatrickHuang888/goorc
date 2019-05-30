package orc

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io"
	"os"

	"github.com/PatrickHuang888/goorc/pb/pb"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

const (
	MAGIC                = "ORC"
	MAGIC_LENGTH         = uint64(3)
	DIRECTORY_SIZE_GUESS = 16 * 1024
)

type Reader interface {
	// get root description
	GetSchema() (*TypeDescription, error)
	GetColumnSchema(columnId uint32) (*TypeDescription, error)
	NumberOfRows() uint64
	Stripes() (StripeReader, error)
}

type reader struct {
	f    *os.File
	tail *pb.FileTail
	tds  []*TypeDescription
}

func (r *reader) GetSchema() (*TypeDescription, error) {
	return r.GetColumnSchema(0)
}

func (r *reader) GetColumnSchema(columnId uint32) (*TypeDescription, error) {
	if columnId > uint32(len(r.tds)) || len(r.tds) == 0 {
		return nil, errors.Errorf("column %d schema does not exist", columnId)
	}
	return r.tds[columnId], nil
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

	rr = &stripeReader{f: r.f, stripeFooters: sfs, tds: r.tds, tail: r.tail, currentStripe: -1}
	return
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

type StripeReader interface {
	NextStripe() bool
	Err() error
	NextBatch(batch ColumnVector) bool
	Close()
}

type stripeReader struct {
	f             *os.File
	stripeFooters []*pb.StripeFooter
	tds           []*TypeDescription
	tail          *pb.FileTail
	currentStripe int
	crs           []*columnReader
	err           error
}

type columnReader struct {
	id              int
	td              *TypeDescription
	encoding        *pb.ColumnEncoding
	streams         map[pb.Stream_Kind]*pb.Stream
	f               *os.File
	si              *pb.StripeInformation
	compressionKind pb.CompressionKind
	chunkSize       uint64

	indexStart uint64 // index area start
	dataStart  uint64 // data area start
	dataRead   uint64 // data stream already read
	//present []bool
	length []uint64 // length data

	//pstDcr  Decoder // present decoder
	dataDcr Decoder // data decoder
	//lngDcr  Decoder // length decoder

	batchCount int

	cmpBuf   *bytes.Buffer // compressed buffer for a chunk
	decmpBuf *bytes.Buffer // decompressed buffer for a chunk
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
	fmt.Println()
}

func (sr *stripeReader) NextStripe() bool {
	sr.currentStripe++
	if sr.currentStripe >= len(sr.tail.Footer.Stripes) {
		return false
	}

	currentStripe := sr.currentStripe
	sf := sr.stripeFooters[currentStripe]
	fmt.Printf("Stripe number %d\n", sr.currentStripe)

	crs := make([]*columnReader, len(sr.tds))

	chunkSize := sr.tail.Postscript.GetCompressionBlockSize()
	for i := 0; i < len(crs); i++ {
		cmpBuf := bytes.NewBuffer(make([]byte, chunkSize))
		decmpBuf := bytes.NewBuffer(make([]byte, chunkSize))
		crs[i] = &columnReader{id: i, td: sr.tds[i], encoding: sf.GetColumns()[i],
			si: sr.tail.Footer.Stripes[currentStripe], compressionKind: sr.tail.Postscript.GetCompression(),
			streams: make(map[pb.Stream_Kind]*pb.Stream), f: sr.f, batchCount: -1,
			chunkSize: chunkSize,
			cmpBuf:    cmpBuf, decmpBuf: decmpBuf}
	}

	indexOffsets := make(map[uint32]uint64)
	dataOffsets := make(map[uint32]uint64)
	for _, stream := range sf.GetStreams() {
		c := stream.GetColumn()
		crs[c].streams[stream.GetKind()] = stream
		// fixme: except row_index others are all data stream?
		if stream.GetKind() == pb.Stream_ROW_INDEX {
			indexOffsets[c] += stream.GetLength()
		} else {
			dataOffsets[c] += stream.GetLength()
		}
	}
	si := sr.tail.Footer.Stripes[sr.currentStripe]
	crs[0].indexStart = si.GetOffset()
	crs[0].dataStart = si.GetOffset() + si.GetIndexLength()
	for i := 1; i < len(crs); i++ {
		crs[i].indexStart = crs[i-1].indexStart + indexOffsets[uint32(i-1)]
		crs[i].dataStart = crs[i-1].dataStart + dataOffsets[uint32(i-1)]
	}

	sr.crs = crs
	for _, v := range crs {
		v.Print()
	}

	return true
}

func (sr *stripeReader) NextBatch(batch ColumnVector) bool {
	// todo: check batch column id
	cr := sr.crs[batch.ColumnId()]
	cr.batchCount++
	fmt.Printf("read batch %d\n", cr.batchCount)

	/*indexStream := cr.streams[pb.Stream_ROW_INDEX]
	fmt.Println("==========")
	fmt.Println(indexStream.String())

	if _, err := cr.f.Seek(int64(cr.indexStart), 0); err != nil {
		fmt.Printf("%+v", err)
		return false
	}

	head := make([]byte, 3)
	if _, err := io.ReadFull(cr.f, head); err != nil {
		fmt.Printf("%+v", errors.WithStack(err))
		return false
	}

	original := (head[0] & 0x01) == 1
	chunkLength := int(head[2])<<15 | int(head[1])<<7 | int(head[0])>>1

	indexBuf := make([]byte, chunkLength)
	if _, err := io.ReadFull(cr.f, indexBuf); err != nil {
		fmt.Printf("%+v", err)
	}

	if original {
		fmt.Println("orgin+++++")
		ri := &pb.RowIndex{}
		if err := proto.Unmarshal(indexBuf, ri); err != nil {
			fmt.Printf("%+v", errors.WithStack(err))
			return false
		}
		fmt.Println(ri.String())
	} else {

		decompressed := make([]byte, cr.chunkBufSize)
		r := flate.NewReader(bytes.NewReader(indexBuf))
		n, err := r.Read(decompressed)
		r.Close()
		if err != nil && err != io.EOF {
			fmt.Printf("%+v", errors.WithStack(err))
			return false
		}
		ri := &pb.RowIndex{}
		if err := proto.Unmarshal(decompressed[:n], ri); err != nil {
			fmt.Printf("%+v", errors.WithStack(err))
			return false
		}
		fmt.Println(ri.String())
	}*/

	enc := cr.encoding.GetKind()
	switch cr.td.Kind {
	case pb.Type_INT:
		switch enc {
		case pb.ColumnEncoding_DIRECT_V2: // Signed Integer RLE v2
			v, ok := batch.(*LongColumnVector)
			if !ok {
				sr.err = errors.New("batch is not LongColumnVector")
				return false
			}

			// reset vector
			v.rows = 0

			result, err := cr.fillIntVector(v)
			if err != nil {
				sr.err = err
			}
			return result

		default:
			sr.err = errors.New("encoding other than direct_v2 for int not impl")
			return false
		}

	case pb.Type_STRING:
		switch enc {
		case pb.ColumnEncoding_DIRECT_V2:
			v, ok := batch.(*BytesColumnVector)
			if !ok {
				sr.err = errors.New("batch is not BytesColumnVector")
			}

			//reset vector
			v.rows = 0

			result, err := cr.fillBytesDirectV2Vector(v)
			if err != nil {
				sr.err = err
			}
			return result
		default:
			sr.err = errors.New("string encoding other than direct_v2 not impl")
		}

	case pb.Type_STRUCT:
		v, ok := batch.(*StructColumnVector)
		if !ok {
			sr.err = errors.New("batch is not StructColumnVector")
		}
		// todo: present
		var ret bool
		for _, vf := range v.fields {
			if sr.NextBatch(vf) {
				ret = true
			}
		}
		return ret

	default:
		sr.err = errors.Errorf("type %s not impl", cr.td.Kind.String())
		return false
	}
	return false
}

func (cr *columnReader) fillBytesDirectV2Vector(v *BytesColumnVector) (next bool, err error) {
	if cr.dataDcr == nil {
		cr.dataDcr = &stringContentDecoder{}
	}
	stringDecoder := cr.dataDcr.(*stringContentDecoder)

	// has leftover
	if stringDecoder.consumeIndex != 0 {
		for ; stringDecoder.consumeIndex < stringDecoder.num; stringDecoder.consumeIndex++ {
			if v.rows < len(v.vector) {
				v.vector[v.rows] = stringDecoder.content[stringDecoder.consumeIndex]
			} else {
				if v.rows < cap(v.vector) {
					// refactor:
					v.vector = append(v.vector, stringDecoder.content[stringDecoder.consumeIndex])
				} else {
					// still not finished
					return true, nil
				}
			}
			v.rows++
		}
		//leftover finished
		stringDecoder.reset()
	}

	//decoding present stream
	presentStart := cr.dataStart
	var presentLength uint64
	if _, present := cr.streams[pb.Stream_PRESENT]; present {
		return false, errors.New("string present not impl")
	}

	dataStream := cr.streams[pb.Stream_DATA]
	dataStart := presentStart + presentLength
	dataLength := dataStream.GetLength()

	// decoding length stream
	// fixme: assuming read length stream totally in memory
	if cr.length == nil {
		lengthDecoder := &intRleV2{signed: false}
		lengthStream := cr.streams[pb.Stream_LENGTH]
		lengthStart := dataStart + dataLength
		lengthLength := lengthStream.GetLength()
		var lengthRead uint64
		for lengthRead < lengthLength {
			if _, err = cr.f.Seek(int64(lengthStart+lengthRead), 0); err != nil {
				return false, errors.WithStack(err)
			}
			//read 1 thunk 1 time
			var r int
			head := make([]byte, 3)
			if _, err = io.ReadFull(cr.f, head); err != nil {
				return false, errors.WithStack(err)
			}
			r += 3
			original := (head[0] & 0x01) == 1
			chunkLength := int(head[2])<<15 | int(head[1])<<7 | int(head[0])>>1
			if uint64(chunkLength) > cr.chunkSize {
				return false, errors.New("chunk length large than compression buffer size")
			}
			cr.cmpBuf.Reset()
			if _, err = io.CopyN(cr.cmpBuf, cr.f, int64(chunkLength)); err != nil {
				return false, errors.WithStack(err)
			}
			r += chunkLength
			lengthRead += uint64(r)
			// decompress
			cr.decmpBuf.Reset()
			_, err := decompress(cr.compressionKind, original, cr.cmpBuf, cr.decmpBuf)
			if err != nil {
				return false, errors.WithStack(err)
			}
			// decode
			if err := lengthDecoder.readValues(cr.decmpBuf); err != nil {
				return false, errors.WithStack(err)
			}
			// set decoded length data
			// refactor: use ref or append ?
			cr.length = append(cr.length, lengthDecoder.uliterals[:lengthDecoder.numLiterals]...)
			stringDecoder.length = cr.length
		}
	}

	// decoding data stream
	for v.rows < cap(v.vector) {
		if cr.dataRead < dataLength {
			// refactor: seek every time?
			if _, err = cr.f.Seek(int64(cr.dataStart+cr.dataRead), 0); err != nil {
				return false, errors.WithStack(err)
			}
			//read 1 thunk 1 time
			var r int
			head := make([]byte, 3)
			if _, err = io.ReadFull(cr.f, head); err != nil {
				return false, errors.WithStack(err)
			}
			r += 3
			original := (head[0] & 0x01) == 1
			chunkLength := int(head[2])<<15 | int(head[1])<<7 | int(head[0])>>1
			if uint64(chunkLength) > cr.chunkSize {
				return false, errors.New("chunk length large than compression buffer size")
			}
			cr.cmpBuf.Reset()
			if _, err = io.CopyN(cr.cmpBuf, cr.f, int64(chunkLength)); err != nil {
				return false, errors.WithStack(err)
			}
			r += chunkLength
			cr.dataRead += uint64(r)
			// decompress
			cr.decmpBuf.Reset()
			_, err := decompress(cr.compressionKind, original, cr.cmpBuf, cr.decmpBuf)
			if err != nil {
				return false, errors.WithStack(err)
			}
			// decode
			if err := stringDecoder.readValues(cr.decmpBuf); err != nil {
				return false, errors.WithStack(err)
			}

			for ; stringDecoder.consumeIndex < stringDecoder.num; stringDecoder.consumeIndex++ {
				if v.rows < len(v.vector) {
					// slice reference
					v.vector[v.rows] = stringDecoder.content[stringDecoder.consumeIndex]
				} else {
					if v.rows < cap(v.vector) {
						// refactor: allocate
						v.vector = append(v.vector, stringDecoder.content[stringDecoder.consumeIndex])
					} else {
						//full
						return true, nil
					}
				}
				v.rows++
			}
			stringDecoder.reset()
		} else {
			break
		}
	}

	return v.rows != 0, nil
}

func (cr *columnReader) fillIntVector(v *LongColumnVector) (next bool, err error) {
	if cr.dataDcr == nil {
		// refactor: init literals size
		cr.dataDcr = &intRleV2{signed: true}
	}
	rle := cr.dataDcr.(*intRleV2)

	//has decoded leftover
	if rle.consumeIndex != 0 {
		for ; rle.consumeIndex < int(rle.numLiterals); rle.consumeIndex++ {
			if v.rows < len(v.vector) {
				v.vector[v.rows] = rle.literals[rle.consumeIndex]
			} else {
				if v.rows < cap(v.vector) {
					// refactor:
					v.vector = append(v.vector, rle.literals[rle.consumeIndex])
				} else {
					// still not finished
					return true, nil
				}
			}
			v.rows++
		}
		//leftover finished
		rle.reset()
	}

	presentStart := cr.dataStart
	var presentLength uint64
	if _, present := cr.streams[pb.Stream_PRESENT]; present {
		return false, errors.New("string present not impl")
	}

	dataStream := cr.streams[pb.Stream_DATA]
	dataStart := presentStart + presentLength
	dataLength := dataStream.GetLength()

	for cr.dataRead < dataLength {
		// refactor: seek every time?
		if _, err = cr.f.Seek(int64(dataStart+cr.dataRead), 0); err != nil {
			return false, errors.WithStack(err)
		}

		//read 1 thunk every time
		var r int
		head := make([]byte, 3)
		if _, err = io.ReadFull(cr.f, head); err != nil {
			return false, errors.WithStack(err)
		}
		r += 3
		original := (head[0] & 0x01) == 1
		chunkLength := int(head[2])<<15 | int(head[1])<<7 | int(head[0])>>1
		if uint64(chunkLength) > cr.chunkSize {
			return false, errors.New("chunk length large than compression buffer size")
		}
		cr.cmpBuf.Reset()
		if _, err = io.CopyN(cr.cmpBuf, cr.f, int64(chunkLength)); err != nil {
			return false, errors.WithStack(err)
		}
		r += chunkLength
		cr.dataRead += uint64(r)
		// decompress
		cr.decmpBuf.Reset()
		_, err := decompress(cr.compressionKind, original, cr.cmpBuf, cr.decmpBuf)
		if err != nil {
			return false, errors.WithStack(err)
		}
		// decode
		if err := rle.readValues(cr.decmpBuf); err != nil {
			return false, errors.WithStack(err)
		}

		for ; rle.consumeIndex < int(rle.numLiterals); rle.consumeIndex++ {
			if v.rows < len(v.vector) {
				v.vector[v.rows] = rle.literals[rle.consumeIndex]
			} else {
				if v.rows < cap(v.vector) {
					// refactor:
					v.vector = append(v.vector, rle.literals[rle.consumeIndex])
				} else {
					//full
					return true, nil
				}
			}
			v.rows++
		}
		//all consumed
		rle.reset()
	}

	return v.rows != 0, nil
}

func decompress(compress pb.CompressionKind, original bool, cmpBuf *bytes.Buffer,
	decmpBuf *bytes.Buffer) (int64, error) {
	if original {
		// todo:
		return 0, errors.New("chunk original not implemented!")
	} else {
		switch compress {
		case pb.CompressionKind_ZLIB:
			r := flate.NewReader(cmpBuf)
			//n, err := r.Read(decompBuf)
			n, err := decmpBuf.ReadFrom(r)
			r.Close()
			if err != nil && err != io.EOF {
				return 0, errors.Wrapf(err, "decompress chunk data error when read footer")
			}
			return n, nil
		default:
			return 0, errors.New("compression kind other than zlib not impl")
		}
	}
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

	// read postScript
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

	// read file footer
	footerStart := psOffset - footerSize
	cmpBufSize := ps.GetCompressionBlockSize()
	footerBuf := bytes.NewBuffer(make([]byte, cmpBufSize))
	footerBuf.Reset()
	cmpFooterBuf := bytes.NewBuffer(buf[footerStart : footerStart+footerSize])

	if err := decompressedTo(footerBuf, cmpFooterBuf, ps.GetCompression()); err != nil {
		return nil, errors.WithStack(err)
	}

	footer := &pb.Footer{}
	if err = proto.Unmarshal(footerBuf.Bytes(), footer); err != nil {
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

// decompress buffer src into dst
func decompressedTo(dst *bytes.Buffer, src *bytes.Buffer, kind pb.CompressionKind) (err error) {
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
				if err= r.Close(); err!=nil {
					return errors.WithStack(err)
				}
			}
		}
	default:
		return errors.New("decompression other than zlib not impl")
	}
	return
}
