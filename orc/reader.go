package orc

import (
	"fmt"
	"github.com/PatrickHuang888/goorc/hive"
	"github.com/PatrickHuang888/goorc/pb/pb"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"io"
	"os"
	"time"
)

const (
	MAGIC                = "ORC"
	DIRECTORY_SIZE_GUESS = 16 * 1024
)

type Reader interface {
	Rows() RecordReader
}

type RecordReader interface {
	NextBatch(batch *hive.VectorizedRowBatch) bool
}

type reader struct {
	f *os.File
	//tail *OrcTail
	ps *pb.PostScript
}

func (r *reader) Rows() RecordReader {
	return nil
}

type ReaderOptions struct {
}

type OrcTail struct {
	Ft               *pb.FileTail
	meta             *pb.Metadata
	ModificationTime time.Time
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

	_, err = extractFileTail(f)
	if err != nil {
		return nil, errors.Wrap(err, "extract tail error")
	}
	r = &reader{f: f}
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
		// todo: emtpy trail, log
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
	// get length of PostScript
	psLen := uint64(buf[readSize-1])
	ensureOrcFooter(f, int(psLen), buf)
	psOffset := uint64(readSize) - 1 - psLen
	ps, err := extractPostScript(buf[psOffset : psOffset+psLen])
	if err != nil {
		return nil, errors.WithStack(err)
	}
	fmt.Println(ps.String())
	/*cs := *ps.CompressionBlockSize
	bufferSize := int(cs)
	ps.GetCompression()
	sz := uint64(size)

	footerSize := int(ps.GetFooterLength())
	metaSize := int(ps.GetMetadataLength())

	//check if extra bytes need to be read
	extra := Max(0, int(psLen)+1+footerSize+metaSize-int(readSize))
	tailSize := 1 + int(psLen) + footerSize + metaSize
	if extra > 0 {
		//more bytes need to be read, read extra bytes
		ebuf := make([]byte, extra)
		if _, err := f.Seek(readSize, 0); err != nil {
			return nil, errors.WithStack(err)
		}
		if _, err = io.ReadFull(f, ebuf); err != nil {
			return nil, errors.WithStack(err)
		}
		buf = append(buf, ebuf...)
	}
	footerStart := int(psOffset) - footerSize - metaSize
	footer := &pb.Footer{}
	//todo: decode compression
	proto.Unmarshal(buf[footerStart:footerStart+tailSize], footer)

	ft := pb.FileTail{Postscript: ps, Footer: footer, FileLength: &sz, PostscriptLength: &psLen}

	ot := &OrcTail{}
	return ot, nil*/
	return nil, nil
}

func extractPostScript(buf []byte) (ps *pb.PostScript, err error) {
	ps = &pb.PostScript{}
	if err = proto.Unmarshal(buf, ps); err != nil {
		return nil, err
	}
	//checkOrcVersion(path, ps)

	// Check compression codec.
	/*switch ps.GetCompression() {
	default:
		return nil, errors.New("unknown compression")
	}*/
	return ps, err
}

func checkOrcVersion(path string, ps *pb.PostScript) error {
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

func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}
