package stream

import (
	"bytes"
	"github.com/patrickhuang888/goorc/orc/common"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
)

type reader struct {
	info *pb.Stream

	start      uint64
	readLength uint64

	buf *bytes.Buffer

	opts *config.ReaderOptions

	in orcio.File
}

func (r *reader) ReadByte() (b byte, err error) {
	if r.buf.Len() >= 1 {
		b, err = r.buf.ReadByte()
		if err != nil {
			return b, errors.WithStack(err)
		}
		return
	}

	if r.readLength < r.info.GetLength() {
		if err = r.readAChunk(); err != nil {
			return 0, err
		}
	}

	b, err = r.buf.ReadByte()
	if err != nil {
		return b, errors.WithStack(err)
	}
	return
}

func (r *reader) Read(p []byte) (n int, err error) {
	if r.buf.Len() >= len(p) {
		n, err = r.buf.Read(p)
		if err != nil {
			return n, errors.WithStack(err)
		}
		return
	}

	for r.buf.Len() < len(p) && r.readLength < r.info.GetLength() {
		if err = r.readAChunk(); err != nil {
			return 0, err
		}
	}

	log.Tracef("stream %s reading, has read %d", r.info.String(), r.readLength)

	n, err = r.buf.Read(p)
	if err != nil {
		return n, errors.WithStack(err)
	}
	return
}

// offset seek to specific chunk and cuncompressedOffset in that chunk
// if NONE_COMPRESSION, offset just stream offset, and uncompressedOffset should be 0
func (r *reader) seek(offset uint64, uncompressedOffset uint64) error {
	if _, err := r.in.Seek(int64(r.start+offset), io.SeekStart); err != nil {
		return err
	}
	if err := r.readAChunk(); err != nil {
		return err
	}
	r.buf.Next(int(uncompressedOffset))
	return nil
}

// readAChunk read one chunk to s.buf
func (r *reader) readAChunk() error {
	// no compression
	if r.opts.CompressionKind == pb.CompressionKind_NONE { // no header
		l := r.opts.ChunkSize
		if r.info.GetLength()-r.readLength < l {
			l = r.info.GetLength() - r.readLength
		}

		log.Tracef("read a chunk, no compression copy %d from stream", l)

		if _, err := io.CopyN(r.buf, r.in, int64(l));err != nil {
			return errors.WithStack(err)
		}

		r.readLength += l
		return nil
	}

	head := make([]byte, 3)
	if _, err := io.ReadFull(r.in, head); err != nil {
		return errors.WithStack(err)
	}
	r.readLength += 3
	chunkLength, original := common.DecChunkHeader(head)

	log.Tracef("read a chunk, stream %s, compressing kind %s, chunkLength %d, original %t",
		r.info.String(), r.opts.CompressionKind, chunkLength, original)

	if uint64(chunkLength) > r.opts.ChunkSize {
		return errors.Errorf("chunk length %d larger than chunk size %d", chunkLength, r.opts.ChunkSize)
	}

	if original {
		if _, err := io.CopyN(r.buf, r.in, int64(chunkLength)); err != nil {
			return errors.WithStack(err)
		}
		r.readLength += uint64(chunkLength)
		return nil
	}

	readBuf := bytes.NewBuffer(make([]byte, chunkLength))
	readBuf.Reset()
	if _, err := io.CopyN(readBuf, r.in, int64(chunkLength)); err != nil {
		return errors.WithStack(err)
	}

	if _, err := common.DecompressChunkData(r.opts.CompressionKind, r.buf, readBuf); err != nil {
		return err
	}

	r.readLength += uint64(chunkLength)

	return nil
}

func (r reader) finished() bool {
	return r.readLength >= r.info.GetLength() && r.buf.Len() == 0
}

func (r *reader) Close(){
	if err:=r.in.Close();err!=nil {
		log.Error("closing error ", err)
	}
}
