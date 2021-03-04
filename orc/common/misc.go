package common

import (
	"bytes"
	"compress/flate"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
)

var logger = log.New()

func SetLogLevel(level log.Level)  {
	logger.SetLevel(level)
}

func CompressingAllInChunks(kind pb.CompressionKind, chunkSize int, dst *bytes.Buffer, src *bytes.Buffer) error {
	if err:= CompressingChunks(kind, chunkSize, dst, src);err!=nil {
		return err
	}
	return CompressingLeft(kind, chunkSize, dst, src)
}

// compressing src to chunks, except last data less than a chunk
func CompressingChunks(kind pb.CompressionKind, chunkSize int, dst *bytes.Buffer, src *bytes.Buffer) error {
	switch kind {
	case pb.CompressionKind_NONE:
		n, err := src.WriteTo(dst)
		logger.Tracef("no compression write %d", n)
		if err != nil {
			return err
		}
	case pb.CompressionKind_ZLIB:
		if err := zlibCompressingToChunks(chunkSize, dst, src); err != nil {
			return err
		}

	default:
		return errors.New("compression kind error")
	}
	return nil
}

func zlibCompressingToChunks(chunkSize int, dst *bytes.Buffer, src *bytes.Buffer) error {
	buf := make([]byte, chunkSize)

	cBuf := bytes.NewBuffer(make([]byte, chunkSize))
	compressor, err := flate.NewWriter(cBuf, -1)
	if err != nil {
		return errors.WithStack(err)
	}

	logger.Tracef("start zlib compressing, chunksize %d remaining %d", chunkSize, src.Len())

	for src.Len() > chunkSize {
		if _, err := io.ReadFull(src, buf); err != nil {
			return errors.WithStack(err)
		}

		cBuf.Reset()
		if _, err = compressor.Write(buf); err != nil {
			return errors.WithStack(err)
		}
		if err = compressor.Close(); err != nil {
			return errors.WithStack(err)
		}

		if cBuf.Len() > chunkSize { // original
			header := encChunkHeader(chunkSize, true)
			if _, err = dst.Write(header); err != nil {
				return errors.WithStack(err)
			}
			if _, err = dst.Write(buf); err != nil {
				return errors.WithStack(err)
			}
			logger.Tracef("write a chunk, compressed original, src remains %d, dst len %d", src.Len(), dst.Len())

		} else {
			header := encChunkHeader(cBuf.Len(), false)
			if _, err = dst.Write(header); err != nil {
				return errors.WithStack(err)
			}
			if _, err = cBuf.WriteTo(dst); err != nil {
				return errors.WithStack(err)
			}
			logger.Tracef("write a chunk, compressed zlib, src remaining %d, dst len %d", src.Len(), cBuf.Len())
		}
	}
	return nil
}

func zlibCompressingLeft(chunkSize int, dst *bytes.Buffer, src *bytes.Buffer) error {
	if src.Len() > chunkSize {
		return errors.New("src left len error")
	}

	data := src.Bytes()

	tmpBuf := bytes.NewBuffer(make([]byte, chunkSize))
	tmpBuf.Reset()
	if _, err := src.WriteTo(tmpBuf); err != nil {
		return errors.WithStack(err)
	}

	tmpCBuf := bytes.NewBuffer(make([]byte, chunkSize))
	tmpCBuf.Reset()
	compressor, err := flate.NewWriter(tmpCBuf, -1)
	if err != nil {
		return errors.WithStack(err)
	}
	if _, err := tmpBuf.WriteTo(compressor); err != nil {
		return errors.WithStack(err)
	}
	if err = compressor.Close(); err != nil {
		return errors.WithStack(err)
	}

	if tmpCBuf.Len() > len(data) { // original
		header := encChunkHeader(len(data), true)
		if _, err = dst.Write(header); err != nil {
			return errors.WithStack(err)
		}
		if _, err = dst.Write(data); err != nil {
			return errors.WithStack(err)
		}
		logger.Tracef("write last chunk, compressing original, dst len %d", dst.Len())
	} else {
		header := encChunkHeader(tmpCBuf.Len(), false)
		if _, err = dst.Write(header); err != nil {
			return errors.WithStack(err)
		}
		if _, err = tmpCBuf.WriteTo(dst); err != nil {
			return errors.WithStack(err)
		}
		logger.Tracef("write last chunk, compressing zlib,  dst len %d", dst.Len())
	}

	return nil
}

func CompressingLeft(kind pb.CompressionKind, chunkSize int, dst *bytes.Buffer, src *bytes.Buffer) error {
	switch kind {
	case pb.CompressionKind_NONE:
		return errors.New("no compression")
	case pb.CompressionKind_ZLIB:
		if err := zlibCompressingLeft(chunkSize, dst, src); err != nil {
			return err
		}

	default:
		return errors.New("compression kind error")
	}
	return nil
}

// zlib compress src valueBuf into dst, maybe to several chunks
// if whole src compressed then split to chunks, then cannot skip chunk decompress,
// so it should be compressing each chunk
func zlibCompressingAll(chunkSize int, dst *bytes.Buffer, src *bytes.Buffer) error {
	if err := zlibCompressingToChunks(chunkSize, dst, src); err != nil {
		return err
	}

	// compressing remaining
	data := src.Bytes()

	buf := bytes.NewBuffer(make([]byte, chunkSize))
	buf.Reset()
	if _, err := src.WriteTo(buf); err != nil {
		return errors.WithStack(err)
	}

	cBuf := bytes.NewBuffer(make([]byte, chunkSize))
	compressor, err := flate.NewWriter(cBuf, -1)
	if err != nil {
		return errors.WithStack(err)
	}
	if _, err := buf.WriteTo(compressor); err != nil {
		return errors.WithStack(err)
	}
	if err = compressor.Close(); err != nil {
		return errors.WithStack(err)
	}
	if cBuf.Len() > len(data) {
		header := encChunkHeader(len(data), true)
		if _, err = dst.Write(header); err != nil {
			return errors.WithStack(err)
		}
		logger.Tracef("compressing original, last write %d, dst len %d", len(data), dst.Len())
		if _, err = dst.Write(data); err != nil {
			return errors.WithStack(err)
		}

	} else {
		header := encChunkHeader(cBuf.Len(), false)
		if _, err = dst.Write(header); err != nil {
			return errors.WithStack(err)
		}
		logger.Tracef("compressing zlib,  last writing %d, dst len %d", cBuf.Len(), dst.Len())
		if _, err = cBuf.WriteTo(dst); err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

func encChunkHeader(l int, orig bool) (header []byte) {
	header = make([]byte, 3)
	if orig {
		header[0] = 0x01 | byte(l<<1)
	} else {
		header[0] = byte(l << 1)
	}
	header[1] = byte(l >> 7)
	header[2] = byte(l >> 15)
	return
}

func DecChunkHeader(h []byte) (length int, orig bool) {
	_ = h[2]
	return int(h[2])<<15 | int(h[1])<<7 | int(h[0])>>1, h[0]&0x01 == 0x01
}

// buffer should be compressed, maybe contains several chunks
func DecompressChunks(kind pb.CompressionKind, dst *bytes.Buffer, src *bytes.Buffer) (err error) {
	switch kind {
	case pb.CompressionKind_ZLIB:
		for src.Len() > 0 {
			header := make([]byte, 3)
			if _, err = src.Read(header); err != nil {
				return errors.WithStack(err)
			}

			chunkLength, original := DecChunkHeader(header)

			if original {
				if _, err = io.CopyN(dst, src, int64(chunkLength)); err != nil {
					return errors.WithStack(err)
				}
			} else {
				buf := bytes.NewBuffer(make([]byte, chunkLength))
				buf.Reset()
				if _, err = io.CopyN(buf, src, int64(chunkLength)); err != nil {
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

func Decompress(kind pb.CompressionKind, dst *bytes.Buffer, src *bytes.Buffer) (n int64, err error) {
	switch kind {

	case pb.CompressionKind_ZLIB:
		r := flate.NewReader(src)
		n, err = dst.ReadFrom(r)
		if err != nil {
			return 0, errors.WithStack(err)
		}
		if err = r.Close(); err != nil {
			return n, errors.WithStack(err)
		}
		return

	default:
		return 0, errors.New("compression kind other than zlib not impl")
	}

	return
}
