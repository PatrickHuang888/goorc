package common

import (
	"bytes"
	"compress/flate"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
)

func Compressing(kind pb.CompressionKind, chunkSize int, dst *bytes.Buffer, src *bytes.Buffer) error {
	switch kind {
	case pb.CompressionKind_NONE:
		n, err := src.WriteTo(dst)
		log.Tracef("no compression write %d", n)
		if err != nil {
			return err
		}
	case pb.CompressionKind_ZLIB:
		if err := ZlibCompressing(chunkSize, dst, src); err != nil {
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
func ZlibCompressing(chunkSize int, dst *bytes.Buffer, src *bytes.Buffer) error {
	var start int
	remaining := src.Len()
	srcBytes := src.Bytes()

	cBuf := bytes.NewBuffer(make([]byte, chunkSize))
	cBuf.Reset()
	compressor, err := flate.NewWriter(cBuf, -1)
	if err != nil {
		return errors.WithStack(err)
	}

	log.Tracef("start zlib compressing, chunksize %d remaining %d", chunkSize, remaining)

	for remaining > chunkSize {

		if _, err = compressor.Write(srcBytes[start : start+chunkSize]); err != nil {
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

			log.Tracef("compressing original, write out %d, remaining %d", chunkSize, remaining-chunkSize)
			if _, err = dst.Write(srcBytes[start : start+chunkSize]); err != nil {
				return errors.WithStack(err)
			}

		} else {
			header := encChunkHeader(cBuf.Len(), false)
			if _, err = dst.Write(header); err != nil {
				return errors.WithStack(err)
			}

			log.Tracef("compressing zlib, write out after compressing %d, remaining %d", cBuf.Len(), remaining-chunkSize)
			if _, err = cBuf.WriteTo(dst); err != nil {
				return errors.WithStack(err)
			}
		}

		start += chunkSize
		remaining -= chunkSize
		compressor.Reset(cBuf)
	}

	if remaining > 0 {
		if _, err := compressor.Write(srcBytes[start : start+remaining]); err != nil {
			return errors.WithStack(err)
		}
		if err := compressor.Close(); err != nil {
			return errors.WithStack(err)
		}

		if cBuf.Len() > remaining {
			header := encChunkHeader(remaining, true)
			if _, err = dst.Write(header); err != nil {
				return errors.WithStack(err)
			}
			log.Tracef("compressing original, last write %d", remaining)
			if _, err = dst.Write(srcBytes[start : start+remaining]); err != nil {
				return errors.WithStack(err)
			}
		} else {
			header := encChunkHeader(cBuf.Len(), false)
			if _, err = dst.Write(header); err != nil {
				return errors.WithStack(err)
			}
			log.Tracef("compressing zlib,  last writing %d", cBuf.Len())
			if _, err = cBuf.WriteTo(dst); err != nil {
				return errors.WithStack(err)
			}
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
func DecompressBuffer(kind pb.CompressionKind, dst *bytes.Buffer, src *bytes.Buffer) (err error) {
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

func DecompressChunkData(kind pb.CompressionKind, dst *bytes.Buffer, src *bytes.Buffer) (n int64, err error) {
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
