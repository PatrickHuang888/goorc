package config

import (
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
)

const (
	MIN_ROW_INDEX_STRIDE         = 1000
	DEFAULT_STRIPE_SIZE          = 256 * 1024 * 1024
	DefalutBufferSize            = 10 * 1024 * 2014
	DEFAULT_INDEX_SIZE           = 100 * 1024
	DEFAULT_PRESENT_SIZE         = 100 * 1024
	DEFAULT_DATA_SIZE            = 1 * 1024 * 1024
	DEFAULT_LENGTH_SIZE          = 100 * 1024
	DEFAULT_ENCODING_BUFFER_SIZE = 100 * 1024
	DefaultChunkSize             = 256 * 1024
	MAX_CHUNK_LENGTH             = uint64(32768) // 15 bit
	DefaultEncoderBufferSize     = 8 * 1024
	DefaultIndexStride           = 1_000
)

type ReaderOptions struct {
	CompressionKind pb.CompressionKind
	ChunkSize       uint64
	IndexStride int
}

type WriterOptions struct {
	//RowSize int

	ChunkSize         int
	CompressionKind   pb.CompressionKind
	StripeSize        int  // ~200MB
	BufferSize        uint // written data in memory
	EncoderBufferSize int

	WriteIndex  bool
	IndexStride int

	//CreateVector bool
}

func CheckWriteOpts(opts * WriterOptions) error {
	if opts.ChunkSize > opts.StripeSize {
		return errors.Errorf("ChunkSize %d larger than StripeSize %d ", opts.ChunkSize, opts.StripeSize)
	}
	return nil
}

func DefaultWriterOptions() WriterOptions {
	return WriterOptions{CompressionKind: pb.CompressionKind_ZLIB, StripeSize: DEFAULT_STRIPE_SIZE, ChunkSize: DefaultChunkSize,
		BufferSize: DefalutBufferSize, EncoderBufferSize: DefalutBufferSize, IndexStride: DefaultIndexStride}
}

func DefaultReaderOptions() ReaderOptions {
	return ReaderOptions{ChunkSize: DefaultChunkSize, CompressionKind: pb.CompressionKind_ZLIB,
		IndexStride: DefaultIndexStride}
}
