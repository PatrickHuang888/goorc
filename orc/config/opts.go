package config

import (
	"github.com/patrickhuang888/goorc/pb/pb"
	"time"
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
	DEFAULT_CHUNK_SIZE           = 256 * 1024
	MAX_CHUNK_LENGTH             = uint64(32768) // 15 bit
	DEFAULT_ROW_SIZE             = 1024 * 10
	DefaultEncoderBufferSize=8*1024
)

type ReaderOptions struct {
	CompressionKind pb.CompressionKind
	ChunkSize       uint64
	RowSize         int

	Loc *time.Location

	HasIndex    bool
	IndexStride uint64
}

type WriterOptions struct {
	RowSize int

	ChunkSize       int
	CompressionKind pb.CompressionKind
	StripeSize      int  // ~200MB
	BufferSize      uint // written data in memory
	EncoderBufferSize int

	WriteIndex  bool
	IndexStride int
}

func DefaultWriterOptions() WriterOptions {
	return WriterOptions{CompressionKind: pb.CompressionKind_ZLIB, StripeSize: DEFAULT_STRIPE_SIZE, ChunkSize: DEFAULT_CHUNK_SIZE,
		BufferSize: DefalutBufferSize, RowSize: DEFAULT_ROW_SIZE, EncoderBufferSize: DefalutBufferSize}
}

func DefaultReaderOptions() ReaderOptions {
	return ReaderOptions{RowSize: DEFAULT_ROW_SIZE, ChunkSize: DEFAULT_CHUNK_SIZE, CompressionKind: pb.CompressionKind_ZLIB}
}
