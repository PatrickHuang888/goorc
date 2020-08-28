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
	ChunkSize       int
	CompressionKind pb.CompressionKind
	StripeSize      uint64 // ~200MB
	BufferSize      uint   // written data in memory
}

func DefaultWriterOptions() WriterOptions {
	o := WriterOptions{}
	o.CompressionKind = pb.CompressionKind_ZLIB
	o.StripeSize = DEFAULT_STRIPE_SIZE
	o.ChunkSize = DEFAULT_CHUNK_SIZE
	o.BufferSize = DefalutBufferSize
	return o
}
