package config

import (
	"github.com/patrickhuang888/goorc/pb/pb"
	"time"
)

type ReaderOptions struct {
	CompressionKind pb.CompressionKind
	ChunkSize       uint64
	RowSize         int

	Loc *time.Location

	HasIndex    bool
	IndexStride uint64

	MockTest bool
}


type WriterOptions struct {
	ChunkSize       int
	CompressionKind pb.CompressionKind
	StripeSize      uint64 // ~200MB
	BufferSize      uint   // written data in memory
}

