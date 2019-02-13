package orc

import (
	"github.com/pkg/errors"
)

const (
	NONE CompressionKind = iota
	ZLIB
	SNAPPY
	LZO
	LZ4
)

type CompressionKind int

type CompressionCodec interface {
}

func NewCodec(kind CompressionKind) (cc CompressionCodec, err error) {
	switch kind {
	case NONE:
	case ZLIB:
	// todo:
	case SNAPPY:
	// todo:
	case LZO:
	// todo:
	case LZ4:
		// todo:
	default:
		return nil, errors.Errorf("unknown compression codec %s", kind)
	}
	return cc, err
}
