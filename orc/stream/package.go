package stream

import "github.com/patrickhuang888/goorc/orc/api"

type streamItf interface {
	Finished() bool
	Close()
}

type DateReader interface {
	streamItf
	Next() (date api.Date, err error)
	Seek(chunk uint64, chunkOffset uint64, offset uint64) error
}
