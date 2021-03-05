package orc

import (
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	"github.com/patrickhuang888/goorc/pb/pb"
	log "github.com/sirupsen/logrus"
)

var logger = log.New()

func SetLogLevel(level log.Level) {
	logger.SetLevel(level)
}

type Reader interface {
	CreateBatchReader(opts *api.BatchOption) (BatchReader, error)

	GetSchema() *api.TypeDescription

	GetReaderOptions() *config.ReaderOptions

	NumberOfRows() uint64

	GetStatistics() []*pb.ColumnStatistics

	Close()
}


type BatchReader interface {

	// return end is there any more rows to read
	Next(vec *api.ColumnVector) (end bool, err error)

	Seek(rowNumber uint64) error

	Close()
}
