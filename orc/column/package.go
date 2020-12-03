package column

import (
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/pb/pb"
	log "github.com/sirupsen/logrus"
	"io"
)

var logger=log.New()

type Reader interface {
	//InitChildren(children []Reader) error
	InitIndex(startOffset uint64, length uint64) error
	InitStream(info *pb.Stream, startOffset uint64) error

	Next() (value api.Value, err error)

	// Seek seek to row number offset to current stripe
	// if column is struct (or like)  children, and struct has present stream, then
	// seek to non-null row that is calculated by parent
	Seek(rowNumber uint64) error

	//Children() []Reader

	Close()
}

type Writer interface {
	Write(value api.Value) error

	// Flush flush stream(index, data) when write out stripe(reach stripe size), reach index stride or close file
	// update ColumnStats.BytesOnDisk and index before stripe written out
	// flush once before write out to store
	Flush() error

	// WriteOut to writer, should flush first, because index will be got after flush and
	// write out before data. n written total data length
	WriteOut(out io.Writer) (n int64, err error)

	//after flush
	GetIndex() *pb.RowIndex

	// GetStreamInfos get no-non streams, used for writing stripe footer after flush
	GetStreamInfos() []*pb.Stream

	// data will be updated after flush
	GetStats() *pb.ColumnStatistics

	// Reset for column writer reset after stripe write out
	Reset()

	//sum of streams size, used for stripe flushing condition, data size in memory
	Size() int
}

