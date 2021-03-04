package column

import (
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	"github.com/patrickhuang888/goorc/orc/stream"
	"github.com/patrickhuang888/goorc/pb/pb"
)

type writer struct {
	schema *api.TypeDescription
	opts   *config.WriterOptions

	stats *pb.ColumnStatistics

	present stream.Writer

	children []Writer

	indexInRows int
	indexStats  *pb.ColumnStatistics
	index       *pb.RowIndex

	flushed bool
}

func (w *writer) reset() {
	w.flushed = false

	if w.schema.HasNulls {
		w.present.Reset()
	}

	if w.opts.WriteIndex {
		w.indexInRows = 0
		w.index.Reset()
		w.indexStats.Reset()
		w.indexStats.HasNull = new(bool)
		w.indexStats.NumberOfValues = new(uint64)
		w.indexStats.BytesOnDisk = new(uint64)
	}

	// stats will not reset, it's for whole writer
}

func (w writer) GetIndex() *pb.RowIndex {
	return w.index
}

func (w writer) GetStats() *pb.ColumnStatistics {
	return w.stats
}
