package column

import (
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	"github.com/patrickhuang888/goorc/orc/stream"
	"github.com/patrickhuang888/goorc/pb/pb"
	"io"
)

func newIntV2Writer(schema *api.TypeDescription, opts *config.WriterOptions) Writer {
	stats := &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64),
		IntStatistics: &pb.IntegerStatistics{Minimum: new(int64), Maximum: new(int64), Sum: new(int64)}}
	base := &writer{schema: schema, present: stream.NewBoolWriter(schema.Id, pb.Stream_PRESENT, opts), stats: stats}
	data := stream.NewIntRLV2Writer(schema.Id, pb.Stream_DATA, opts, true)
	return &intWriter{base, data}
}

type intWriter struct {
	*writer
	data stream.IntWriter
}

func (w *intWriter) Write(presents []bool, presentsFromParent bool, vec interface{}) (rows int, err error) {
	vector := vec.([]int64)
	rows = len(vector)

	if !presentsFromParent {
		if err=w.present.WriteBools(presents);err!=nil {
			return
		}
	}

	if len(presents)==0 {
		if err= w.data.WriteInts(vector);err!=nil {
			return
		}
	}
}

func (i intWriter) Flush() error {
	panic("implement me")
}

func (i intWriter) WriteOut(out io.Writer) (n int64, err error) {
	panic("implement me")
}

func (i intWriter) GetIndex() *pb.RowIndex {
	panic("implement me")
}

func (w intWriter) GetStreamInfos() []*pb.Stream {
	var ss []*pb.Stream
	if w.present.Info().GetLength() != 0 {
		ss = append(ss, w.present.Info())
	}
	ss = append(ss, w.data.Info())
	return ss
}

func (w intWriter) GetStats() *pb.ColumnStatistics {
	return w.stats
}

func (i intWriter) Reset() {
	panic("implement me")
}
