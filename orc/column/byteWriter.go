package column

import (
	"github.com/patrickhuang888/goorc/orc"
	"github.com/patrickhuang888/goorc/orc/config"
	"github.com/patrickhuang888/goorc/orc/stream"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
)

type byteWriter struct {
	*writer
	data stream.Writer
}

func newByteWriter(schema *orc.TypeDescription, opts *config.WriterOptions) Writer {
	stats := &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64), BinaryStatistics: &pb.BinaryStatistics{Sum: new(int64)}}
	base := &writer{schema: schema, present: stream.NewBoolWriter(schema.Id, pb.Stream_PRESENT, opts), stats: stats}
	data := stream.NewByteWriter(schema.Id, pb.Stream_DATA, opts)
	return &byteWriter{base, data}
}

func (c *byteWriter) Write(presents []bool, presentsFromParent bool, vec interface{}) (rows int, err error) {
	vector := vec.([]byte)
	rows = len(vector)

	if len(presents) == 0 {
		for _, v := range vector {
			if err = c.writeValue(v); err != nil {
				return
			}
			if c.writeIndex {
				c.doIndex()
			}
		}
		return
	}

	if len(presents) != len(vector) {
		return 0, errors.New("presents error")
	}

	if !presentsFromParent {
		*c.stats.HasNull = true
	}

	for i, p := range presents {

		if !presentsFromParent {
			if err = c.present.Write(p); err != nil {
				return
			}
		}

		if p {
			if err = c.writeValue(vector[i]); err != nil {
				return
			}
		}

		if c.writeIndex {
			c.doIndex()
		}
	}

	return
}

func (c *byteWriter) writeValue(v byte) error {
	var err error

	if err = c.data.Write(v); err != nil {
		return err
	}

	if c.writeIndex {
		if c.indexStats.BinaryStatistics == nil {
			c.indexStats.BinaryStatistics = &pb.BinaryStatistics{Sum: new(int64)}
		}
		*c.indexStats.BinaryStatistics.Sum++
		*c.indexStats.NumberOfValues++
	}

	*c.stats.BinaryStatistics.Sum++
	*c.stats.NumberOfValues++
	return nil
}

func (c *byteWriter) doIndex() {
	c.indexInRows++

	if c.indexInRows >= c.indexStride {
		c.present.MarkPosition()
		c.data.MarkPosition()

		entry := &pb.RowIndexEntry{Statistics: c.indexStats}
		c.indexEntries = append(c.indexEntries, entry)
		c.indexStats = &pb.ColumnStatistics{BinaryStatistics: &pb.BinaryStatistics{Sum: new(int64)}, NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64)}

		c.indexInRows = 0
	}
}

func (c *byteWriter) Flush() error {
	var err error

	if err = c.present.Flush(); err != nil {
		return err
	}
	if err = c.data.Flush(); err != nil {
		return err
	}

	*c.stats.BytesOnDisk += c.present.Info().GetLength()
	*c.stats.BytesOnDisk += c.data.Info().GetLength()
	return nil
}

func (c *byteWriter) Size() int {
	return c.present.Size() + c.data.Size()
}

func (c *byteWriter) GetStreamInfos() []*pb.Stream {
	return []*pb.Stream{c.present.Info(), c.data.Info()}
}

func (c *byteWriter) GetStats() *pb.ColumnStatistics {
	return c.stats
}

func (c *byteWriter) Reset() {
	c.writer.reset()
	c.data.Reset()
}

func (c *byteWriter) WriteOut(out io.Writer) (n int64, err error) {
	var np, nd int64
	if np, err = c.present.WriteOut(out); err != nil {
		return
	}
	if nd, err = c.data.WriteOut(out); err != nil {
		return
	}
	n = np + nd
	return
}

// after flush
func (c *byteWriter) GetIndex() *pb.RowIndex {
	if c.writeIndex {
		index := &pb.RowIndex{}

		pp := c.present.GetAndClearPositions()
		dp := c.data.GetAndClearPositions()

		if len(c.indexEntries) != len(pp) || len(c.indexEntries) != len(dp) {
			log.Errorf("index entry and position error")
			return nil
		}

		for i, e := range c.indexEntries {
			e.Positions = append(e.Positions, pp[i]...)
			e.Positions = append(e.Positions, dp[i]...)
		}
		index.Entry = c.indexEntries
		return index
	}

	return nil
}
