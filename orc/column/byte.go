package column

import (
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/orc/stream"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
)

type byteWriter struct {
	*writer
	present *stream.Writer
	data    *stream.Writer
}

func (c *byteWriter) Write(values []api.Value) error {
	for _, v := range values {
		if err := c.write(v); err != nil {
			return err
		}
	}
	return nil
}

func (c *byteWriter) write(value api.Value) error {
	var err error
	hasValue := true

	if c.schema.HasNulls {
		if err = c.present.Write(!value.Null); err != nil {
			return err
		}
		if value.Null {
			hasValue = false
		}
	}

	if hasValue {
		if err = c.data.Write(value.V); err != nil {
			return err
		}

		*c.stats.BinaryStatistics.Sum++
		*c.stats.NumberOfValues++
	}

	if c.opts.WriteIndex {
		c.doIndex()

		if c.indexStats.BinaryStatistics == nil {
			c.indexStats.BinaryStatistics = &pb.BinaryStatistics{Sum: new(int64)}
		}
		*c.indexStats.BinaryStatistics.Sum++
		*c.indexStats.NumberOfValues++
	}

	return nil
}

func (c *byteWriter) Size() int {
	return c.present.Size() + c.data.Size()
}

func newByteWriter(schema *api.TypeDescription, opts *config.WriterOptions) Writer {
	stats := &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64), BinaryStatistics: &pb.BinaryStatistics{Sum: new(int64)}}
	base := &writer{schema: schema, opts: opts, stats: stats}
	present := stream.NewBoolWriter(schema.Id, pb.Stream_PRESENT, opts)
	data := stream.NewByteWriter(schema.Id, pb.Stream_DATA, opts)
	return &byteWriter{base, present, data}
}

func (c *byteWriter) doIndex() {
	c.indexInRows++

	if c.indexInRows >= c.opts.IndexStride {
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

func (c *byteWriter) GetStreamInfos() []*pb.Stream {
	var ss []*pb.Stream
	if c.present.Info().GetLength() != 0 {
		ss = append(ss, c.present.Info())
	}
	ss = append(ss, c.data.Info())
	return ss
}

func (c *byteWriter) GetStats() *pb.ColumnStatistics {
	return c.stats
}

func (c *byteWriter) Reset() {
	c.reset()
	c.present.Reset()
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
	if c.opts.WriteIndex {
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

type byteReader struct {
	*reader
	present *stream.BoolReader
	data    *stream.ByteReader
}

func NewByteReader(schema *api.TypeDescription, opts *config.ReaderOptions, f orcio.File) Reader {
	return &byteReader{reader: &reader{opts: opts, schema: schema, f: f}}
}

func (c *byteReader) InitStream(info *pb.Stream, startOffset uint64) error {
	if info.GetKind() == pb.Stream_PRESENT {
		ic, err := c.f.Clone()
		if err != nil {
			return err
		}
		c.present = stream.NewBoolReader(c.opts, info, startOffset, ic)
		ic.Seek(int64(startOffset), 0)
		return nil
	}

	if info.GetKind() == pb.Stream_DATA {
		ic, err := c.f.Clone()
		if err != nil {
			return err
		}
		c.data = stream.NewByteReader(c.opts, info, startOffset, ic)
		ic.Seek(int64(startOffset), 0)
		return nil
	}

	return errors.New("stream kind error")
}

func (c *byteReader) Next(values []api.Value) error {
	if err := c.checkInit(); err != nil {
		return err
	}

	if c.schema.HasNulls {
		for i := 0; i < len(values); i++ {
			p, err := c.present.Next()
			if err != nil {
				return err
			}
			values[i].Null = !p
		}
	}

	for i := 0; i < len(values); i++ {
		hasValue := true
		if c.schema.HasNulls && values[i].Null {
			hasValue = false
		}
		if hasValue {
			v, err := c.data.Next()
			if err != nil {
				return err
			}
			values[i].V = v
		}
	}

	return nil
}

func (c *byteReader) seek(indexEntry *pb.RowIndexEntry) error {
	pos := indexEntry.GetPositions()

	if !c.schema.HasNulls {
		if c.opts.CompressionKind == pb.CompressionKind_NONE {
			return c.data.Seek(pos[0], 0, pos[1])
		}

		return c.data.Seek(pos[0], pos[1], pos[2])
	}

	if c.opts.CompressionKind == pb.CompressionKind_NONE {
		if err := c.present.Seek(pos[0], 0, pos[1]); err != nil {
			return err
		}
		if err := c.data.Seek(pos[2], 0, pos[3]); err != nil {
			return err
		}
		return nil
	}

	if err := c.present.Seek(pos[0], pos[1], pos[2]); err != nil {
		return err
	}
	if err := c.data.Seek(pos[3], pos[4], pos[5]); err != nil {
		return err
	}
	return nil
}

func (c *byteReader) Seek(rowNumber uint64) error {
	if !c.opts.HasIndex {
		return errors.New("no index")
	}

	stride := rowNumber / c.opts.IndexStride
	strideOffset := rowNumber % (stride * c.opts.IndexStride)

	if err := c.seek(c.index.GetEntry()[stride]); err != nil {
		return err
	}

	vec := make([]api.Value, 0, strideOffset)
	if err := c.Next(vec); err != nil {
		return err
	}

	return nil
}

func (c *byteReader) Close() {
	if c.schema.HasNulls {
		c.present.Close()
	}
	c.data.Close()
}

func (c byteReader) checkInit() error {
	if c.data == nil {
		return errors.New("stream data not initialized!")
	}
	if c.schema.HasNulls && c.present == nil {
		return errors.New("stream present not initialized!")
	}
	return nil
}
