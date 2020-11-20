package column

import (
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/orc/stream"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
	"io"
)

type byteWriter struct {
	*writer
	data *stream.Writer
}

func (c *byteWriter) Write(value api.Value) error {
	hasValue := true

	if c.schema.HasNulls {
		if err := c.present.Write(!value.Null); err != nil {
			return err
		}
		if value.Null {
			hasValue = false
		}
	}

	if hasValue {
		if err := c.data.Write(value.V); err != nil {
			return err
		}

		*c.stats.BinaryStatistics.Sum++
		*c.stats.NumberOfValues++

		if c.opts.WriteIndex {
			c.indexInRows++

			if c.indexInRows >= c.opts.IndexStride {
				c.present.MarkPosition()
				c.data.MarkPosition()

				if c.index == nil {
					c.index = &pb.RowIndex{}
				}
				c.index.Entry = append(c.index.Entry, &pb.RowIndexEntry{Statistics: c.indexStats})
				// new stats
				c.indexStats = &pb.ColumnStatistics{BinaryStatistics: &pb.BinaryStatistics{Sum: new(int64)}, NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64)}
				if c.schema.HasNulls {
					*c.indexStats.HasNull = true
				}
				// does not write index statistic bytes on disk, java impl either
				c.indexInRows = 0
			}

			*c.indexStats.BinaryStatistics.Sum++
			*c.indexStats.NumberOfValues++
		}
	}

	return nil
}

func (c *byteWriter) Size() int {
	if c.schema.HasNulls {
		return c.present.Size() + c.data.Size()
	}
	return c.data.Size()
}

func newByteWriter(schema *api.TypeDescription, opts *config.WriterOptions) Writer {
	stats := &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64), BinaryStatistics: &pb.BinaryStatistics{Sum: new(int64)}}
	var present *stream.Writer
	if schema.HasNulls {
		*stats.HasNull = true
		present = stream.NewBoolWriter(schema.Id, pb.Stream_PRESENT, opts)
	}
	base := &writer{schema: schema, opts: opts, stats: stats, present: present}
	data := stream.NewByteWriter(schema.Id, pb.Stream_DATA, opts)
	return &byteWriter{base, data}
}

func (c *byteWriter) Flush() error {
	c.flushed = true

	if c.schema.HasNulls {
		if err := c.present.Flush(); err != nil {
			return err
		}
	}
	if err := c.data.Flush(); err != nil {
		return err
	}

	if c.schema.HasNulls {
		*c.stats.BytesOnDisk = c.present.Info().GetLength()
	}
	*c.stats.BytesOnDisk += c.data.Info().GetLength()

	if c.opts.WriteIndex {
		var pp [][]uint64
		if c.schema.HasNulls {
			pp = c.present.GetPositions()
			if len(c.index.Entry) != len(pp) {
				return errors.New("index entry and position error")
			}
		}

		dp := c.data.GetPositions()
		if len(c.index.Entry) != len(dp) {
			return errors.New("index entry and position error")
		}

		for i, e := range c.index.Entry {
			e.Positions = append(e.Positions, pp[i]...)
			e.Positions = append(e.Positions, dp[i]...)
		}
	}

	return nil
}

func (c *byteWriter) GetStreamInfos() []*pb.Stream {
	if c.schema.HasNulls {
		return []*pb.Stream{c.present.Info(), c.data.Info()}
	}
	return []*pb.Stream{c.data.Info()}
}

func (c *byteWriter) Reset() {
	c.reset()
	c.data.Reset()
}

func (c *byteWriter) WriteOut(out io.Writer) (n int64, err error) {
	if !c.flushed {
		err = errors.New("not flushed!")
		return
	}

	var np, nd int64
	if c.schema.HasNulls {
		if np, err = c.present.WriteOut(out); err != nil {
			return
		}
	}
	if nd, err = c.data.WriteOut(out); err != nil {
		return
	}
	n = np + nd
	return
}

type byteReader struct {
	*reader
	present *stream.BoolReader
	data    *stream.ByteReader
}

func newByteReader(schema *api.TypeDescription, opts *config.ReaderOptions, f orcio.File) Reader {
	return &byteReader{reader: &reader{opts: opts, schema: schema, f: f}}
}

func (c *byteReader) InitStream(info *pb.Stream, startOffset uint64) error {
	if info.GetKind() == pb.Stream_PRESENT {
		if !c.schema.HasNulls {
			return errors.New("column schema has no nulls")
		}
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

func (c *byteReader) Next() (value api.Value, err error) {
	if err = c.checkInit(); err != nil {
		return
	}

	if c.schema.HasNulls {
		var p bool
		if p, err = c.present.Next();err != nil {
			return
		}
		value.Null= !p
	}

	hasValue := true
	if c.schema.HasNulls && value.Null {
		hasValue = false
	}
	if hasValue {
		value.V, err = c.data.Next()
		if err != nil {
			return
		}
	}
	return
}

func (c *byteReader) seek(indexEntry *pb.RowIndexEntry) error {
	if err := c.checkInit(); err != nil {
		return err
	}

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

	for i := 0; i < int(strideOffset); i++ {
		if _, err := c.Next(); err != nil {
			return err
		}
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
