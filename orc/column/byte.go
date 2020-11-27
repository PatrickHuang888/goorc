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

func newByteWriter(schema *api.TypeDescription, opts *config.WriterOptions) Writer {
	stats := &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64), BinaryStatistics: &pb.BinaryStatistics{Sum: new(int64)}}
	var present *stream.Writer
	if schema.HasNulls {
		*stats.HasNull = true
		present = stream.NewBoolWriter(schema.Id, pb.Stream_PRESENT, opts)
	}
	var indexStats *pb.ColumnStatistics
	if opts.WriteIndex {
		indexStats=  &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64), BinaryStatistics: &pb.BinaryStatistics{Sum: new(int64)}}
	}
	base := &writer{schema: schema, opts: opts, stats: stats, present: present, indexStats: indexStats}
	data := stream.NewByteWriter(schema.Id, pb.Stream_DATA, opts)
	return &byteWriter{base, data}
}

type byteWriter struct {
	*writer
	data *stream.Writer
}

func (w *byteWriter) Write(value api.Value) error {
	if w.schema.HasNulls {
		if err := w.present.Write(!value.Null); err != nil {
			return err
		}
	}

	if value.Null {
		return nil
	}

	if err := w.data.Write(value.V); err != nil {
		return err
	}

	*w.stats.BinaryStatistics.Sum++
	*w.stats.NumberOfValues++

	if w.opts.WriteIndex {
		w.indexInRows++
		if w.indexInRows >= w.opts.IndexStride {
			w.present.MarkPosition()
			w.data.MarkPosition()

			if w.index == nil {
				w.index = &pb.RowIndex{}
			}
			w.index.Entry = append(w.index.Entry, &pb.RowIndexEntry{Statistics: w.indexStats})
			// new stats
			w.indexStats = &pb.ColumnStatistics{BinaryStatistics: &pb.BinaryStatistics{Sum: new(int64)}, NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64)}
			if w.schema.HasNulls {
				*w.indexStats.HasNull = true
			}
			// does not write index statistic bytes on disk, java impl either
			w.indexInRows = 0
		}
		*w.indexStats.BinaryStatistics.Sum++
		*w.indexStats.NumberOfValues++
	}
	return nil
}

func (w *byteWriter) Size() int {
	if w.schema.HasNulls {
		return w.present.Size() + w.data.Size()
	}
	return w.data.Size()
}

func (w *byteWriter) Flush() error {
	w.flushed = true

	if w.schema.HasNulls {
		if err := w.present.Flush(); err != nil {
			return err
		}
	}
	if err := w.data.Flush(); err != nil {
		return err
	}

	if w.schema.HasNulls {
		*w.stats.BytesOnDisk = w.present.Info().GetLength()
	}
	*w.stats.BytesOnDisk += w.data.Info().GetLength()

	if w.opts.WriteIndex {
		var pp [][]uint64
		if w.schema.HasNulls {
			pp = w.present.GetPositions()
			if len(w.index.Entry) != len(pp) {
				return errors.New("index entry and position error")
			}
		}
		dp := w.data.GetPositions()
		if len(w.index.Entry) != len(dp) {
			return errors.New("index entry and position error")
		}

		for i, e := range w.index.Entry {
			e.Positions = append(e.Positions, pp[i]...)
			e.Positions = append(e.Positions, dp[i]...)
		}
	}
	return nil
}

func (w *byteWriter) GetStreamInfos() []*pb.Stream {
	if w.schema.HasNulls {
		return []*pb.Stream{w.present.Info(), w.data.Info()}
	}
	return []*pb.Stream{w.data.Info()}
}

func (w *byteWriter) Reset() {
	w.reset()
	w.data.Reset()
}

func (w *byteWriter) WriteOut(out io.Writer) (n int64, err error) {
	if !w.flushed {
		err = errors.New("not flushed!")
		return
	}

	var np, nd int64
	if w.schema.HasNulls {
		if np, err = w.present.WriteOut(out); err != nil {
			return
		}
	}
	if nd, err = w.data.WriteOut(out); err != nil {
		return
	}
	n = np + nd
	return
}

type byteReader struct {
	*reader
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
		if p, err = c.present.Next(); err != nil {
			return
		}
		value.Null = !p
	}

	if !value.Null {
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
