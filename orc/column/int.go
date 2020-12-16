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

func newIntV2Writer(schema *api.TypeDescription, opts *config.WriterOptions) Writer {
	stats := &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64),
		IntStatistics: &pb.IntegerStatistics{Minimum: new(int64), Maximum: new(int64), Sum: new(int64)}}
	var present *stream.Writer
	if schema.HasNulls {
		*stats.HasNull = true
		present = stream.NewBoolWriter(schema.Id, pb.Stream_PRESENT, opts)
	}
	var indexStats *pb.ColumnStatistics
	var index *pb.RowIndex
	if opts.WriteIndex {
		indexStats = &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64),
			IntStatistics: &pb.IntegerStatistics{Sum: new(int64), Minimum: new(int64), Maximum: new(int64)}}
		index = &pb.RowIndex{}
	}
	base := &writer{opts:opts, schema: schema, stats: stats, present: present, index: index, indexStats: indexStats}
	data := stream.NewIntRLV2Writer(schema.Id, pb.Stream_DATA, opts, true)
	return &intWriter{base, data}
}

type intWriter struct {
	*writer
	data *stream.Writer
}

func (w *intWriter) Writes(values []api.Value) error {
	for _, v := range values {
		if err := w.Write(v); err != nil {
			return err
		}
	}
	return nil
}

func (w *intWriter) Write(value api.Value) error {
	if w.schema.HasNulls {
		if err := w.present.Write(!value.Null); err != nil {
			return err
		}
	}

	if !value.Null {
		v := value.V.(int64)
		if err := w.data.Write(v); err != nil {
			return err
		}
		*w.stats.IntStatistics.Sum += v
		if v < *w.stats.IntStatistics.Minimum {
			*w.stats.IntStatistics.Minimum = v
		}
		if v > *w.stats.IntStatistics.Maximum {
			*w.stats.IntStatistics.Maximum = v
		}
		*w.stats.NumberOfValues++
	}

	if w.opts.WriteIndex {
		w.indexInRows++
		if w.indexInRows >= w.opts.IndexStride {
			var pp []uint64
			if w.schema.HasNulls {
				pp = append(pp, w.present.GetPosition()...)
			}
			pp = append(pp, w.data.GetPosition()...)
			w.index.Entry = append(w.index.Entry, &pb.RowIndexEntry{Positions: pp, Statistics: w.indexStats})

			// new stats
			w.indexStats = &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64),
				IntStatistics: &pb.IntegerStatistics{Sum: new(int64), Minimum: new(int64), Maximum: new(int64)}}
			if w.schema.HasNulls {
				*w.indexStats.HasNull = true
			}
			w.indexInRows = 0
		}
		if !value.Null {
			v := value.V.(int64)
			*w.indexStats.IntStatistics.Sum += v
			if v < *w.indexStats.IntStatistics.Minimum {
				*w.indexStats.IntStatistics.Minimum = v
			}
			if v > *w.indexStats.IntStatistics.Maximum {
				*w.indexStats.IntStatistics.Maximum = v
			}
			*w.indexStats.NumberOfValues++
		}
	}
	return nil
}

func (w intWriter) Size() int {
	if w.schema.HasNulls {
		return w.present.Size() + w.data.Size()
	}
	return w.data.Size()
}

func (w intWriter) Flush() error {
	if w.schema.HasNulls {
		if err := w.present.Flush(); err != nil {
			return err
		}
		*w.stats.BytesOnDisk = w.present.Info().GetLength()
	}
	if err := w.data.Flush(); err != nil {
		return err
	}
	*w.stats.BytesOnDisk += w.data.Info().GetLength()

	w.flushed = true
	return nil
}

func (w *intWriter) WriteOut(out io.Writer) (n int64, err error) {
	if !w.flushed {
		err = errors.New("not flushed")
		return
	}

	var pn, dn int64
	if w.schema.HasNulls {
		if pn, err = w.present.WriteOut(out); err != nil {
			return
		}
	}
	if dn, err = w.data.WriteOut(out); err != nil {
		return
	}
	n = pn + dn
	return
}

func (w intWriter) GetIndex() *pb.RowIndex {
	return w.index
}

func (w intWriter) GetStreamInfos() []*pb.Stream {
	if w.schema.HasNulls {
		return []*pb.Stream{w.present.Info(), w.data.Info()}
	}
	return []*pb.Stream{w.data.Info()}
}

func (w intWriter) GetStats() *pb.ColumnStatistics {
	return w.stats
}

func (w *intWriter) Reset() {
	w.reset()
	w.data.Reset()
}

func newIntV2Reader(schema *api.TypeDescription, opts *config.ReaderOptions, f orcio.File) Reader {
	return &intV2Reader{reader: &reader{schema: schema, opts: opts, f: f}}
}

type intV2Reader struct {
	*reader

	data *stream.IntRLV2Reader
}

func (c *intV2Reader) InitStream(info *pb.Stream, startOffset uint64) error {

	if c.schema.Encoding == pb.ColumnEncoding_DIRECT {
		err := errors.New("int reader encoding direct not impl")
		return err
	}

	if info.GetKind() == pb.Stream_PRESENT {
		ic, err := c.f.Clone()
		if err != nil {
			return err
		}
		if _, err := ic.Seek(int64(startOffset), io.SeekStart); err != nil {
			return err
		}
		c.present = stream.NewBoolReader(c.opts, info, startOffset, ic)
		return nil
	}

	if info.GetKind() == pb.Stream_DATA {
		ic, err := c.f.Clone()
		if err != nil {
			return err
		}
		if _, err := ic.Seek(int64(startOffset), io.SeekStart); err != nil {
			return err
		}
		c.data = stream.NewIntRLV2Reader(c.opts, info, startOffset, true, ic)
		return nil
	}
	return errors.New("stream unknown")
}

func (c *intV2Reader) Next() (value api.Value, err error) {
	if err = c.checkInit(); err != nil {
		return
	}

	// if parent struct has nulls then child like int's schema will not has nulls ?
	if c.schema.HasNulls {
		var p bool
		if p, err = c.present.Next(); err != nil {
			return
		}
		value.Null = !p
	}

	if !value.Null {
		value.V, err = c.data.NextInt64()
		if err != nil {
			return
		}
	}
	return
}

func (c intV2Reader) checkInit() error {
	if c.data == nil {
		return errors.New("stream data not initialized!")
	}
	if c.schema.HasNulls && c.present == nil {
		return errors.New("stream present not initialized!")
	}
	return nil
}

func (c *intV2Reader) seek(indexEntry *pb.RowIndexEntry) error {
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
		if c.present != nil {
			if err := c.present.Seek(pos[0], 0, pos[1], pos[2]); err != nil {
				return err
			}
		}
		if err := c.data.Seek(pos[3], 0, pos[4]); err != nil {
			return err
		}
		return nil
	}

	if err := c.present.Seek(pos[0], pos[1], pos[2], pos[3]); err != nil {
		return err
	}
	if err := c.data.Seek(pos[4], pos[5], pos[6]); err != nil {
		return err
	}
	return nil
}

func (c *intV2Reader) Seek(rowNumber uint64) error {
	if !c.opts.HasIndex {
		return errors.New("no index")
	}

	var offset uint64
	if rowNumber < uint64(c.opts.IndexStride) {
		// from start
		if err := c.seek(nil); err != nil {
			return err
		}
		offset = rowNumber
	} else {
		stride := rowNumber / uint64(c.opts.IndexStride)
		offset = rowNumber % (stride * uint64(c.opts.IndexStride))
		if err := c.seek(c.index.GetEntry()[stride-1]); err != nil {
			return err
		}
	}

	for i := 0; i < int(offset); i++ {
		if _, err := c.Next(); err != nil {
			return err
		}
	}
	return nil
}

func (c *intV2Reader) Close() {
	if c.schema.HasNulls {
		c.present.Close()
	}
	c.data.Close()
}
