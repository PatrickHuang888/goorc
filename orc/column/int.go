package column

import (
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	"github.com/patrickhuang888/goorc/orc/encoding"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/orc/stream"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
	"io"
)

func newIntV2Writer(schema *api.TypeDescription, opts *config.WriterOptions) Writer {
	stats := &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64),
		IntStatistics: &pb.IntegerStatistics{Minimum: new(int64), Maximum: new(int64), Sum: new(int64)}}
	present := stream.NewBoolWriter(schema.Id, pb.Stream_PRESENT, opts)
	data := stream.NewIntRLV2Writer(schema.Id, pb.Stream_DATA, opts, true)
	base := &writer{schema: schema, stats: stats}
	return &intWriter{base, present, data}
}

type intWriter struct {
	*writer
	present *stream.Writer
	data    *stream.Writer
}

func (w *intWriter) Write(values []api.Value) error {
	for _, v := range values {
		if err := w.write(v); err != nil {
			return err
		}
	}
	return nil
}

func (w *intWriter) write(value api.Value) error {
	if w.schema.HasNulls {
		if err := w.present.Write(!value.Null); err != nil {
			return err
		}
	}

	hasValue := true
	if w.schema.HasNulls && value.Null {
		hasValue = false
	}

	if hasValue {
		v := value.V.(int64)

		// ?? zigzag before encoding
		if err := w.data.Write(encoding.Zigzag(v)); err != nil {
			return err
		}

		*w.stats.IntStatistics.Sum += v
		if v < *w.stats.IntStatistics.Minimum {
			*w.stats.IntStatistics.Minimum = v
		}
		if v > *w.stats.IntStatistics.Maximum {
			*w.stats.IntStatistics.Maximum = v
		}
	}

	*w.stats.NumberOfValues++

	// todo: write index
	return nil
}

func (w intWriter) Size() int {
	return w.present.Size() + w.data.Size()
}

func (w intWriter) Flush() error {
	w.flushed = true

	if err := w.present.Flush(); err != nil {
		return err
	}
	if err := w.data.Flush(); err != nil {
		return err
	}

	*w.stats.BytesOnDisk += w.present.Info().GetLength()
	*w.stats.BytesOnDisk += w.data.Info().GetLength()

	return nil
}

func (w *intWriter) WriteOut(out io.Writer) (n int64, err error) {
	if !w.flushed {
		err = errors.New("not flushed")
		return
	}

	var pn, dn int64
	if pn, err = w.present.WriteOut(out); err != nil {
		return
	}
	if dn, err = w.data.WriteOut(out); err != nil {
		return
	}
	n = pn + dn
	return
}

func (w intWriter) GetIndex() *pb.RowIndex {
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

func (w *intWriter) Reset() {
	w.reset()
	w.present.Reset()
	w.data.Reset()
}

func newIntV2Reader(schema *api.TypeDescription, opts *config.ReaderOptions, f orcio.File) Reader {
	return &intV2Reader{reader: &reader{schema: schema, opts: opts, f: f}}
}

type intV2Reader struct {
	*reader

	present *stream.BoolReader
	data    *stream.IntRLV2Reader
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
		c.data = stream.NewRLV2Reader(c.opts, info, startOffset, true, ic)
		return nil
	}
	return errors.New("stream unknown")
}

func (c *intV2Reader) Next(values []api.Value) error {
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
			v, err := c.data.NextInt64()
			if err != nil {
				return err
			}
			values[i].V = v
		}
	}

	return nil
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
			if err := c.present.Seek(pos[0], 0, pos[1]); err != nil {
				return err
			}
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

func (c *intV2Reader) Seek(rowNumber uint64) error {
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

func (c *intV2Reader) Close() {
	if c.schema.HasNulls {
		c.present.Close()
	}
	c.data.Close()
}
