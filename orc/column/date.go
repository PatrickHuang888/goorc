package column

import (
	"errors"
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/orc/stream"
	"github.com/patrickhuang888/goorc/pb/pb"
	"io"
)

func NewDateV2Reader(schema *api.TypeDescription, opts *config.ReaderOptions, f orcio.File) Reader {
	return &dateV2Reader{reader: &reader{schema: schema, opts: opts, f: f}}
}

type dateV2Reader struct {
	*reader
	present *stream.BoolReader
	data    *stream.IntRLV2Reader
}

func (c *dateV2Reader) InitStream(info *pb.Stream, startOffset uint64) error {

	if c.schema.Encoding == pb.ColumnEncoding_DIRECT {
		err := errors.New("encoding direct not impl")
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
		return err
	}

	return errors.New("stream unknown")
}

func (c *dateV2Reader) Next() (value api.Value, err error) {
	/*vector := (*vec).([]api.Date)
	vector = vector[:0]

	if !pFromParent {
		if err = c.nextPresents(presents); err != nil {
			return
		}
	}

	for i := 0; i < cap(vector) && c.cursor < c.numberOfRows; i++ {

		if len(*presents) == 0 || (len(*presents) != 0 && (*presents)[i]) {
			var v int64
			v, err = c.data.NextInt64()
			if err != nil {
				return
			}
			vector = append(vector, api.FromDays(v))
		} else {
			vector = append(vector, api.Date{})
		}

		c.cursor++
	}

	*vec = vector
	rows = len(vector)*/
	return
}

func (c *dateV2Reader) seek(indexEntry *pb.RowIndexEntry) error {
	pos := indexEntry.GetPositions()

	if c.present == nil {
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

func (c *dateV2Reader) Seek(rowNumber uint64) error {
	if !c.opts.HasIndex {
		return errors.New("no index")
	}

	stride := rowNumber / uint64(c.opts.IndexStride)
	offsetInStride := rowNumber % (stride * uint64(c.opts.IndexStride))

	if err := c.seek(c.index.GetEntry()[stride]); err != nil {
		return err
	}

	//c.cursor = stride * c.opts.IndexStride

	for i := 0; i < int(offsetInStride); i++ {
		if c.present != nil {
			if _, err := c.present.Next(); err != nil {
				return err
			}
		}
		if _, err := c.data.NextInt64(); err != nil {
			return err
		}
		//c.cursor++
	}
	return nil
}

func (c *dateV2Reader) Close() {
	if c.present != nil {
		c.present.Close()
	}
	c.data.Close()
}

type timestampWriter struct {
	*writer
	present   *stream.Writer
	data      *stream.Writer
	secondary *stream.Writer
}

func (t *timestampWriter) Writes(values []api.Value) error {
	return nil
}

func (t *timestampWriter) Write(value api.Value) error {
	hasValue := true

	if t.schema.HasNulls {
		if err := t.present.Write(!value.Null); err != nil {
			return err
		}
		if value.Null {
			hasValue = false
		}
	}

	if hasValue {
		time := value.V.(api.Timestamp)
		if err := t.data.Write(time.Seconds); err != nil {
			return err
		}
		if err := t.secondary.Write(uint64(time.Nanos)); err != nil {
			return err
		}

		*t.stats.NumberOfValues++

		if t.opts.WriteIndex {
			t.indexInRows++

			if t.indexInRows >= t.opts.IndexStride {
				// todo: write index
				entry := &pb.RowIndexEntry{Statistics: t.indexStats}
				t.index.Entry = append(t.index.Entry, entry)
				t.indexStats = &pb.ColumnStatistics{
					TimestampStatistics: &pb.TimestampStatistics{Maximum: new(int64), Minimum: new(int64), MaximumUtc: new(int64), MinimumUtc: new(int64)},
					NumberOfValues:      new(uint64), HasNull: new(bool)}
				if t.schema.HasNulls {
					*t.indexStats.HasNull = true
				}
				t.indexInRows = 0
			}

			if t.indexStats.TimestampStatistics == nil {
				t.indexStats.TimestampStatistics = &pb.TimestampStatistics{Maximum: new(int64), Minimum: new(int64), MaximumUtc: new(int64), MinimumUtc: new(int64)}
			}

			if time.GetMilliSeconds() > t.indexStats.TimestampStatistics.GetMaximum() {
				*t.indexStats.TimestampStatistics.Maximum = time.GetMilliSeconds()
			}
			if time.GetMilliSecondsUtc() > t.indexStats.TimestampStatistics.GetMaximumUtc() {
				*t.indexStats.TimestampStatistics.MaximumUtc = time.GetMilliSecondsUtc()
			}
			if t.indexStats.TimestampStatistics.GetMinimum() < time.GetMilliSeconds() {
				*t.indexStats.TimestampStatistics.Minimum = time.GetMilliSeconds()
			}
			if t.indexStats.TimestampStatistics.GetMaximumUtc() < time.GetMilliSecondsUtc() {
				*t.indexStats.TimestampStatistics.MinimumUtc = time.GetMilliSecondsUtc()
			}
			*t.indexStats.NumberOfValues++
		}
	}

	return nil
}

func (t *timestampWriter) Flush() error {
	if err := t.present.Flush(); err != nil {
		return err
	}
	if err := t.data.Flush(); err != nil {
		return err
	}
	if err := t.secondary.Flush(); err != nil {
		return err
	}

	if t.schema.HasNulls {
		*t.stats.BytesOnDisk = t.present.Info().GetLength()
	}
	*t.stats.BytesOnDisk += t.data.Info().GetLength()
	*t.stats.BytesOnDisk += t.secondary.Info().GetLength()
	return nil
}

func (t *timestampWriter) WriteOut(out io.Writer) (n int64, err error) {
	var np, nd, ns int64
	if np, err = t.present.WriteOut(out); err != nil {
		return
	}
	if nd, err = t.data.WriteOut(out); err != nil {
		return
	}
	if ns, err = t.secondary.WriteOut(out); err != nil {
		return
	}
	n = np + nd + ns
	return
}

func (t timestampWriter) GetStreamInfos() []*pb.Stream {
	if t.schema.HasNulls {
		return []*pb.Stream{t.present.Info(), t.data.Info(), t.secondary.Info()}
	}
	return []*pb.Stream{t.data.Info(), t.secondary.Info()}
}

func (t *timestampWriter) Reset() {
	t.writer.reset()

	t.data.Reset()
	t.secondary.Reset()
}

func (t timestampWriter) Size() int {
	return t.present.Size() + t.data.Size() + t.secondary.Size()
}

func newTimestampV2Writer(schema *api.TypeDescription, opts *config.WriterOptions) Writer {
	stats := &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64),
		TimestampStatistics: &pb.TimestampStatistics{Minimum: new(int64), MinimumUtc: new(int64), Maximum: new(int64), MaximumUtc: new(int64)}}
	var present *stream.Writer
	if schema.HasNulls {
		*stats.HasNull= true
		present = stream.NewBoolWriter(schema.Id, pb.Stream_PRESENT, opts)
	}
	data := stream.NewIntRLV2Writer(schema.Id, pb.Stream_DATA, opts, true)
	secondary := stream.NewIntRLV2Writer(schema.Id, pb.Stream_SECONDARY, opts, false)
	return &timestampWriter{&writer{schema: schema, opts: opts, stats: stats}, present, data, secondary}
}
