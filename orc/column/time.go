package column

import (
	"errors"
	"io"

	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/orc/stream"
	"github.com/patrickhuang888/goorc/pb/pb"
)

func NewDateV2Writer(schema *api.TypeDescription, opts *config.WriterOptions) Writer {
	stats := &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64),
		DateStatistics: &pb.DateStatistics{Minimum: new(int32), Maximum: new(int32)}}
	var present *stream.Writer
	if schema.HasNulls {
		*stats.HasNull = true
		present = stream.NewBoolWriter(schema.Id, pb.Stream_PRESENT, opts)
	}
	var indexStats *pb.ColumnStatistics
	var index *pb.RowIndex
	if opts.WriteIndex {
		indexStats = &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64),
			DateStatistics: &pb.DateStatistics{Minimum: new(int32), Maximum: new(int32)}}
		index = &pb.RowIndex{}
	}
	base := &writer{schema: schema, opts: opts, stats: stats, present: present, indexStats: indexStats, index: index}
	data := stream.NewIntRLV2Writer(schema.Id, pb.Stream_DATA, opts, true)
	return &byteWriter{base, data}
}

type dateV2Writer struct {
	*writer
	data *stream.Writer
}

func (w *dateV2Writer) Write(value api.Value) error {
	if w.schema.HasNulls {
		if err := w.present.Write(!value.Null); err != nil {
			return err
		}
	}

	if !value.Null {
		if err := w.data.Write(value.V); err != nil {
			return err
		}
		*w.stats.BinaryStatistics.Sum++
		*w.stats.NumberOfValues++ // makeSure:
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
			w.indexStats = &pb.ColumnStatistics{BinaryStatistics: &pb.BinaryStatistics{Sum: new(int64)}, NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64)}
			if w.schema.HasNulls {
				*w.indexStats.HasNull = true
			}
			w.indexInRows = 0
		}
		// fixme: does not write index statistic bytes on disk, java impl either
		if !value.Null {
			*w.indexStats.BinaryStatistics.Sum++
			*w.indexStats.NumberOfValues++
		}
	}
	return nil
}

func (w *dateV2Writer) Flush() error {
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
	w.flushed = true
	return nil
}

func (w *dateV2Writer) WriteOut(out io.Writer) (n int64, err error) {
	var pn, dn int64
	if w.schema.HasNulls {
		if pn, err = w.present.WriteOut(out); err != nil {
			return
		}
	}
	if dn, err = w.data.WriteOut(out); err != nil {
		return
	}
	return pn + dn, nil
}

func (w dateV2Writer) GetStreamInfos() []*pb.Stream {
	if w.schema.HasNulls {
		return []*pb.Stream{w.present.Info(), w.data.Info()}
	}
	return []*pb.Stream{w.data.Info()}
}

func (w *dateV2Writer) Reset() {
	if w.schema.HasNulls {
		w.present.Reset()
	}
	w.data.Reset()
}

func (w dateV2Writer) Size() int {
	if w.schema.HasNulls {
		return w.present.Size() + w.data.Size()
	}
	return w.data.Size()
}

func NewDateV2Reader(schema *api.TypeDescription, opts *config.ReaderOptions, f orcio.File) Reader {
	return &dateV2Reader{reader: &reader{schema: schema, opts: opts, f: f}}
}

type dateV2Reader struct {
	*reader
	data stream.DateReader
}

func (r *dateV2Reader) InitStream(info *pb.Stream, startOffset uint64) error {
	ic, err := r.f.Clone()
	if err != nil {
		return err
	}
	if _, err := ic.Seek(int64(startOffset), io.SeekStart); err != nil {
		return err
	}

	if r.schema.Encoding != pb.ColumnEncoding_DIRECT_V2 {
		return errors.New("encoding error")
	}

	if info.GetKind() == pb.Stream_PRESENT {
		r.present = stream.NewBoolReader(r.opts, info, startOffset, ic)
		return nil
	}
	if info.GetKind() == pb.Stream_DATA {
		r.data = stream.NewDateV2Reader(r.opts, info, startOffset, ic)
		return err
	}
	return errors.New("stream kind unknown")
}

func (r *dateV2Reader) Next() (value api.Value, err error) {
	if err = r.checkInit(); err != nil {
		return
	}

	if r.schema.HasNulls {
		var p bool
		if p, err = r.present.Next(); err != nil {
			return
		}
		value.Null = !p
	}

	if !value.Null {
		value.V, err = r.data.Next()
		if err != nil {
			return
		}
	}
	return
}

func (r *dateV2Reader) seek(indexEntry *pb.RowIndexEntry) error {
	if r.schema.HasNulls {
		if err := r.seekPresent(indexEntry); err != nil {
			return err
		}
	}
	var dataChunk, dataChunkOffset, dataOffset uint64
	if indexEntry != nil {
		pos := indexEntry.Positions
		if r.opts.CompressionKind == pb.CompressionKind_NONE {
			if r.schema.HasNulls {
				dataChunkOffset = pos[3]
				dataOffset = pos[4]
			} else {
				dataChunkOffset = pos[0]
				dataOffset = pos[1]
			}
		} else {
			if r.schema.HasNulls {
				dataChunk = pos[4]
				dataChunkOffset = pos[5]
				dataOffset = pos[6]
			} else {
				dataChunk = pos[0]
				dataChunkOffset = pos[1]
				dataOffset = pos[2]
			}
		}
	}
	return r.data.Seek(dataChunk, dataChunkOffset, dataOffset)
}

func (r *dateV2Reader) Seek(rowNumber uint64) error {
	if err := r.checkInit(); err != nil {
		return err
	}
	if !r.opts.HasIndex {
		return errors.New("no index")
	}

	entry, offset := r.reader.getIndexEntryAndOffset(rowNumber)
	if err := r.seek(entry); err != nil {
		return err
	}
	for i := 0; i < int(offset); i++ {
		if _, err := r.Next(); err != nil {
			return err
		}
	}
	return nil
}

func (c *dateV2Reader) Close() {
	if c.present != nil {
		c.present.Close()
	}
	c.data.Close()
}

func (r dateV2Reader) checkInit() error {
	if r.data == nil {
		return errors.New("stream data not initialized!")
	}
	if r.schema.HasNulls && r.present == nil {
		return errors.New("stream present not initialized!")
	}
	return nil
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
		*stats.HasNull = true
		present = stream.NewBoolWriter(schema.Id, pb.Stream_PRESENT, opts)
	}
	data := stream.NewIntRLV2Writer(schema.Id, pb.Stream_DATA, opts, true)
	secondary := stream.NewIntRLV2Writer(schema.Id, pb.Stream_SECONDARY, opts, false)
	return &timestampWriter{&writer{schema: schema, opts: opts, stats: stats}, present, data, secondary}
}
