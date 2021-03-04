package column

import (
	"errors"
	"io"
	"time"

	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/orc/stream"
	"github.com/patrickhuang888/goorc/pb/pb"
)

func NewDateV2Writer(schema *api.TypeDescription, opts *config.WriterOptions) Writer {
	stats := &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64),
		DateStatistics: &pb.DateStatistics{Minimum: new(int32), Maximum: new(int32)}}
	var present stream.Writer
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
	return &dateV2Writer{base, data}
}

type dateV2Writer struct {
	*writer
	data stream.Writer
}

func (w *dateV2Writer) Write(value api.Value) error {
	if w.schema.HasNulls {
		if err := w.present.Write(!value.Null); err != nil {
			return err
		}
	}

	var days int32
	if !value.Null {
		date := value.V.(api.Date)
		days = api.ToDays(date)
		if err := w.data.Write(int64(days)); err != nil {
			return err
		}
		if days < w.stats.DateStatistics.GetMinimum() {
			*w.stats.DateStatistics.Minimum = days
		}
		if days > w.stats.DateStatistics.GetMaximum() {
			*w.stats.DateStatistics.Maximum = days
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
			w.indexStats = &pb.ColumnStatistics{DateStatistics: &pb.DateStatistics{Minimum: new(int32), Maximum: new(int32)},
				NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64)}
			if w.schema.HasNulls {
				*w.indexStats.HasNull = true
			}
			w.indexInRows = 0
		}
		if !value.Null {
			if days < w.stats.DateStatistics.GetMinimum() {
				*w.stats.DateStatistics.Minimum = days
			}
			if days > w.stats.DateStatistics.GetMaximum() {
				*w.stats.DateStatistics.Maximum = days
			}
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
	w.reset()
	w.data.Reset()
}

func (w dateV2Writer) Size() int {
	if w.schema.HasNulls {
		return w.present.Size() + w.data.Size()
	}
	return w.data.Size()
}

func NewDateV2Reader(schema *api.TypeDescription, opts *config.ReaderOptions, f orcio.File) Reader {
	return &DateV2Reader{reader: reader{schema: schema, opts: opts, f: f}}
}

type DateV2Reader struct {
	reader

	present *stream.BoolReader
	data    stream.DateReader
}

func (r *DateV2Reader) InitStream(info *pb.Stream, startOffset uint64) error {
	f, err := r.f.Clone()
	if err != nil {
		return err
	}
	if _, err := f.Seek(int64(startOffset), io.SeekStart); err != nil {
		return err
	}

	if r.schema.Encoding != pb.ColumnEncoding_DIRECT_V2 {
		return errors.New("encoding error")
	}

	if info.GetKind() == pb.Stream_PRESENT {
		if !r.schema.HasNulls {
			return errors.New("schema has no nulls")
		}
		r.present = stream.NewBoolReader(r.opts, info, startOffset, f)
		return nil
	}
	if info.GetKind() == pb.Stream_DATA {
		r.data = stream.NewDateV2Reader(r.opts, info, startOffset, f)
		return err
	}
	return errors.New("stream kind unknown")
}

func (r *DateV2Reader) NextBatch(vec *api.ColumnVector) error {
	var err error
	for i := 0; i < len(vec.Vector); i++ {
		if r.present != nil {
			var p bool
			if p, err = r.present.Next(); err != nil {
				return err
			}
			vec.Vector[i].Null = !p
		}

		if !vec.Vector[i].Null {
			vec.Vector[i].V, err = r.data.Next()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *DateV2Reader) Skip(rows uint64) error {
	var err error
	p := true

	for i := 0; i < int(rows); i++ {
		if r.present != nil {
			if p, err = r.present.Next(); err != nil {
				return err
			}
		}

		if p {
			if _, err = r.data.Next(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *DateV2Reader) SeekStride(stride int) error {
	pos, err := r.getStridePositions(stride)
	if err != nil {
		return err
	}

	if r.present != nil {
		var pChunk, pChunkOffset, pOffset1, pOffset2 uint64
		if r.opts.CompressionKind == pb.CompressionKind_NONE {
			// no compression
			pChunkOffset = pos[0]
			pOffset1 = pos[1]
			pOffset2 = pos[2]
		} else {
			// compression
			pChunk = pos[0]
			pChunkOffset = pos[1]
			pOffset1 = pos[2]
			pOffset2 = pos[3]
		}
		if err = r.present.Seek(pChunk, pChunkOffset, pOffset1, pOffset2); err != nil {
			return err
		}
	}

	var dataChunk, dataChunkOffset, dataOffset uint64
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

	return r.data.Seek(dataChunk, dataChunkOffset, dataOffset)
}

func (r *DateV2Reader) Close() {
	if r.present != nil {
		r.present.Close()
	}
	r.data.Close()
}

func NewTimestampV2Writer(schema *api.TypeDescription, opts *config.WriterOptions) Writer {
	stats := &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64),
		TimestampStatistics: &pb.TimestampStatistics{Minimum: new(int64), MinimumUtc: new(int64), Maximum: new(int64), MaximumUtc: new(int64)}}
	var present stream.Writer
	if schema.HasNulls {
		*stats.HasNull = true
		present = stream.NewBoolWriter(schema.Id, pb.Stream_PRESENT, opts)
	}
	var indexStats *pb.ColumnStatistics
	var index *pb.RowIndex
	if opts.WriteIndex {
		indexStats = &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64),
			DoubleStatistics: &pb.DoubleStatistics{Maximum: new(float64), Minimum: new(float64), Sum: new(float64)}}
		index = &pb.RowIndex{}
	}
	data := stream.NewIntRLV2Writer(schema.Id, pb.Stream_DATA, opts, true)
	secondary := stream.NewIntRLV2Writer(schema.Id, pb.Stream_SECONDARY, opts, false)
	return &timestampWriter{&writer{schema: schema, opts: opts, stats: stats, present: present, index: index,
		indexStats: indexStats}, data, secondary}
}

type timestampWriter struct {
	*writer
	data      stream.Writer
	secondary stream.Writer
}

func (w *timestampWriter) Write(value api.Value) error {
	if w.schema.HasNulls {
		if err := w.present.Write(!value.Null); err != nil {
			return err
		}
	}

	if !value.Null {
		t := value.V.(api.Timestamp)
		if err := w.data.Write(t.Seconds); err != nil {
			return err
		}
		if err := w.secondary.Write(api.EncodingTimestampNanos(t.Nanos)); err != nil {
			return err
		}

		*w.stats.NumberOfValues++
		ms := t.GetMilliSeconds()
		if ms < w.stats.TimestampStatistics.GetMinimum() {
			*w.stats.TimestampStatistics.Minimum = ms
		}
		if ms > w.stats.TimestampStatistics.GetMaximum() {
			*w.stats.TimestampStatistics.Maximum = ms
		}
		msUtc := t.GetMilliSecondsUtc()
		if ms < w.stats.TimestampStatistics.GetMinimumUtc() {
			*w.stats.TimestampStatistics.Minimum = msUtc
		}
		if ms > w.stats.TimestampStatistics.GetMaximum() {
			*w.stats.TimestampStatistics.MaximumUtc = msUtc
		}

		if w.opts.WriteIndex {
			w.indexInRows++

			if w.indexInRows >= w.opts.IndexStride {
				var pp []uint64
				if w.schema.HasNulls {
					pp = append(pp, w.present.GetPosition()...)
				}
				pp = append(pp, w.data.GetPosition()...)
				pp = append(pp, w.secondary.GetPosition()...)
				w.index.Entry = append(w.index.Entry, &pb.RowIndexEntry{Positions: pp, Statistics: w.indexStats})

				w.indexStats = &pb.ColumnStatistics{
					TimestampStatistics: &pb.TimestampStatistics{Maximum: new(int64), Minimum: new(int64), MaximumUtc: new(int64), MinimumUtc: new(int64)},
					NumberOfValues:      new(uint64), HasNull: new(bool)}
				if w.schema.HasNulls {
					*w.indexStats.HasNull = true
				}
				w.indexInRows = 0
			}

			if w.indexStats.TimestampStatistics == nil {
				w.indexStats.TimestampStatistics = &pb.TimestampStatistics{Maximum: new(int64), Minimum: new(int64), MaximumUtc: new(int64), MinimumUtc: new(int64)}
			}

			if ms > w.indexStats.TimestampStatistics.GetMaximum() {
				*w.indexStats.TimestampStatistics.Maximum = ms
			}
			if ms < w.indexStats.TimestampStatistics.GetMinimum() {
				*w.indexStats.TimestampStatistics.Minimum = ms
			}
			if msUtc > w.indexStats.TimestampStatistics.GetMaximumUtc() {
				*w.indexStats.TimestampStatistics.MaximumUtc = msUtc
			}
			if msUtc < w.indexStats.TimestampStatistics.GetMinimumUtc() {
				*w.indexStats.TimestampStatistics.Minimum = msUtc
			}

			*w.indexStats.NumberOfValues++
		}
	}

	return nil
}

func (w *timestampWriter) Flush() error {
	if w.schema.HasNulls {
		if err := w.present.Flush(); err != nil {
			return err
		}
		*w.stats.BytesOnDisk = w.present.Info().GetLength()
	}
	if err := w.data.Flush(); err != nil {
		return err
	}
	if err := w.secondary.Flush(); err != nil {
		return err
	}

	*w.stats.BytesOnDisk += w.data.Info().GetLength()
	*w.stats.BytesOnDisk += w.secondary.Info().GetLength()

	w.flushed = true
	return nil
}

func (w *timestampWriter) WriteOut(out io.Writer) (n int64, err error) {
	if !w.flushed {
		return 0, errors.New("not flushed")
	}
	var np, nd, ns int64
	if w.schema.HasNulls {
		if np, err = w.present.WriteOut(out); err != nil {
			return
		}
	}
	if nd, err = w.data.WriteOut(out); err != nil {
		return
	}
	if ns, err = w.secondary.WriteOut(out); err != nil {
		return
	}
	n = np + nd + ns
	return
}

func (w timestampWriter) GetStreamInfos() []*pb.Stream {
	if w.schema.HasNulls {
		return []*pb.Stream{w.present.Info(), w.data.Info(), w.secondary.Info()}
	}
	return []*pb.Stream{w.data.Info(), w.secondary.Info()}
}

func (w *timestampWriter) Reset() {
	w.writer.reset()
	w.data.Reset()
	w.secondary.Reset()
}

func (w timestampWriter) Size() int {
	if w.schema.HasNulls {
		return w.present.Size() + w.data.Size() + w.secondary.Size()
	}
	return w.data.Size() + w.secondary.Size()
}

func NewTimestampV2Reader(schema *api.TypeDescription, opts *config.ReaderOptions, f orcio.File, loc *time.Location) Reader {
	if loc == nil {
		loc = time.Local
	}
	return &TimestampV2Reader{reader: reader{schema: schema, opts: opts, f: f}, loc: loc}
}

type TimestampV2Reader struct {
	reader
	loc *time.Location

	present   *stream.BoolReader
	data      *stream.IntRLV2Reader
	secondary *stream.IntRLV2Reader
}

func (r *TimestampV2Reader) InitStream(info *pb.Stream, startOffset uint64) error {
	ic, err := r.f.Clone()
	if err != nil {
		return err
	}
	if _, err = ic.Seek(int64(startOffset), io.SeekStart); err != nil {
		return err
	}

	switch info.GetKind() {
	case pb.Stream_PRESENT:
		if !r.schema.HasNulls {
			return errors.New("schema has no nulls")
		}
		r.present = stream.NewBoolReader(r.opts, info, startOffset, ic)
	case pb.Stream_DATA:
		r.data = stream.NewIntRLV2Reader(r.opts, info, startOffset, true, ic)
	case pb.Stream_SECONDARY:
		r.secondary = stream.NewIntRLV2Reader(r.opts, info, startOffset, false, ic)
	default:
		return errors.New("stream kind error")
	}
	return nil
}

func (r *TimestampV2Reader) NextBatch(vec *api.ColumnVector) error {
	var err error
	for i := 0; i < len(vec.Vector); i++ {
		if r.present != nil {
			var p bool
			if p, err = r.present.Next(); err != nil {
				return err
			}
			vec.Vector[i].Null = !p
		}

		if !vec.Vector[i].Null {
			var s int64
			if s, err = r.data.NextInt64(); err != nil {
				return err
			}
			var ns uint64
			if ns, err = r.secondary.NextUInt64(); err != nil {
				return err
			}
			vec.Vector[i].V = api.Timestamp{Loc: r.loc, Seconds: s, Nanos: api.DecodingTimestampNanos(ns)}
		}
	}
	return nil
}

func (r *TimestampV2Reader) Skip(rows uint64) error {
	var err error
	p := true

	for i := 0; i < int(rows); i++ {
		if r.present != nil {
			if p, err = r.present.Next(); err != nil {
				return err
			}
		}

		if p {
			if _, err = r.data.NextInt64(); err != nil {
				return err
			}
			if _, err = r.secondary.NextUInt64(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *TimestampV2Reader) SeekStride(stride int) error {
	if stride == 0 {
		if r.present != nil {
			if err := r.present.Seek(0, 0, 0, 0); err != nil {
				return err
			}
		}
		if err := r.data.Seek(0, 0, 0); err != nil {
			return err
		}
		if err := r.secondary.Seek(0, 0, 0); err != nil {
			return err
		}
		return nil
	}

	var dataChunk, dataChunkOffset, dataOffset uint64
	var secondaryChunk, secondaryChunkOffset, secondaryOffset uint64

	pos, err := r.getStridePositions(stride)
	if err != nil {
		return err
	}

	if r.present != nil {
		var pChunk, pChunkOffset, pOffset1, pOffset2 uint64
		if r.opts.CompressionKind == pb.CompressionKind_NONE {
			pChunkOffset = pos[0]
			pOffset1 = pos[1]
			pOffset2 = pos[2]

			dataChunkOffset = pos[3]
			dataOffset = pos[4]
			secondaryChunkOffset = pos[5]
			secondaryOffset = pos[6]

		} else {
			pChunk = pos[0]
			pChunkOffset = pos[1]
			pOffset1 = pos[2]
			pOffset2 = pos[3]

			dataChunk = pos[4]
			dataChunkOffset = pos[5]
			dataOffset = pos[6]
			secondaryChunk = pos[7]
			secondaryChunkOffset = pos[8]
			secondaryOffset = pos[9]
		}

		if err = r.present.Seek(pChunk, pChunkOffset, pOffset1, pOffset2); err != nil {
			return err
		}

	} else {

		if r.opts.CompressionKind == pb.CompressionKind_NONE {
			dataChunkOffset = pos[0]
			dataOffset = pos[1]
			secondaryChunkOffset = pos[2]
			secondaryOffset = pos[3]

		} else {
			dataChunk = pos[0]
			dataChunkOffset = pos[1]
			dataOffset = pos[2]
			secondaryChunk = pos[3]
			secondaryChunkOffset = pos[4]
			secondaryOffset = pos[5]
		}
	}

	if err = r.data.Seek(dataChunk, dataChunkOffset, dataOffset); err != nil {
		return err
	}
	if err = r.secondary.Seek(secondaryChunk, secondaryChunkOffset, secondaryOffset); err != nil {
		return err
	}
	return nil
}

func (r *TimestampV2Reader) Close() {
	if r.present != nil {
		r.present.Close()
	}
	r.data.Close()
	r.secondary.Close()
}
