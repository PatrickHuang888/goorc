package column

import (
	"io"

	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/orc/stream"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
)

func NewFloatReader(schema *api.TypeDescription, opts *config.ReaderOptions, f orcio.File, is64 bool) Reader {
	return &floatReader{reader: &reader{f: f, schema: schema, opts: opts}, is64: is64}
}

type floatReader struct {
	*reader
	data *stream.FloatReader
	is64 bool
}

func (r *floatReader) InitStream(info *pb.Stream, startOffset uint64) error {
	ic, err := r.f.Clone()
	if err != nil {
		return err
	}
	if _, err = ic.Seek(int64(startOffset), io.SeekStart); err != nil {
		return err
	}

	switch info.GetKind() {
	case pb.Stream_PRESENT:
		r.present = stream.NewBoolReader(r.opts, info, startOffset, ic)
	case pb.Stream_DATA:
		r.data = stream.NewFloatReader(r.opts, info, startOffset, ic, r.is64)
	default:
		return errors.New("stream kind error")
	}
	return nil
}

func (r *floatReader) Next() (value api.Value, err error) {
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
		value.V, err = r.data.NextFloat()
		if err != nil {
			return
		}
	}
	return
}

func (r *floatReader) NextBatch(batch *api.ColumnVector) error {
	var err error
	if err = r.checkInit(); err != nil {
		return err
	}

	if r.schema.Id != batch.Id {
		return errors.New("column error")
	}

	for i := 0; i < len(batch.Vector); i++ {
		if r.schema.HasNulls {
			var p bool
			if p, err = r.present.Next(); err != nil {
				return err
			}
			batch.Vector[i].Null = !p
		}

		if !batch.Vector[i].Null {
			batch.Vector[i].V, err = r.data.NextFloat()
			if err != nil {
				return err
			}
		}
	}
	return err
}

func (r *floatReader) Seek(rowNumber uint64) error {
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

func (r *floatReader) seek(indexEntry *pb.RowIndexEntry) error {
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

func (r floatReader) checkInit() error {
	if r.data == nil {
		return errors.New("stream data not initialized!")
	}
	if r.schema.HasNulls && r.present == nil {
		return errors.New("stream present not initialized!")
	}
	return nil
}

func (r *floatReader) Close() {
	if r.schema.HasNulls {
		r.present.Close()
	}
	r.data.Close()
}

func newFloatWriter(schema *api.TypeDescription, opts *config.WriterOptions, is64 bool) Writer {
	stats := &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64),
		DoubleStatistics: &pb.DoubleStatistics{Sum: new(float64), Maximum: new(float64), Minimum: new(float64)}}
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
	base := &writer{schema: schema, opts: opts, present: present, indexStats: indexStats, index: index, stats: stats}
	data := stream.NewFloatWriter(schema.Id, pb.Stream_DATA, opts, is64)
	return &floatWriter{base, data, is64}
}

type floatWriter struct {
	*writer
	data stream.Writer
	is64 bool
}

func (w *floatWriter) Write(value api.Value) error {
	if w.schema.HasNulls {
		if err := w.present.Write(!value.Null); err != nil {
			return err
		}
	}

	if !value.Null {
		if err := w.data.Write(value.V); err != nil {
			return err
		}
		var v float64
		if w.is64 {
			v = value.V.(float64)
		} else {
			v = float64(value.V.(float32))
		}

		*w.stats.DoubleStatistics.Sum += v
		*w.stats.NumberOfValues++
		if v < *w.stats.DoubleStatistics.Minimum {
			*w.stats.DoubleStatistics.Minimum = v
		}
		if v > *w.stats.DoubleStatistics.Maximum {
			*w.stats.DoubleStatistics.Maximum = v
		}
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
			w.indexStats = &pb.ColumnStatistics{DoubleStatistics: &pb.DoubleStatistics{Sum: new(float64), Maximum: new(float64), Minimum: new(float64)},
				NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64)}
			if w.schema.HasNulls {
				*w.indexStats.HasNull = true
			}
			w.indexInRows = 0
		}
		if !value.Null {
			v := value.V.(float64)
			*w.indexStats.DoubleStatistics.Sum += v
			if v < *w.indexStats.DoubleStatistics.Minimum {
				*w.indexStats.DoubleStatistics.Minimum = v
			}
			if v > *w.indexStats.DoubleStatistics.Maximum {
				*w.indexStats.DoubleStatistics.Maximum = v
			}

			*w.indexStats.NumberOfValues++
		}
	}
	return nil
}

func (w *floatWriter) Flush() error {
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
	return nil
}

func (w *floatWriter) WriteOut(out io.Writer) (n int64, err error) {
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

func (w floatWriter) GetStreamInfos() []*pb.Stream {
	if w.schema.HasNulls {
		return []*pb.Stream{w.present.Info(), w.data.Info()}
	}
	return []*pb.Stream{w.data.Info()}
}

func (w *floatWriter) Reset() {
	w.reset()
	w.data.Reset()
}

func (w floatWriter) Size() int {
	if w.schema.HasNulls {
		return w.present.Size() + w.data.Size()
	}
	return w.data.Size()
}
