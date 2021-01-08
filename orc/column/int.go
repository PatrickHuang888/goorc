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

func newIntV2Writer(schema *api.TypeDescription, opts *config.WriterOptions, bits int) Writer {
	stats := &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64),
		IntStatistics: &pb.IntegerStatistics{Minimum: new(int64), Maximum: new(int64), Sum: new(int64)}}
	var present stream.Writer
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
	base := &writer{opts: opts, schema: schema, stats: stats, present: present, index: index, indexStats: indexStats}
	data := stream.NewIntRLV2Writer(schema.Id, pb.Stream_DATA, opts, true)
	return &intWriter{base, data, bits}
}

const (
	BitsSmallInt = 16
	BitsInt      = 32
	BitsBigInt   = 64
)

type intWriter struct {
	*writer
	data stream.Writer
	bits int
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

	var v int64
	if !value.Null {
		switch w.bits {
		case BitsSmallInt:
			v = int64(value.V.(int16))
		case BitsInt:
			v = int64(value.V.(int32))
		case BitsBigInt:
			v = value.V.(int64)
		}

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

func NewIntV2Reader(schema *api.TypeDescription, opts *config.ReaderOptions, f orcio.File, bits int) Reader {
	return &intV2Reader{reader: &reader{f: f, schema: schema, opts: opts}, bits: bits}
}

type intV2Reader struct {
	*reader
	data *stream.IntRLV2Reader
	bits int
}

func (r *intV2Reader) InitStream(info *pb.Stream, startOffset uint64) error {

	if r.schema.Encoding == pb.ColumnEncoding_DIRECT {
		err := errors.New("int reader encoding direct not impl")
		return err
	}

	if info.GetKind() == pb.Stream_PRESENT {
		ic, err := r.f.Clone()
		if err != nil {
			return err
		}
		if _, err := ic.Seek(int64(startOffset), io.SeekStart); err != nil {
			return err
		}
		r.present = stream.NewBoolReader(r.opts, info, startOffset, ic)
		return nil
	}

	if info.GetKind() == pb.Stream_DATA {
		ic, err := r.f.Clone()
		if err != nil {
			return err
		}
		if _, err := ic.Seek(int64(startOffset), io.SeekStart); err != nil {
			return err
		}
		r.data = stream.NewIntRLV2Reader(r.opts, info, startOffset, true, ic)
		return nil
	}
	return errors.New("stream unknown")
}

func (r *intV2Reader) Next() (value api.Value, err error) {
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
		var v int64
		if v, err = r.data.NextInt64(); err != nil {
			return
		}
		switch r.bits {
		case BitsSmallInt:
			value.V = int16(v)
		case BitsInt:
			value.V = int32(v)
		case BitsBigInt:
			value.V = v
		default:
			err = errors.New("reader bits error")
		}
	}
	return
}

func (r *intV2Reader) NextBatch(batch *api.ColumnVector) error {
	var err error
	if err = r.checkInit(); err != nil {
		return err
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
			var v int64
			if v, err = r.data.NextInt64(); err != nil {
				return err
			}
			switch r.bits {
			case BitsSmallInt:
				batch.Vector[i].V = int16(v)
			case BitsInt:
				batch.Vector[i].V = int32(v)
			case BitsBigInt:
				batch.Vector[i].V = v
			default:
				err = errors.New("reader bits error")
			}
		}
	}
	return nil
}

func (r intV2Reader) checkInit() error {
	if r.data == nil {
		return errors.New("stream data not initialized!")
	}
	if r.schema.HasNulls && r.present == nil {
		return errors.New("stream present not initialized!")
	}
	return nil
}

func (r *intV2Reader) seek(indexEntry *pb.RowIndexEntry) error {
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

func (r *intV2Reader) Seek(rowNumber uint64) error {
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

func (r *intV2Reader) Close() {
	if r.schema.HasNulls {
		r.present.Close()
	}
	r.data.Close()
}
