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

func (w *intWriter) Reset() {
	w.reset()
	w.data.Reset()
}

func NewIntV2Reader(schema *api.TypeDescription, opts *config.ReaderOptions, f orcio.File, bits int) Reader {
	return &IntV2Reader{reader: reader{f: f, schema: schema, opts: opts}, bits: bits}
}

type IntV2Reader struct {
	reader

	present *stream.BoolReader
	data    *stream.IntRLV2Reader
	bits    int
}

func (r *IntV2Reader) InitStream(info *pb.Stream, startOffset uint64) error {
	f, err := r.f.Clone()
	if err != nil {
		return err
	}
	if _, err := f.Seek(int64(startOffset), io.SeekStart); err != nil {
		return err
	}

	if r.schema.Encoding == pb.ColumnEncoding_DIRECT {
		err := errors.New("int reader encoding direct not impl")
		return err
	}

	if info.GetKind() == pb.Stream_PRESENT {
		r.present = stream.NewBoolReader(r.opts, info, startOffset, f)
		return nil
	}
	if info.GetKind() == pb.Stream_DATA {
		r.data = stream.NewIntRLV2Reader(r.opts, info, startOffset, true, f)
		return nil
	}
	return errors.New("stream unknown")
}

func (r *IntV2Reader) NextBatch(vec *api.ColumnVector) error {
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
			var v int64
			if v, err = r.data.NextInt64(); err != nil {
				return err
			}
			switch r.bits {
			case BitsSmallInt:
				vec.Vector[i].V = int16(v)
			case BitsInt:
				vec.Vector[i].V = int32(v)
			case BitsBigInt:
				vec.Vector[i].V = v
			default:
				err = errors.New("reader bits error")
			}
		}
	}
	return nil
}

func (r *IntV2Reader) Skip(rows uint64) error {
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
		}
	}
	return nil
}

func (r *IntV2Reader) SeekStride(stride int) error {
	if stride == 0 {
		if r.present != nil {
			if err := r.present.Seek(0, 0, 0, 0); err != nil {
				return err
			}
		}
		if err := r.data.Seek(0, 0, 0); err != nil {
			return err
		}
		return nil
	}

	var dataChunk, dataChunkOffset, dataOffset uint64

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

		} else {
			pChunk = pos[0]
			pChunkOffset = pos[1]
			pOffset1 = pos[2]
			pOffset2 = pos[3]

			dataChunk = pos[4]
			dataChunkOffset = pos[5]
			dataOffset = pos[6]
		}

		if err = r.present.Seek(pChunk, pChunkOffset, pOffset1, pOffset2); err != nil {
			return err
		}

	} else {

		if r.opts.CompressionKind == pb.CompressionKind_NONE {
			dataChunkOffset = pos[0]
			dataOffset = pos[1]
		} else {
			dataChunk = pos[0]
			dataChunkOffset = pos[1]
			dataOffset = pos[2]
		}
	}

	return r.data.Seek(dataChunk, dataChunkOffset, dataOffset)
}

func (r *IntV2Reader) Close() {
	if r.present != nil {
		r.present.Close()
	}
	r.data.Close()
}
