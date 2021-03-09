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

func newBoolWriter(schema *api.TypeDescription, opts *config.WriterOptions) Writer {
	stats := &pb.ColumnStatistics{BucketStatistics: &pb.BucketStatistics{Count: make([]uint64, 1)},
		NumberOfValues: new(uint64), BytesOnDisk: new(uint64), HasNull: new(bool)}
	var present stream.Writer
	if schema.HasNulls {
		present = stream.NewBoolWriter(schema.Id, pb.Stream_PRESENT, opts)
		*stats.HasNull = true
	}
	var indexStats *pb.ColumnStatistics
	var index *pb.RowIndex
	if opts.WriteIndex {
		indexStats = &pb.ColumnStatistics{BucketStatistics: &pb.BucketStatistics{Count: make([]uint64, 1)},
			HasNull: new(bool), NumberOfValues: new(uint64), BytesOnDisk: new(uint64)}
		if schema.HasNulls {
			*indexStats.HasNull = true
		}
		index = &pb.RowIndex{}
	}
	base := &writer{schema: schema, opts: opts, present: present, stats: stats, indexStats: indexStats, index: index}
	data := stream.NewBoolWriter(schema.Id, pb.Stream_DATA, opts)
	return &boolWriter{base, data}
}

type boolWriter struct {
	*writer
	data stream.Writer
}

func (w *boolWriter) Write(value api.Value) error {
	if w.schema.HasNulls {
		if err := w.present.Write(!value.Null); err != nil {
			return err
		}
	}
	if !value.Null {
		if err := w.data.Write(value.V); err != nil {
			return err
		}
		if value.V.(bool) {
			// true count
			(*w.stats.BucketStatistics).Count[0]++
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
			w.indexStats = &pb.ColumnStatistics{BucketStatistics: &pb.BucketStatistics{Count: make([]uint64, 1)},
				NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64)}
			if w.schema.HasNulls {
				*w.indexStats.HasNull = true
			}
			w.indexInRows = 0
		}
		// no bytes on disk index stats
		if !value.Null {
			(*w.indexStats.BucketStatistics).Count[0]++
			*w.indexStats.NumberOfValues++
		}
	}
	return nil
}

func (w *boolWriter) Flush() error {
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

func (w *boolWriter) WriteOut(out io.Writer) (n int64, err error) {
	if !w.flushed {
		err = errors.New("not flushed!")
		return
	}

	var pn, dn int64
	if w.schema.HasNulls {
		if pn, err = w.present.WriteOut(out); err != nil {
			return 0, err
		}
	}
	if dn, err = w.data.WriteOut(out); err != nil {
		return 0, err
	}
	return pn + dn, nil
}

func (w boolWriter) GetStreamInfos() []*pb.Stream {
	if w.schema.HasNulls {
		return []*pb.Stream{w.present.Info(), w.data.Info()}
	}
	return []*pb.Stream{w.data.Info()}
}

func (w *boolWriter) Reset() {
	w.reset()
	w.data.Reset()
}

func (w boolWriter) Size() int {
	if w.schema.HasNulls {
		return w.present.Size() + w.data.Size()
	}
	return w.data.Size()
}

func NewBoolReader(schema *api.TypeDescription, opts *config.ReaderOptions, f orcio.File) Reader {
	return &BoolReader{reader: reader{opts: opts, schema: schema, f: f}}
}

type BoolReader struct {
	reader

	present *stream.BoolReader
	data    *stream.BoolReader
}

func (r *BoolReader) InitStream(info *pb.Stream, startOffset uint64) error {
	f, err := r.f.Clone()
	if err != nil {
		return err
	}
	if _, err = f.Seek(int64(startOffset), io.SeekStart); err != nil {
		return err
	}

	if info.GetKind() == pb.Stream_PRESENT {
		if !r.schema.HasNulls {
			return errors.New("column schema has no nulls")
		}
		r.present = stream.NewBoolReader(r.opts, info, startOffset, f)
		return nil
	}
	if info.GetKind() == pb.Stream_DATA {
		r.data = stream.NewBoolReader(r.opts, info, startOffset, f)
		return nil
	}
	return errors.New("stream kind error")
}

func (r *BoolReader) NextBatch(vec *api.ColumnVector) error {
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

func (r *BoolReader) Skip(rows uint64) error {
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

func (r *BoolReader) SeekStride(stride int) error {
	if stride == 0 {
		if r.present != nil {
			if err := r.present.Seek(0, 0, 0, 0); err != nil {
				return err
			}
		}
		if err := r.data.Seek(0, 0, 0, 0); err != nil {
			return err
		}
		return nil
	}

	var dataChunk, dataChunkOffset, dataOffset1, dataOffset2 uint64

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
			dataOffset1 = pos[4]
			dataOffset2 = pos[5]

		} else {
			pChunk = pos[0]
			pChunkOffset = pos[1]
			pOffset1 = pos[2]
			pOffset2 = pos[3]

			dataChunk = pos[4]
			dataChunkOffset = pos[5]
			dataOffset1 = pos[6]
			dataOffset2 = pos[7]
		}

		if err = r.present.Seek(pChunk, pChunkOffset, pOffset1, pOffset2); err != nil {
			return err
		}

	} else {

		if r.opts.CompressionKind == pb.CompressionKind_NONE {
			dataChunkOffset = pos[0]
			dataOffset1 = pos[1]
			dataOffset2 = pos[2]
		} else {
			dataChunk = pos[0]
			dataChunkOffset = pos[1]
			dataOffset1 = pos[2]
			dataOffset2 = pos[3]
		}
	}

	return r.data.Seek(dataChunk, dataChunkOffset, dataOffset1, dataOffset2)
}

func (r *BoolReader) Close() {
	if r.present != nil {
		r.present.Close()
	}
	r.data.Close()
}
