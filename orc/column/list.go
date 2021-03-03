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

func NewListV2Reader(schema *api.TypeDescription, opts *config.ReaderOptions, f orcio.File) Reader {
	return &ListV2Reader{reader: reader{schema: schema, opts: opts, f: f}}
}

type ListV2Reader struct {
	reader

	present *stream.BoolReader
	length  *stream.IntRLV2Reader

	child Reader
}

func (r *ListV2Reader) InitStream(info *pb.Stream, startOffset uint64) error {
	f, err := r.f.Clone()
	if err != nil {
		return err
	}
	if _, err := f.Seek(int64(startOffset), io.SeekStart); err != nil {
		return err
	}

	if info.GetKind() == pb.Stream_PRESENT {
		r.present = stream.NewBoolReader(r.opts, info, startOffset, f)
		return nil
	}
	if info.GetKind() == pb.Stream_LENGTH {
		r.length = stream.NewIntRLV2Reader(r.opts, info, startOffset, false, f)
		return nil
	}
	return errors.New("struct column no stream other than present")
}

func (r *ListV2Reader) SetChild(child Reader) {
	r.child = child
}

func (r *ListV2Reader) NextBatch(vec *api.ColumnVector) error {
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
			v, err := r.length.NextUInt64()
			if err != nil {
				return err
			}
			vec.Vector[i].V = v
		}

		// prepare vector for child
		vec.Children[0].Vector = vec.Children[0].Vector[:0]
		if vec.Vector[i].Null {
			vec.Children[0].Vector = append(vec.Children[0].Vector, api.Value{Null: true})
		} else {
			l := vec.Vector[i].V.(uint64)
			for j := 0; j < int(l); j++ {
				vec.Children[0].Vector = append(vec.Children[0].Vector, api.Value{})
			}
		}

		if err := r.child.NextBatch(vec.Children[0]); err != nil {
			return err
		}
	}
	return nil
}

func (r *ListV2Reader) Skip(rows uint64) error {
	var err error
	p := true

	for i := 0; i < int(rows); i++ {
		if r.present != nil {
			if p, err = r.present.Next(); err != nil {
				return err
			}
		}

		if p {
			l, err := r.length.NextUInt64()
			if err != nil {
				return err
			}
			if err = r.child.Skip(l); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *ListV2Reader) SeekStride(stride int) error {
	if stride == 0 {
		if r.present != nil {
			if err := r.present.Seek(0, 0, 0, 0); err != nil {
				return err
			}
		}
		if err := r.length.Seek(0, 0, 0); err != nil {
			return err
		}

		if err := r.child.SeekStride(0); err != nil {
			return err
		}
		return nil
	}

	var lengthChunk, lengthChunkOffset, lengthOffset uint64

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

			lengthChunkOffset = pos[3]
			lengthOffset = pos[4]

		} else {
			pChunk = pos[0]
			pChunkOffset = pos[1]
			pOffset1 = pos[2]
			pOffset2 = pos[3]

			lengthChunk = pos[4]
			lengthChunkOffset = pos[5]
			lengthOffset = pos[6]
		}
		if err = r.present.Seek(pChunk, pChunkOffset, pOffset1, pOffset2); err != nil {
			return err
		}

	} else {

		if r.opts.CompressionKind == pb.CompressionKind_NONE {
			lengthChunkOffset = pos[0]
			lengthOffset = pos[1]
		} else {
			lengthChunk = pos[0]
			lengthChunkOffset = pos[1]
			lengthOffset = pos[2]
		}
	}

	if err := r.length.Seek(lengthChunk, lengthChunkOffset, lengthOffset); err != nil {
		return err
	}

	if err := r.child.SeekStride(stride); err != nil {
		return err
	}
	return nil
}

func (r *ListV2Reader) Close() {
	if r.present != nil {
		r.present.Close()
	}
	r.length.Close()
}

func NewListV2Writer(schema *api.TypeDescription, opts *config.WriterOptions) Writer {
	stats := &pb.ColumnStatistics{NumberOfValues: new(uint64), BytesOnDisk: new(uint64), HasNull: new(bool)}
	var present stream.Writer
	if schema.HasNulls {
		*stats.HasNull = true
		present = stream.NewBoolWriter(schema.Id, pb.Stream_PRESENT, opts)
	}
	var indexStats *pb.ColumnStatistics
	var index *pb.RowIndex
	if opts.WriteIndex {
		indexStats = &pb.ColumnStatistics{HasNull: new(bool), NumberOfValues: new(uint64), BytesOnDisk: new(uint64)}
		if schema.HasNulls {
			*indexStats.HasNull = true
		}
		index = &pb.RowIndex{}
	}
	return &structWriter{&writer{schema: schema, opts: opts, present: present, indexStats: indexStats, stats: stats, index: index}}
}

type listV2Writer struct {
	*writer
	length stream.Writer
}

func (w *listV2Writer) Write(value api.Value) error {
	if w.schema.HasNulls {
		if err := w.present.Write(!value.Null); err != nil {
			return err
		}
	}

	if !value.Null {
		if err := w.length.Write(value.V); err != nil {
			return err
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
			pp = append(pp, w.length.GetPosition()...)
			w.index.Entry = append(w.index.Entry, &pb.RowIndexEntry{Positions: pp, Statistics: w.indexStats})

			// new stats
			w.indexStats = &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64)}
			if w.schema.HasNulls {
				*w.indexStats.HasNull = true
			}
			w.indexInRows = 0
		}
		if !value.Null {
			*w.indexStats.NumberOfValues++
		}
	}
	return nil
}

func (w *listV2Writer) Flush() error {
	w.flushed = true

	if w.schema.HasNulls {
		if err := w.present.Flush(); err != nil {
			return err
		}
	}
	if err := w.length.Flush(); err != nil {
		return err
	}

	if w.schema.HasNulls {
		*w.stats.BytesOnDisk = w.present.Info().GetLength()
	}
	*w.stats.BytesOnDisk += w.length.Info().GetLength()
	return nil
}

func (w *listV2Writer) WriteOut(out io.Writer) (n int64, err error) {
	var pn int64
	if w.schema.HasNulls {
		var err error
		if pn, err = w.present.WriteOut(out); err != nil {
			return 0, err
		}
	}
	ln, err := w.length.WriteOut(out)
	if err != nil {
		return 0, err
	}
	return pn + ln, nil
}

func (w listV2Writer) GetStreamInfos() []*pb.Stream {
	if w.schema.HasNulls {
		return []*pb.Stream{w.present.Info(), w.length.Info()}
	}
	return []*pb.Stream{w.length.Info()}
}

func (w *listV2Writer) Reset() {
	w.writer.reset()
	w.length.Reset()
}

func (w listV2Writer) Size() int {
	if w.schema.HasNulls {
		return w.present.Size() + w.length.Size()
	}
	return w.length.Size()
}
