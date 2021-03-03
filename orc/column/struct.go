package column

import (
	"io"

	"github.com/pkg/errors"

	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/orc/stream"
	"github.com/patrickhuang888/goorc/pb/pb"
)

func NewStructReader(schema *api.TypeDescription, opts *config.ReaderOptions, f orcio.File) Reader {
	return &StructReader{reader: reader{schema: schema, opts: opts, f: f}}
}

type StructReader struct {
	reader

	present *stream.BoolReader

	children []Reader
}

func (r *StructReader) AddChild(child Reader) {
	r.children = append(r.children, child)
}

func (r *StructReader) InitStream(info *pb.Stream, startOffset uint64) error {
	f, err := r.f.Clone()
	if err != nil {
		return err
	}
	if _, err := f.Seek(int64(startOffset), io.SeekStart); err != nil {
		return err
	}

	if info.GetKind() == pb.Stream_PRESENT {
		if !r.schema.HasNulls {
			return errors.New("should no present")
		}
		r.present = stream.NewBoolReader(r.opts, info, startOffset, f)
		return nil
	}
	return errors.New("struct column no stream other than present")
}

func (r *StructReader) SeekStride(stride int) error {
	if stride == 0 {
		if r.present != nil {
			if err := r.present.Seek(0, 0, 0, 0); err != nil {
				return err
			}
		}
		for _, c := range r.children {
			if err := c.SeekStride(0); err != nil {
				return err
			}
		}
		return nil
	}

	if r.present != nil {
		var pChunk, pChunkOffset, pOffset1, pOffset2 uint64

		pos, err := r.getStridePositions(stride)
		if err != nil {
			return err
		}

		if r.opts.CompressionKind == pb.CompressionKind_NONE {
			pChunkOffset = pos[0]
			pOffset1 = pos[1]
			pOffset2 = pos[2]
		} else {
			pChunk = pos[0]
			pChunkOffset = pos[1]
			pOffset1 = pos[2]
			pOffset2 = pos[3]
		}

		if err := r.present.Seek(pChunk, pChunkOffset, pOffset1, pOffset2); err != nil {
			return err
		}
	}

	for _, c := range r.children {
		if err := c.SeekStride(stride); err != nil {
			return err
		}
	}
	return nil
}

func (r *StructReader) Skip(rows uint64) error {
	var err error
	p := true

	for i := 0; i < int(rows); i++ {
		if r.present != nil {
			if p, err = r.present.Next(); err != nil {
				return err
			}
		}

		for _, c := range r.children {
			if p {
				if err := c.Skip(1); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (r *StructReader) NextBatch(vec *api.ColumnVector) error {
	if r.present != nil {
		for i := 0; i < len(vec.Vector); i++ {
			p, err := r.present.Next()
			if err != nil {
				return err
			}
			vec.Vector[i].Null = !p
		}
		fillNulls(vec)
	}

	for i, c := range r.children {
		if err := c.NextBatch(vec.Children[i]); err != nil {
			return err
		}
	}
	return nil
}

func fillNulls(vec *api.ColumnVector) {
	for _, c := range vec.Children {
		for i, v := range vec.Vector {
			c.Vector[i].Null = v.Null
		}
		fillNulls(c)
	}
}

func (r *StructReader) Close() {
	if r.present != nil {
		r.present.Close()
	}

	for _, c := range r.children {
		c.Close()
	}
}

func newStructWriter(schema *api.TypeDescription, opts *config.WriterOptions) Writer {
	stats := &pb.ColumnStatistics{BucketStatistics: &pb.BucketStatistics{Count: make([]uint64, 1)},
		NumberOfValues: new(uint64), BytesOnDisk: new(uint64), HasNull: new(bool)}
	var present stream.Writer
	if schema.HasNulls {
		*stats.HasNull = true
		present = stream.NewBoolWriter(schema.Id, pb.Stream_PRESENT, opts)
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
	return &structWriter{&writer{schema: schema, opts: opts, present: present, indexStats: indexStats, stats: stats, index: index}}
}

type structWriter struct {
	*writer
}

func (w *structWriter) Write(value api.Value) error {
	if !w.schema.HasNulls {
		return nil
	}

	if err := w.present.Write(!value.Null); err != nil {
		return err
	}
	if !value.Null {
		(*w.stats.BucketStatistics).Count[0]++
	}
	*w.stats.NumberOfValues++

	if w.opts.WriteIndex {
		w.indexInRows++
		if w.indexInRows >= w.opts.IndexStride {
			var pp []uint64
			if w.schema.HasNulls {
				pp = append(pp, w.present.GetPosition()...)
			}
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
		}
		*w.indexStats.NumberOfValues++
	}
	return nil
}

func (w *structWriter) Flush() error {
	if w.schema.HasNulls {
		if err := w.present.Flush(); err != nil {
			return err
		}
		*w.stats.BytesOnDisk = w.present.Info().GetLength()
	}
	w.flushed = true
	return nil
}

func (w *structWriter) WriteOut(out io.Writer) (n int64, err error) {
	if !w.flushed {
		err = errors.New("not flushed")
		return
	}

	var pn int64
	if w.schema.HasNulls {
		if pn, err = w.present.WriteOut(out); err != nil {
			return 0, err
		}
	}
	return pn, nil
}

func (w structWriter) GetStreamInfos() []*pb.Stream {
	if w.schema.HasNulls {
		return []*pb.Stream{w.present.Info()}
	}
	return nil
}

func (w *structWriter) Reset() {
	w.reset()
}

func (w structWriter) Size() int {
	if w.schema.HasNulls {
		return w.present.Size()
	}
	return 0
}
