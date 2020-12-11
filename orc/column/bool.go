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
	var present *stream.Writer
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
			*indexStats.HasNull= true
		}
		index = &pb.RowIndex{}
	}
	base := &writer{schema: schema, opts: opts, present: present, stats: stats, indexStats: indexStats, index: index}
	data := stream.NewBoolWriter(schema.Id, pb.Stream_DATA, opts)
	return &boolWriter{base, data}
}

type boolWriter struct {
	*writer
	data *stream.Writer
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

func newBoolReader(schema *api.TypeDescription, opts *config.ReaderOptions, f orcio.File) Reader {
	return &boolReader{reader: &reader{opts: opts, schema: schema, f: f}}
}

type boolReader struct {
	*reader
	data *stream.BoolReader
}

func (r *boolReader) InitStream(info *pb.Stream, startOffset uint64) error {
	if info.GetKind() == pb.Stream_PRESENT {
		if !r.schema.HasNulls {
			return errors.New("column schema has no nulls")
		}
		ic, err := r.f.Clone()
		if err != nil {
			return err
		}
		r.present = stream.NewBoolReader(r.opts, info, startOffset, ic)
		ic.Seek(int64(startOffset), 0)
		return nil
	}
	if info.GetKind() == pb.Stream_DATA {
		ic, err := r.f.Clone()
		if err != nil {
			return err
		}
		r.data = stream.NewBoolReader(r.opts, info, startOffset, ic)
		ic.Seek(int64(startOffset), 0)
		return nil
	}
	return errors.New("stream kind error")
}

func (r *boolReader) Next() (value api.Value, err error) {
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

func (r *boolReader) Seek(rowNumber uint64) error {
	if !r.opts.HasIndex {
		return errors.New("no index")
	}

	var offset uint64
	if rowNumber < uint64(r.opts.IndexStride) {
		// from start
		if err := r.seek(nil); err != nil {
			return err
		}
		offset = rowNumber
	} else {
		stride := rowNumber / uint64(r.opts.IndexStride)
		offset = rowNumber % (stride * uint64(r.opts.IndexStride))
		if err := r.seek(r.index.GetEntry()[stride-1]); err != nil {
			return err
		}
	}

	for i := 0; i < int(offset); i++ {
		if _, err := r.Next(); err != nil {
			return err
		}
	}
	return nil
}

func (r *boolReader) seek(indexEntry *pb.RowIndexEntry) error {
	if err := r.checkInit(); err != nil {
		return err
	}

	// from start
	if indexEntry == nil {
		if r.schema.HasNulls {
			if err := r.present.Seek(0, 0, 0, 0); err != nil {
				return err
			}
		}
		if err := r.data.Seek(0, 0, 0, 0); err != nil {
			return err
		}
		return nil
	}

	pos := indexEntry.GetPositions()

	if !r.schema.HasNulls { // no present
		if r.opts.CompressionKind == pb.CompressionKind_NONE {
			return r.data.Seek(pos[0], 0, pos[1], pos[2])
		}
		return r.data.Seek(pos[0], pos[1], pos[2], pos[3])
	}

	if r.opts.CompressionKind == pb.CompressionKind_NONE {
		if err := r.present.Seek(pos[0], 0, pos[1], pos[2]); err != nil {
			return err
		}
		if err := r.data.Seek(pos[3], 0, pos[4], pos[5]); err != nil {
			return err
		}
		return nil
	}

	if err := r.present.Seek(pos[0], pos[1], pos[2], pos[3]); err != nil {
		return err
	}
	if err := r.data.Seek(pos[4], pos[5], pos[6], pos[7]); err != nil {
		return err
	}
	return nil
}

func (r *boolReader) Close() {
	if r.schema.HasNulls {
		r.present.Close()
	}
	r.data.Close()
}

func (r boolReader) checkInit() error {
	if r.data == nil {
		return errors.New("stream data not initialized!")
	}
	if r.schema.HasNulls && r.present == nil {
		return errors.New("stream present not initialized!")
	}
	return nil
}