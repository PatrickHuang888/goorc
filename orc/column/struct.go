package column

import (
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	"github.com/patrickhuang888/goorc/orc/stream"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
	"io"
)

type structReader struct {
	*reader
}

func (c *structReader) InitStream(info *pb.Stream, startOffset uint64) error {
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

	return errors.New("struct column no stream other than present")
}

func (c *structReader) Next() (value api.Value, err error) {
	if err = c.checkInit(); err != nil {
		return
	}

	if c.schema.HasNulls {
		var p bool
		if p, err = c.present.Next(); err != nil {
			return
		}
		value.Null = !p
	}
	return
}

func (r *structReader) seek(indexEntry *pb.RowIndexEntry) error {
	if r.schema.HasNulls {
		return r.seekPresent(indexEntry)
	}
	return nil
}

func (r *structReader) Seek(rowNumber uint64) error {
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

func (r structReader) checkInit() error {
	if r.schema.HasNulls && r.present == nil {
		return errors.New("stream present not initialized!")
	}
	return nil
}

func (c *structReader) Close() {
	if c.schema.HasNulls {
		c.present.Close()
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
	if w.schema.HasNulls {
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

func (w structWriter) Reset() {
	if w.schema.HasNulls {
		w.reset()
	}
}

func (w structWriter) Size() int {
	if w.schema.HasNulls {
		return w.present.Size()
	}
	return 0
}
