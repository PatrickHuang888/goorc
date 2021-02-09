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
	return &listV2Reader{reader: &reader{schema: schema, opts: opts, f: f}}
}

func NewMapV2Reader(schema *api.TypeDescription, opts *config.ReaderOptions, f orcio.File) Reader {
	return &listV2Reader{reader: &reader{schema: schema, opts: opts, f: f}}
}

type listV2Reader struct {
	*reader
	length *stream.IntRLV2Reader

	child Reader
}

func (r *listV2Reader) InitStream(info *pb.Stream, startOffset uint64) error {
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

func (r *listV2Reader) Next() (value api.Value, err error) {
	if r.schema.HasNulls {
		var p bool
		if p, err = r.present.Next(); err != nil {
			return
		}
		value.Null = !p
	}

	if !value.Null {
		if value.V, err = r.length.NextUInt64(); err != nil {
			return
		}
	}
	return
}

func (r *listV2Reader) NextBatch(vec *api.ColumnVector) error {
	var err error

	for i := 0; i < len(vec.Vector); i++ {

		if r.schema.HasNulls {
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
			vec.Vector[i].V= int(v)
		}

		if vec.Vector[i].Null {
			vec.Children[0].Vector= append(vec.Children[0].Vector, api.Value{Null: true})
		}else {
			for j:=0; j<vec.Vector[i].V.(int);j++ {
				v, err := r.child.Next()
				if err != nil {
					return err
				}
				vec.Children[0].Vector = append(vec.Children[0].Vector, v)
			}
		}

	}
	return nil
}

func (r *listV2Reader) Seek(rowNumber uint64) error {
	entry, offset, err := r.reader.getIndexEntryAndOffset(rowNumber)
	if err!=nil {
		return err
	}

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

func (r *listV2Reader) seek(indexEntry *pb.RowIndexEntry) error {
	if r.schema.HasNulls {
		if err := r.seekPresent(indexEntry); err != nil {
			return err
		}
	}

	var lengthChunk, lengthChunkOffset, lengthOffset uint64
	if indexEntry != nil {
		if r.opts.CompressionKind == pb.CompressionKind_NONE {
			if r.schema.HasNulls {
				// has nulls no compression
				lengthChunkOffset = indexEntry.Positions[3]
				lengthOffset = indexEntry.Positions[4]
			} else {
				// no nulls, no compression
				lengthChunkOffset = indexEntry.Positions[0]
				lengthOffset = indexEntry.Positions[1]
			}

		} else {
			if r.schema.HasNulls {
				// has nulls, compression
				lengthChunk = indexEntry.Positions[4]
				lengthChunkOffset = indexEntry.Positions[5]
				lengthOffset = indexEntry.Positions[6]
			} else {
				// no nulls, compression
				lengthChunk = indexEntry.Positions[0]
				lengthChunkOffset = indexEntry.Positions[1]
				lengthOffset = indexEntry.Positions[2]
			}
		}
	}
	if err := r.length.Seek(lengthChunk, lengthChunkOffset, lengthOffset); err != nil {
		return err
	}
	return nil
}

func (r *listV2Reader) Close() {
	if r.schema.HasNulls {
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
