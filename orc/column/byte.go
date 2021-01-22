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

func newByteWriter(schema *api.TypeDescription, opts *config.WriterOptions) Writer {
	stats := &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64), BinaryStatistics: &pb.BinaryStatistics{Sum: new(int64)}}
	var present stream.Writer
	if schema.HasNulls {
		*stats.HasNull = true
		present = stream.NewBoolWriter(schema.Id, pb.Stream_PRESENT, opts)
	}
	var indexStats *pb.ColumnStatistics
	var index *pb.RowIndex
	if opts.WriteIndex {
		indexStats = &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64), BinaryStatistics: &pb.BinaryStatistics{Sum: new(int64)}}
		index = &pb.RowIndex{}
	}
	base := &writer{schema: schema, opts: opts, stats: stats, present: present, indexStats: indexStats, index: index}
	data := stream.NewByteWriter(schema.Id, pb.Stream_DATA, opts)
	return &byteWriter{base, data}
}

type byteWriter struct {
	*writer
	data stream.Writer
}

func (w *byteWriter) Write(value api.Value) error {
	if w.schema.HasNulls {
		if err := w.present.Write(!value.Null); err != nil {
			return err
		}
	}

	if !value.Null {
		if err := w.data.Write(value.V); err != nil {
			return err
		}
		*w.stats.BinaryStatistics.Sum++
		*w.stats.NumberOfValues++ // makeSure:
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
			w.indexStats = &pb.ColumnStatistics{BinaryStatistics: &pb.BinaryStatistics{Sum: new(int64)}, NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64)}
			if w.schema.HasNulls {
				*w.indexStats.HasNull = true
			}
			w.indexInRows = 0
		}
		// java impl does not write index statistic bytes on disk, has nulls ...
		if !value.Null {
			*w.indexStats.BinaryStatistics.Sum++
			*w.indexStats.NumberOfValues++
		}
	}
	return nil
}

func (w *byteWriter) Size() int {
	if w.schema.HasNulls {
		return w.present.Size() + w.data.Size()
	}
	return w.data.Size()
}

func (w *byteWriter) Flush() error {
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

func (w *byteWriter) GetStreamInfos() []*pb.Stream {
	if w.schema.HasNulls {
		return []*pb.Stream{w.present.Info(), w.data.Info()}
	}
	return []*pb.Stream{w.data.Info()}
}

func (w *byteWriter) Reset() {
	w.reset()
	w.data.Reset()
}

func (w *byteWriter) WriteOut(out io.Writer) (n int64, err error) {
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

type byteReader struct {
	*reader
	data *stream.ByteReader
}

func NewByteReader(schema *api.TypeDescription, opts *config.ReaderOptions, f orcio.File) Reader {
	return &byteReader{reader: &reader{opts: opts, schema: schema, f: f}}
}

func (r *byteReader) InitStream(info *pb.Stream, startOffset uint64) error {
	f, err := r.f.Clone()
	if err != nil {
		return err
	}
	if _, err = f.Seek(int64(startOffset), io.SeekStart); err != nil {
		return err
	}

	switch info.GetKind() {
	case pb.Stream_PRESENT:
		if !r.schema.HasNulls {
			return errors.New("column schema has no nulls")
		}
		r.present = stream.NewBoolReader(r.opts, info, startOffset, f)
	case pb.Stream_DATA:
		r.data = stream.NewByteReader(r.opts, info, startOffset, f)
	default:
		err = errors.New("stream kind error")
	}
	return nil
}

func (r *byteReader) Next() (value api.Value, err error) {
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

func (r *byteReader) NextBatch(vector []api.Value) error {
	var err error
	for i := 0; i < len(vector); i++ {
		if r.schema.HasNulls {
			var p bool
			if p, err = r.present.Next(); err != nil {
				return err
			}
			vector[i].Null = !p
		}
		if !vector[i].Null {
			if vector[i].V, err = r.data.Next(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *byteReader) seek(indexEntry *pb.RowIndexEntry) error {
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

func (r *byteReader) Seek(rowNumber uint64) error {
	entry, offset, err := r.reader.getIndexEntryAndOffset(rowNumber)
	if err != nil {
		return err
	}
	if err = r.seek(entry); err != nil {
		return err
	}
	for i := 0; i < int(offset); i++ {
		if _, err := r.Next(); err != nil {
			return err
		}
	}
	return nil
}

func (r *byteReader) Close() {
	if r.schema.HasNulls {
		r.present.Close()
	}
	r.data.Close()
}
