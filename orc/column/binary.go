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

func NewBinaryV2Writer(schema *api.TypeDescription, opts *config.WriterOptions) Writer {
	stats := &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64),
		BinaryStatistics: &pb.BinaryStatistics{Sum: new(int64)}}
	var present stream.Writer
	if schema.HasNulls {
		*stats.HasNull = true
		present = stream.NewBoolWriter(schema.Id, pb.Stream_PRESENT, opts)
	}
	var indexStats *pb.ColumnStatistics
	var index *pb.RowIndex
	if opts.WriteIndex {
		indexStats = &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64),
			BinaryStatistics: &pb.BinaryStatistics{Sum: new(int64)}}
		index = &pb.RowIndex{}
	}
	base := &writer{schema: schema, opts: opts, stats: stats, present: present, indexStats: indexStats, index: index}
	data := stream.NewStringContentsWriter(schema.Id, pb.Stream_DATA, opts)
	length := stream.NewIntRLV2Writer(schema.Id, pb.Stream_LENGTH, opts, false)
	return &binaryV2Writer{base, data, length}
}

type binaryV2Writer struct {
	*writer
	data   stream.Writer
	length stream.Writer
}

func (w *binaryV2Writer) Write(value api.Value) error {
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
			pp = append(pp, w.length.GetPosition()...)
			w.index.Entry = append(w.index.Entry, &pb.RowIndexEntry{Positions: pp, Statistics: w.indexStats})

			// new stats
			w.indexStats = &pb.ColumnStatistics{BinaryStatistics: &pb.BinaryStatistics{Sum: new(int64)}, NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64)}
			if w.schema.HasNulls {
				*w.indexStats.HasNull = true
			}
			w.indexInRows = 0
		}
		if !value.Null {
			*w.indexStats.BinaryStatistics.Sum++
			*w.indexStats.NumberOfValues++
		}
	}
	return nil
}

func (w *binaryV2Writer) Flush() error {
	w.flushed = true

	if w.schema.HasNulls {
		if err := w.present.Flush(); err != nil {
			return err
		}
	}
	if err := w.data.Flush(); err != nil {
		return err
	}
	if err := w.length.Flush(); err != nil {
		return err
	}

	if w.schema.HasNulls {
		*w.stats.BytesOnDisk = w.present.Info().GetLength()
	}
	*w.stats.BytesOnDisk += w.data.Info().GetLength()
	*w.stats.BytesOnDisk += w.length.Info().GetLength()
	return nil
}

func (w *binaryV2Writer) WriteOut(out io.Writer) (n int64, err error) {
	var pn int64
	if w.schema.HasNulls {
		var err error
		if pn, err = w.present.WriteOut(out); err != nil {
			return 0, err
		}
	}
	dn, err := w.data.WriteOut(out)
	if err != nil {
		return 0, err
	}
	ln, err := w.length.WriteOut(out)
	if err != nil {
		return 0, err
	}
	return pn + dn + ln, nil
}

func (w binaryV2Writer) GetStreamInfos() []*pb.Stream {
	if w.schema.HasNulls {
		return []*pb.Stream{w.present.Info(), w.data.Info(), w.length.Info()}
	}
	return []*pb.Stream{w.data.Info(), w.length.Info()}
}

func (w *binaryV2Writer) Reset() {
	w.reset()
	w.data.Reset()
	w.length.Reset()
}

func (w binaryV2Writer) Size() int {
	if w.schema.HasNulls {
		return w.present.Size() + w.data.Size() + w.length.Size()
	}
	return w.data.Size() + w.length.Size()
}

func NewBinaryV2Reader(opts *config.ReaderOptions, schema *api.TypeDescription, f orcio.File) Reader {
	return &binaryV2Reader{reader: &reader{opts: opts, schema: schema, f: f}}
}

type binaryV2Reader struct {
	*reader
	data   *stream.StringContentsReader
	length *stream.IntRLV2Reader
}

func (r *binaryV2Reader) InitStream(info *pb.Stream, startOffset uint64) error {
	f, err := r.f.Clone()
	if err != nil {
		return err
	}
	if _, err = f.Seek(int64(startOffset), io.SeekStart); err != nil {
		return err
	}

	switch info.GetKind() {
	case pb.Stream_PRESENT:
		r.present = stream.NewBoolReader(r.opts, info, startOffset, f)
	case pb.Stream_DATA:
		r.data = stream.NewStringContentsReader(r.opts, info, startOffset, f)
	case pb.Stream_LENGTH:
		r.length = stream.NewIntRLV2Reader(r.opts, info, startOffset, false, f)
	default:
		errors.New("stream kind not unknown")
	}
	return nil
}

func (r *binaryV2Reader) Next() (value api.Value, err error) {
	if r.schema.HasNulls {
		var p bool
		if p, err = r.present.Next(); err != nil {
			return
		}
		value.Null = !p
	}

	if !value.Null {
		var l uint64
		l, err = r.length.NextUInt64()
		if err != nil {
			return
		}
		if value.V, err = r.data.NextBytes(l); err != nil {
			return
		}
	}
	return
}

func (r *binaryV2Reader) NextBatch(vector []api.Value) error {
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
			var l uint64
			if l, err = r.length.NextUInt64(); err != nil {
				return err
			}
			if vector[i].V, err = r.data.NextBytes(l); err != nil {
				return err
			}
		}
	}
	return err
}

func (r *binaryV2Reader) Seek(rowNumber uint64) error {
	entry, offset, err := r.reader.getIndexEntryAndOffset(rowNumber)
	if err!=nil {
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

func (r *binaryV2Reader) seek(indexEntry *pb.RowIndexEntry) error {
	if r.schema.HasNulls {
		if err := r.seekPresent(indexEntry); err != nil {
			return err
		}
	}
	var dataChunk, dataChunkOffset uint64
	var lengthChunk, lengthChunkOffset, lengthOffset uint64
	if indexEntry != nil {
		pos := indexEntry.Positions
		if r.opts.CompressionKind == pb.CompressionKind_NONE {
			if r.schema.HasNulls {
				dataChunkOffset = pos[3]
				lengthChunkOffset = indexEntry.Positions[4]
				lengthOffset = indexEntry.Positions[5]
			} else {
				dataChunkOffset = pos[0]
				lengthChunkOffset = indexEntry.Positions[1]
				lengthOffset = indexEntry.Positions[2]
			}

		} else {
			if r.schema.HasNulls {
				dataChunk = pos[4]
				dataChunkOffset = pos[5]
				lengthChunk = indexEntry.Positions[6]
				lengthChunkOffset = indexEntry.Positions[7]
				lengthOffset = indexEntry.Positions[8]
			} else { // no nulls, has compression
				dataChunk = pos[0]
				dataChunkOffset = pos[1]
				lengthChunk = indexEntry.Positions[2]
				lengthChunkOffset = indexEntry.Positions[3]
				lengthOffset = indexEntry.Positions[4]
			}
		}
	}
	if err := r.data.Seek(dataChunk, dataChunkOffset); err != nil {
		return err
	}
	if err := r.length.Seek(lengthChunk, lengthChunkOffset, lengthOffset); err != nil {
		return err
	}
	return nil
}

func (r *binaryV2Reader) Close() {
	if r.schema.HasNulls {
		r.present.Close()
	}
	r.data.Close()
	r.length.Close()
}
