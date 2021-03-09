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

type ByteReader struct {
	reader

	present *stream.BoolReader
	data    *stream.ByteReader
}

func NewByteReader(schema *api.TypeDescription, opts *config.ReaderOptions, f orcio.File) Reader {
	return &ByteReader{reader: reader{opts: opts, schema: schema, f: f}}
}

func (r *ByteReader) InitStream(info *pb.Stream, startOffset uint64) error {
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

func (r *ByteReader) NextBatch(vec *api.ColumnVector) error {
	var err error
	for i := 0; i < len(vec.Vector); i++ {
		if r.present!=nil {
			var p bool
			if p, err = r.present.Next(); err != nil {
				return err
			}
			vec.Vector[i].Null = !p
		}
		if !vec.Vector[i].Null {
			if vec.Vector[i].V, err = r.data.Next(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *ByteReader) Skip(rows uint64) error {
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

func (r *ByteReader) SeekStride(stride int) error {
	if stride==0 {
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

	}else {

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

func (r *ByteReader) Close() {
	if r.present != nil {
		r.present.Close()
	}
	r.data.Close()
}
