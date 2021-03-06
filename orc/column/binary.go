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
	return &BinaryV2Reader{reader: reader{opts: opts, schema: schema, f: f}}
}

type BinaryV2Reader struct {
	reader

	present *stream.BoolReader
	data    *stream.StringContentsReader
	length  *stream.IntRLV2Reader
}

func (r *BinaryV2Reader) InitStream(info *pb.Stream, startOffset uint64) error {
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
			return errors.New("has no nulls")
		}
		r.present = stream.NewBoolReader(r.opts, info, startOffset, f)
	case pb.Stream_DATA:
		r.data = stream.NewStringContentsReader(r.opts, info, startOffset, f)
	case pb.Stream_LENGTH:
		r.length = stream.NewIntRLV2Reader(r.opts, info, startOffset, false, f)
	default:
		return errors.New("stream kind not unknown")
	}
	return nil
}

func (r *BinaryV2Reader) Skip(rows uint64) error {
	var err error
	p := true

	for i := 0; i < int(rows); i++ {
		if r.present!=nil {
			if p, err = r.present.Next(); err != nil {
				return err
			}
		}

		if p {
			var l uint64
			l, err = r.length.NextUInt64()
			if err != nil {
				return err
			}
			if _, err = r.data.NextBytes(l); err != nil {
				return nil
			}
		}
	}
	return nil
}

func (r *BinaryV2Reader) NextBatch(vec *api.ColumnVector) error {
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
			var l uint64
			if l, err = r.length.NextUInt64(); err != nil {
				return err
			}
			if vec.Vector[i].V, err = r.data.NextBytes(l); err != nil {
				return err
			}
		}
	}
	return err
}

func (r *BinaryV2Reader) SeekStride(stride int) error {
	if stride == 0 {
		if r.present != nil {
			if err := r.present.Seek(0, 0, 0, 0); err != nil {
				return err
			}
		}
		if err := r.data.Seek(0, 0); err != nil {
			return err
		}
		if err := r.length.Seek(0, 0, 0); err != nil {
			return err
		}
		return nil
	}

	var dataChunk, dataChunkOffset uint64
	var lengthChunk, lengthChunkOffset, lengthOffset uint64

	pos, err := r.getStridePositions(stride)
	if err != nil {
		return err
	}

	if r.present != nil {
		var pChunk, pChunkOffset, pOffset1, pOffset2 uint64
		if r.opts.CompressionKind == pb.CompressionKind_NONE {
			// no compression
			pChunkOffset = pos[0]
			pOffset1 = pos[1]
			pOffset2 = pos[2]

			dataChunkOffset = pos[3]
			lengthChunkOffset = pos[4]
			lengthOffset = pos[5]

		} else {
			// compression
			pChunk = pos[0]
			pChunkOffset = pos[1]
			pOffset1 = pos[2]
			pOffset2 = pos[3]

			dataChunk = pos[4]
			dataChunkOffset = pos[5]
			lengthChunk = pos[6]
			lengthChunkOffset = pos[7]
			lengthOffset = pos[8]
		}
		if err = r.present.Seek(pChunk, pChunkOffset, pOffset1, pOffset2); err != nil {
			return err
		}

	} else {

		if r.opts.CompressionKind == pb.CompressionKind_NONE {
			dataChunkOffset = pos[0]
			lengthChunkOffset = pos[1]
			lengthOffset = pos[2]

		} else {
			dataChunk = pos[0]
			dataChunkOffset = pos[1]
			lengthChunk = pos[2]
			lengthChunkOffset = pos[3]
			lengthOffset = pos[4]
		}
	}

	if err = r.data.Seek(dataChunk, dataChunkOffset); err != nil {
		return err
	}
	if err = r.length.Seek(lengthChunk, lengthChunkOffset, lengthOffset); err != nil {
		return err
	}
	return nil
}

func (r *BinaryV2Reader) Close() {
	if r.present != nil {
		r.present.Close()
	}
	r.data.Close()
	r.length.Close()
}
