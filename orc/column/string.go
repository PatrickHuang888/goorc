package column

import (
	"io"

	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/orc/stream"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
)

const (
	MaxByteLength = 1024
)

func newStringDirectV2Writer(schema *api.TypeDescription, opts *config.WriterOptions) Writer {
	stats := &pb.ColumnStatistics{StringStatistics: &pb.StringStatistics{
		Maximum: new(string), Minimum: new(string), Sum: new(int64), LowerBound: new(string), UpperBound: new(string)},
		NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64)}
	var present *stream.Writer
	if schema.HasNulls {
		*stats.HasNull = true
		present = stream.NewBoolWriter(schema.Id, pb.Stream_PRESENT, opts)
	}
	var index *pb.RowIndex
	var indexStats *pb.ColumnStatistics
	if opts.WriteIndex {
		index = &pb.RowIndex{}
		indexStats = &pb.ColumnStatistics{NumberOfValues: new(uint64), BytesOnDisk: new(uint64), HasNull: new(bool),
			StringStatistics: &pb.StringStatistics{Maximum: new(string), Minimum: new(string), Sum: new(int64), LowerBound: new(string), UpperBound: new(string)}}
	}
	base := &writer{schema: schema, opts: opts, stats: stats, present: present, index: index, indexStats: indexStats}
	data := stream.NewStringContentsWriter(schema.Id, pb.Stream_DATA, opts)
	length := stream.NewIntRLV2Writer(schema.Id, pb.Stream_LENGTH, opts, false)
	return &stringDirectV2Writer{base, data, length}
}

type stringDirectV2Writer struct {
	*writer
	data   *stream.Writer
	length *stream.Writer
}

func (w *stringDirectV2Writer) Write(value api.Value) error {
	if w.schema.HasNulls {
		if err := w.present.Write(!value.Null); err != nil {
			return err
		}
	}

	var dataLength int
	if !value.Null {
		s, ok := value.V.(string)
		if !ok {
			return errors.New("string column writing, value should be string")
		}
		// string encoded in utf-8
		data := []byte(s)
		dataLength = len(data)
		if err := w.data.Write(data); err != nil {
			return err
		}
		if err := w.length.Write(uint64(dataLength)); err != nil {
			return err
		}

		*w.stats.NumberOfValues++
		*w.stats.StringStatistics.Sum += int64(dataLength)
		if dataLength >= MaxByteLength {
			if s < *w.stats.StringStatistics.LowerBound {
				*w.stats.StringStatistics.LowerBound = s
			}
			if s > *w.stats.StringStatistics.UpperBound {
				*w.stats.StringStatistics.UpperBound = s
			}
		} else {
			if s < *w.stats.StringStatistics.Minimum {
				*w.stats.StringStatistics.Minimum = s
			}
			if s > *w.stats.StringStatistics.Maximum {
				*w.stats.StringStatistics.Maximum = s
			}
		}
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
			w.index.Entry = append(w.index.Entry, &pb.RowIndexEntry{Statistics: w.indexStats, Positions: pp})
			// new stats
			w.indexStats = &pb.ColumnStatistics{StringStatistics: &pb.StringStatistics{
				Maximum: new(string), Minimum: new(string), Sum: new(int64), LowerBound: new(string), UpperBound: new(string)},
				NumberOfValues: new(uint64), HasNull: new(bool)}
			if w.schema.HasNulls {
				*w.indexStats.HasNull = true
			}
			w.indexInRows = 0
		}

		if !value.Null {
			*w.indexStats.NumberOfValues++
			*w.indexStats.StringStatistics.Sum += int64(dataLength)
			s := value.V.(string)
			if dataLength >= MaxByteLength {
				if s < *w.indexStats.StringStatistics.LowerBound {
					*w.indexStats.StringStatistics.LowerBound = s
				}
				if s > *w.indexStats.StringStatistics.UpperBound {
					*w.indexStats.StringStatistics.UpperBound = s
				}
			} else {
				if s < *w.indexStats.StringStatistics.Minimum {
					*w.indexStats.StringStatistics.Minimum = s
				}
				if s > *w.indexStats.StringStatistics.Maximum {
					*w.indexStats.StringStatistics.Maximum = s
				}
			}
		}
	}
	return nil
}

func (w *stringDirectV2Writer) Flush() error {
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

func (w *stringDirectV2Writer) WriteOut(out io.Writer) (int64, error) {
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

func (w stringDirectV2Writer) GetStreamInfos() []*pb.Stream {
	if w.schema.HasNulls {
		return []*pb.Stream{w.present.Info(), w.data.Info(), w.length.Info()}
	}
	return []*pb.Stream{w.data.Info(), w.length.Info()}
}

func (w *stringDirectV2Writer) Reset() {
	w.writer.reset()
	if w.schema.HasNulls {
		w.present.Reset()
	}
	w.data.Reset()
	w.length.Reset()
}

func (w stringDirectV2Writer) Size() int {
	if w.schema.HasNulls {
		return w.present.Size() + w.data.Size() + w.length.Size()
	}
	return w.data.Size() + w.length.Size()
}

func newStringDirectV2Reader(opts *config.ReaderOptions, schema *api.TypeDescription, f orcio.File) Reader {
	return &stringDirectV2Reader{reader: &reader{opts: opts, schema: schema, f: f}}
}

type stringDirectV2Reader struct {
	*reader
	data   *stream.StringContentsReader
	length *stream.IntRLV2Reader
}

func (s *stringDirectV2Reader) InitStream(info *pb.Stream, startOffset uint64) error {
	f, err := s.f.Clone()
	if err != nil {
		return err
	}
	if _, err := f.Seek(int64(startOffset), io.SeekStart); err != nil {
		return err
	}

	switch info.GetKind() {
	case pb.Stream_PRESENT:
		s.present = stream.NewBoolReader(s.opts, info, startOffset, f)
	case pb.Stream_DATA:
		s.data = stream.NewStringContentsReader(s.opts, info, startOffset, f)
	case pb.Stream_LENGTH:
		s.length = stream.NewIntRLV2Reader(s.opts, info, startOffset, false, f)
	default:
		errors.New("stream kind not unknown")
	}
	return nil
}

func (r *stringDirectV2Reader) Next() (value api.Value, err error) {
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

	hasValue := true
	if r.schema.HasNulls && value.Null {
		hasValue = false
	}
	if hasValue {
		var l uint64
		l, err = r.length.NextUInt64()
		if err != nil {
			return
		}
		if value.V, err = r.data.NextString(l); err != nil {
			return
		}
	}
	return
}

func (r *stringDirectV2Reader) Seek(rowNumber uint64) error {
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

func (r *stringDirectV2Reader) seek(entry *pb.RowIndexEntry) error {
	if err := r.checkInit(); err != nil {
		return err
	}

	// from start
	if entry == nil {
		if r.schema.HasNulls {
			if err := r.present.Seek(0, 0, 0, 0); err != nil {
				return err
			}
		}
		if err := r.data.Seek(0, 0, 0); err != nil {
			return err
		}
		if err := r.length.Seek(0, 0, 0); err != nil {
			return err
		}
		return nil
	}

	var presentChunk, presentChunkOffset, presentOffset1, presentOffset2 uint64
	var dataChunk, dataChunkOffset, dataOffset uint64
	var lengthChunk, lengthChunkOffset, lengthOffset uint64
	if r.opts.CompressionKind == pb.CompressionKind_NONE {
		if r.schema.HasNulls {
			presentChunkOffset = entry.Positions[0]
			presentOffset1 = entry.Positions[1]
			presentOffset2 = entry.Positions[2]
			dataChunkOffset = entry.Positions[3]
			dataOffset = entry.Positions[4]
			lengthChunkOffset = entry.Positions[5]
			lengthOffset = entry.Positions[6]

		} else {
			dataChunkOffset = entry.Positions[0]
			dataOffset = entry.Positions[1]
			lengthChunkOffset = entry.Positions[2]
			lengthOffset = entry.Positions[3]
		}

	} else { // compression
		if r.schema.HasNulls {
			presentChunk = entry.Positions[0]
			presentChunkOffset = entry.Positions[1]
			presentOffset1 = entry.Positions[2]
			presentOffset2 = entry.Positions[3]
			dataChunk = entry.Positions[4]
			dataChunkOffset = entry.Positions[5]
			dataOffset = entry.Positions[6]
			lengthChunk = entry.Positions[7]
			lengthChunkOffset = entry.Positions[8]
			lengthOffset = entry.Positions[9]

		} else {
			dataChunk = entry.Positions[0]
			dataChunkOffset = entry.Positions[1]
			dataOffset = entry.Positions[2]
			lengthChunk = entry.Positions[3]
			lengthChunkOffset = entry.Positions[4]
			lengthOffset = entry.Positions[5]
		}
	}

	if r.schema.HasNulls {
		if err := r.present.Seek(presentChunk, presentChunkOffset, presentOffset1, presentOffset2); err != nil {
			return err
		}
	}
	if err := r.data.Seek(dataChunk, dataChunkOffset, dataOffset); err != nil {
		return err
	}
	if err := r.length.Seek(lengthChunk, lengthChunkOffset, lengthOffset); err != nil {
		return err
	}
	return nil
}

func (r *stringDirectV2Reader) Close() {
	if r.schema.HasNulls {
		r.present.Close()
	}
	r.data.Close()
	r.length.Close()
}

func (r stringDirectV2Reader) checkInit() error {
	if r.schema.HasNulls {
		if r.present == nil {
			return errors.New("present stream not initialized")
		}
	}
	if r.data == nil {
		return errors.New("data stream not initialized")
	}
	if r.length == nil {
		return errors.New("length stream not initialized")
	}
	return nil
}
