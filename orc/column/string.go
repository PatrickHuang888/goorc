package column

import (
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/orc/stream"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
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
	base := &writer{schema: schema, opts: opts, stats: stats, present: present}
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
		w.present.Write(!value.Null)
	}

	if value.Null {
		return nil
	}

	s, ok := value.V.(string)
	if !ok {
		return errors.New("string column writing, value should be string")
	}
	// string encoded in utf-8
	data := []byte(s)
	if err := w.data.Write(data); err != nil {
		return err
	}
	if err := w.length.Write(uint64(len(data))); err != nil {
		return err
	}

	*w.stats.NumberOfValues++
	*w.stats.StringStatistics.Sum += int64(len(data))
	// todo: min, max, lowerbound, upperbound

	if w.opts.WriteIndex {
		w.indexInRows++
		if w.indexInRows >= w.opts.IndexStride {
			// todo: write index
			w.index.Entry = append(w.index.Entry, &pb.RowIndexEntry{Statistics: w.indexStats})
			// new stats
			// no index statistic bytes on disk, java writer neither
			w.indexStats = &pb.ColumnStatistics{StringStatistics: &pb.StringStatistics{
				Maximum: new(string), Minimum: new(string), Sum: new(int64), LowerBound: new(string), UpperBound: new(string)},
				NumberOfValues: new(uint64), HasNull: new(bool)}
			if w.schema.HasNulls {
				*w.indexStats.HasNull = true
			}

			w.indexInRows = 0
		}

		*w.indexStats.NumberOfValues++
		*w.indexStats.StringStatistics.Sum += int64(len(data))
		// todo: min, max, lowerbound, upperbound
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
	data    *stream.StringContentsReader
	length  *stream.IntRLV2Reader
}

func (s *stringDirectV2Reader) InitStream(info *pb.Stream, startOffset uint64) error {
	if info.GetKind() == pb.Stream_PRESENT {
		fp, err := s.f.Clone()
		if err != nil {
			return err
		}
		if _, err := fp.Seek(int64(startOffset), io.SeekStart); err != nil {
			return err
		}
		s.present = stream.NewBoolReader(s.opts, info, startOffset, fp)
		return nil
	}
	if info.GetKind() == pb.Stream_DATA {
		fd, err := s.f.Clone()
		if err != nil {
			return err
		}
		if _, err := fd.Seek(int64(startOffset), io.SeekStart); err != nil {
			return err
		}
		s.data = stream.NewStringContentsReader(s.opts, info, startOffset, fd)
		return nil
	}
	if info.GetKind() == pb.Stream_LENGTH {
		fl, err := s.f.Clone()
		if err != nil {
			return err
		}
		if _, err := fl.Seek(int64(startOffset), io.SeekStart); err != nil {
			return err
		}
		s.length = stream.NewIntRLV2Reader(s.opts, info, startOffset, false, fl)
		return nil
	}
	log.Warnf("stream %s not recognized", info.GetKind())
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
	// todo:
	/*page := rowNumber / uint64(r.opts.IndexStride)
	offset := rowNumber % uint64(r.opts.IndexStride)
	entry := r.index.Entry[page]

	if r.opts.CompressionKind == pb.CompressionKind_NONE {
		if r.schema.HasNulls { // no compression, has presents
			if err := r.present.Seek(0, entry.Positions[0], offset); err != nil {
				return err
			}
			if err := r.length.Seek(0, entry.Positions[2], 0); err != nil {
				return err
			}
			lens := make([]uint64, offset)
			for i := 0; i < int(offset); i++ {
				var err error
				if lens[i], err = r.length.NextUInt64(); err != nil {
					return err
				}
			}
			if err := r.data.Seek(0, entry.Positions[1], lens); err != nil {
				return err
			}
			return nil
		}

		// no compression, no presents
		if err := r.length.Seek(0, entry.Positions[1], 0); err != nil {
			return err
		}
		lens := make([]uint64, offset)
		for i := 0; i < int(offset); i++ {
			var err error
			if lens[i], err = r.length.NextUInt64(); err != nil {
				return err
			}
		}
		if err := r.data.Seek(0, entry.Positions[0], lens); err != nil {
			return err
		}
		return nil
	}

	if r.schema.HasNulls { // compression, has presents
		if err := r.present.Seek(entry.Positions[0], entry.Positions[1], offset); err != nil {
			return err
		}
		if err := r.length.Seek(entry.Positions[4], entry.Positions[5], 0); err != nil {
			return err
		}
		lens := make([]uint64, offset)
		for i := 0; i < int(offset); i++ {
			var err error
			if lens[i], err = r.length.NextUInt64(); err != nil {
				return err
			}
		}
		if err := r.data.Seek(entry.Positions[2], entry.Positions[3], lens); err != nil {
			return err
		}
		return nil
	}

	// compression, no presents
	if err := r.length.Seek(entry.Positions[2], entry.Positions[3], 0); err != nil {
		return err
	}
	lens := make([]uint64, offset)
	for i := 0; i < int(offset); i++ {
		var err error
		if lens[i], err = r.length.NextUInt64(); err != nil {
			return err
		}
	}
	if err := r.data.Seek(entry.Positions[0], entry.Positions[1], lens); err != nil {
		return err
	}*/
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
