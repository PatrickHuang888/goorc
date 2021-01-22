package column

import (
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/orc/stream"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
	"io"
	"sort"
)

const (
	MaxByteLength = 1024
)

func newStringDirectV2Writer(schema *api.TypeDescription, opts *config.WriterOptions) Writer {
	stats := &pb.ColumnStatistics{StringStatistics: &pb.StringStatistics{
		Maximum: new(string), Minimum: new(string), Sum: new(int64)},
		NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64)}
	var present stream.Writer
	if schema.HasNulls {
		*stats.HasNull = true
		present = stream.NewBoolWriter(schema.Id, pb.Stream_PRESENT, opts)
	}
	var index *pb.RowIndex
	var indexStats *pb.ColumnStatistics
	if opts.WriteIndex {
		index = &pb.RowIndex{}
		indexStats = &pb.ColumnStatistics{NumberOfValues: new(uint64), BytesOnDisk: new(uint64), HasNull: new(bool),
			StringStatistics: &pb.StringStatistics{Maximum: new(string), Minimum: new(string), Sum: new(int64)}}
	}
	base := &writer{schema: schema, opts: opts, stats: stats, present: present, index: index, indexStats: indexStats}
	data := stream.NewStringContentsWriter(schema.Id, pb.Stream_DATA, opts)
	length := stream.NewIntRLV2Writer(schema.Id, pb.Stream_LENGTH, opts, false)
	return &stringDirectV2Writer{base, data, length}
}

type stringDirectV2Writer struct {
	*writer
	data   stream.Writer
	length stream.Writer
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

		lower := w.stats.StringStatistics.LowerBound
		upper := w.stats.StringStatistics.UpperBound

		if lower != nil || upper != nil { // has lower
			lower = w.stats.StringStatistics.LowerBound
			if s < *lower || *lower == "" {
				*lower = s
			}
			upper = w.stats.StringStatistics.UpperBound
			if s > *upper || *upper == "" {
				*upper = s
			}

		} else { // no lower

			if dataLength >= MaxByteLength {
				lower = w.stats.StringStatistics.Minimum
				w.stats.StringStatistics.Minimum = nil
				if s < *lower || *lower == "" {
					*w.stats.StringStatistics.LowerBound = s
				}
				upper := w.stats.StringStatistics.Maximum
				w.stats.StringStatistics.Maximum = nil
				if s > *upper || *upper == "" {
					*w.stats.StringStatistics.UpperBound = s
				}

			} else {
				min := w.stats.StringStatistics.Minimum
				if s < *min || *min == "" {
					*min = s
				}
				max := w.stats.StringStatistics.Maximum
				if s > *max || *max == "" {
					*max = s
				}
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
				Maximum: new(string), Minimum: new(string), Sum: new(int64)},
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

			lower := w.indexStats.StringStatistics.LowerBound
			upper := w.indexStats.StringStatistics.UpperBound

			if lower != nil || upper != nil { // no lower
				if dataLength >= MaxByteLength {
					lower = w.indexStats.StringStatistics.Minimum
					w.indexStats.StringStatistics.Minimum = nil
					if s < *lower || *lower == "" {
						*w.indexStats.StringStatistics.LowerBound = s
					}
					upper := w.indexStats.StringStatistics.Maximum
					w.indexStats.StringStatistics.Maximum = nil
					if s > *upper || *upper == "" {
						*w.indexStats.StringStatistics.UpperBound = s
					}

				} else {
					min := w.indexStats.StringStatistics.GetMinimum()
					if s < min || min == "" {
						*w.indexStats.StringStatistics.Minimum = s
					}
					max := w.indexStats.StringStatistics.GetMaximum()
					if s > max || max == "" {
						*w.indexStats.StringStatistics.Maximum = s
					}
				}

			} else { // has lower

				lower := w.indexStats.StringStatistics.LowerBound
				if s < *lower || *lower == "" {
					*lower = s
				}
				upper = w.indexStats.StringStatistics.UpperBound
				if s > *upper || *upper == "" {
					*upper = s
				}
			}
		}
	}
	return nil
}

func (w *stringDirectV2Writer) Flush() error {
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

	w.flushed = true
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
	w.data.Reset()
	w.length.Reset()
}

func (w stringDirectV2Writer) Size() int {
	if w.schema.HasNulls {
		return w.present.Size() + w.data.Size() + w.length.Size()
	}
	return w.data.Size() + w.length.Size()
}

func NewStringDirectV2Reader(opts *config.ReaderOptions, schema *api.TypeDescription, f orcio.File) Reader {
	return &stringDirectV2Reader{reader: &reader{opts: opts, schema: schema, f: f}}
}

type stringDirectV2Reader struct {
	*reader
	data   *stream.StringContentsReader
	length *stream.IntRLV2Reader
}

func (r *stringDirectV2Reader) InitStream(info *pb.Stream, startOffset uint64) error {
	f, err := r.f.Clone()
	if err != nil {
		return err
	}
	if _, err := f.Seek(int64(startOffset), io.SeekStart); err != nil {
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
		return errors.New("stream kind not unknown")
	}
	return nil
}

func (r *stringDirectV2Reader) Next() (value api.Value, err error) {
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
		if value.V, err = r.data.NextString(l); err != nil {
			return
		}
	}
	return
}

func (r *stringDirectV2Reader) NextBatch(vector []api.Value) error {
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
			l, err = r.length.NextUInt64()
			if err != nil {
				return err
			}
			if vector[i].V, err = r.data.NextString(l); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *stringDirectV2Reader) Seek(rowNumber uint64) error {
	entry, offset, err := r.reader.getIndexEntryAndOffset(rowNumber)
	if err != nil {
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

func (r *stringDirectV2Reader) seek(indexEntry *pb.RowIndexEntry) error {
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
			} else {
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

func (r *stringDirectV2Reader) Close() {
	if r.schema.HasNulls {
		r.present.Close()
	}
	r.data.Close()
	r.length.Close()
}

func NewStringDictionaryV2Reader(opts *config.ReaderOptions, schema *api.TypeDescription, f orcio.File) Reader {
	return &stringDictionaryV2Reader{reader: &reader{opts: opts, schema: schema, f: f}}
}

type stringDictionaryV2Reader struct {
	*reader
	data        *stream.IntRLV2Reader
	dict        *stream.StringContentsReader
	length      *stream.IntRLV2Reader
	dictStrings []string
}

func (r *stringDictionaryV2Reader) InitStream(info *pb.Stream, startOffset uint64) error {
	f, err := r.f.Clone()
	if err != nil {
		return err
	}
	if _, err := f.Seek(int64(startOffset), io.SeekStart); err != nil {
		return err
	}

	switch info.GetKind() {
	case pb.Stream_PRESENT:
		r.present = stream.NewBoolReader(r.opts, info, startOffset, f)
	case pb.Stream_DATA:
		r.data = stream.NewIntRLV2Reader(r.opts, info, startOffset, false, f)
	case pb.Stream_DICTIONARY_DATA:
		r.dict = stream.NewStringContentsReader(r.opts, info, startOffset, f)
	case pb.Stream_LENGTH:
		r.length = stream.NewIntRLV2Reader(r.opts, info, startOffset, false, f)
	default:
		return errors.New("stream kind not unknown")
	}
	return nil
}

func (r *stringDictionaryV2Reader) Next() (value api.Value, err error) {
	if r.schema.HasNulls {
		var p bool
		if p, err = r.present.Next(); err != nil {
			return
		}
		value.Null = !p
	}
	if !value.Null {
		var data uint64
		if data, err = r.data.NextUInt64(); err != nil {
			return
		}
		if int(data) > len(r.dictStrings)-1 {
			for i := len(r.dictStrings); i <= int(data); i++ {
				var l uint64
				if l, err = r.length.NextUInt64(); err != nil {
					return
				}
				var s string
				if s, err = r.dict.NextString(l); err != nil {
					return
				}
				r.dictStrings = append(r.dictStrings, s)
			}
		}
		value.V = r.dictStrings[data]
	}
	return
}

func (r *stringDictionaryV2Reader) NextBatch(vector []api.Value) error {
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
			var data uint64
			if data, err = r.data.NextUInt64(); err != nil {
				return err
			}
			if int(data) > len(r.dictStrings)-1 {
				for i := len(r.dictStrings); i <= int(data); i++ {
					var l uint64
					if l, err = r.length.NextUInt64(); err != nil {
						return err
					}
					var s string
					if s, err = r.dict.NextString(l); err != nil {
						return err
					}
					r.dictStrings = append(r.dictStrings, s)
				}
			}
			vector[i].V = r.dictStrings[data]
		}
	}
	return nil
}

func (r *stringDictionaryV2Reader) Seek(rowNumber uint64) error {
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

func (r *stringDictionaryV2Reader) seek(indexEntry *pb.RowIndexEntry) error {
	if r.schema.HasNulls {
		if err := r.seekPresent(indexEntry); err != nil {
			return err
		}
	}
	// need data index only
	var dataChunk, dataChunkOffset, dataOffset uint64
	if indexEntry != nil {
		pos := indexEntry.Positions
		if r.opts.CompressionKind == pb.CompressionKind_NONE { // no compression
			if r.schema.HasNulls { // no compression has presents
				dataChunkOffset = pos[3]
				dataOffset = pos[4]
			} else { // no compression, no present
				dataChunkOffset = pos[0]
				dataOffset = pos[1]
			}

		} else {
			if r.schema.HasNulls { // has compression, has presents
				dataChunk = pos[4]
				dataChunkOffset = pos[5]
				dataOffset = pos[6]

			} else { // has compression, no presents
				dataChunk = pos[0]
				dataChunkOffset = pos[1]
				dataOffset = pos[2]
			}
		}
	}
	if err := r.data.Seek(dataChunk, dataChunkOffset, dataOffset); err != nil {
		return err
	}
	return nil
}

func (r *stringDictionaryV2Reader) Close() {
	if r.schema.HasNulls {
		r.present.Close()
	}
	r.data.Close()
	r.dict.Close()
	r.length.Close()
}

func NewStringDictionaryV2Writer(schema *api.TypeDescription, opts *config.WriterOptions) Writer {
	stats := &pb.ColumnStatistics{StringStatistics: &pb.StringStatistics{
		Maximum: new(string), Minimum: new(string), Sum: new(int64), LowerBound: new(string), UpperBound: new(string)},
		NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64)}
	var present stream.Writer
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
	data := stream.NewIntRLV2Writer(schema.Id, pb.Stream_DATA, opts, false)
	dict := stream.NewStringContentsWriter(schema.Id, pb.Stream_DICTIONARY_DATA, opts)
	length := stream.NewIntRLV2Writer(schema.Id, pb.Stream_LENGTH, opts, false)
	return &stringDictionaryV2Writer{writer: base, dataStream: data, dictStream: dict, lengthStream: length, data: map[string][]int{}}
}

type stringDictionaryV2Writer struct {
	*writer
	dataStream   stream.Writer
	dictStream   stream.Writer
	lengthStream stream.Writer

	count int
	data  map[string][]int
}

func (w *stringDictionaryV2Writer) Write(value api.Value) error {
	if w.schema.HasNulls {
		if err := w.present.Write(!value.Null); err != nil {
			return err
		}
	}

	if !value.Null {
		s, ok := value.V.(string)
		if !ok {
			return errors.New("string column writing, value should be string")
		}
		// string encoded in utf-8
		data := []byte(s)
		l := len(data)

		vs := w.data[s]
		w.count++
		vs = append(vs, w.count)
		w.data[s] = vs

		*w.stats.NumberOfValues++
		*w.stats.StringStatistics.Sum += int64(l)
		if l >= MaxByteLength {
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

	// todo: index writing
	/*if w.opts.WriteIndex {
		w.indexInRows++
		if w.indexInRows >= w.opts.IndexStride {
			var pp []uint64
			if w.schema.HasNulls {
				pp = append(pp, w.present.GetPosition()...)
			}
			pp = append(pp, w.dataStream.GetPosition()...)
			pp = append(pp, w.lengthStream.GetPosition()...)
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
			*w.indexStats.StringStatistics.Sum += int64(l)
			s := value.V.(string)
			if l >= MaxByteLength {
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
	}*/
	return nil
}

func (w *stringDictionaryV2Writer) Flush() error {
	var dict []string
	for k := range w.data {
		dict = append(dict, k)
	}
	sort.Strings(dict)

	data := make([]int, w.count)
	for dictIndex, keyString := range dict {
		positions := w.data[keyString]
		for _, position := range positions {
			data[position] = dictIndex
		}
	}
	for _, v := range data {
		if err := w.dataStream.Write(v); err != nil {
			return err
		}
	}
	for _, v := range dict {
		if err := w.dictStream.Write(v); err != nil {
			return err
		}
		bs := []byte(v)
		if err := w.lengthStream.Write(uint64(len(bs))); err != nil {
			return err
		}
	}

	if w.schema.HasNulls {
		if err := w.present.Flush(); err != nil {
			return err
		}
	}
	if err := w.dataStream.Flush(); err != nil {
		return err
	}
	if err := w.dictStream.Flush(); err != nil {
		return err
	}
	if err := w.lengthStream.Flush(); err != nil {
		return err
	}

	if w.schema.HasNulls {
		*w.stats.BytesOnDisk = w.present.Info().GetLength()
	}
	*w.stats.BytesOnDisk += w.dataStream.Info().GetLength()
	*w.stats.BytesOnDisk += w.dictStream.Info().GetLength()
	*w.stats.BytesOnDisk += w.lengthStream.Info().GetLength()

	w.flushed = true
	return nil
}

func (w *stringDictionaryV2Writer) WriteOut(out io.Writer) (n int64, err error) {
	var pn, dn, dcn, ln int64
	if w.schema.HasNulls {
		if pn, err = w.present.WriteOut(out); err != nil {
			return 0, err
		}
	}
	if dn, err = w.dataStream.WriteOut(out); err != nil {
		return 0, err
	}
	if dcn, err = w.dictStream.WriteOut(out); err != nil {
		return 0, err
	}
	if ln, err = w.lengthStream.WriteOut(out); err != nil {
		return 0, err
	}
	return pn + dn + dcn + ln, nil
}

func (w stringDictionaryV2Writer) GetStreamInfos() []*pb.Stream {
	if w.schema.HasNulls {
		return []*pb.Stream{w.present.Info(), w.dataStream.Info(), w.dictStream.Info(), w.lengthStream.Info()}
	}
	return []*pb.Stream{w.dataStream.Info(), w.dictStream.Info(), w.lengthStream.Info()}
}

func (w stringDictionaryV2Writer) Reset() {
	w.reset()
	w.dataStream.Reset()
	w.dictStream.Reset()
	w.lengthStream.Reset()

	w.data = map[string][]int{}
	w.count = 0
}

func (w stringDictionaryV2Writer) Size() int {
	// fixme: size calculation

	if w.schema.HasNulls {
		return w.present.Size() + w.dataStream.Size() + w.dictStream.Size() + w.lengthStream.Size()
	}
	return w.dataStream.Size() + w.dictStream.Size() + w.lengthStream.Size()
}
