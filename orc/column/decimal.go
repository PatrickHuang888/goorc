package column

import (
	"fmt"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"io"
	"strconv"

	"github.com/pkg/errors"

	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	"github.com/patrickhuang888/goorc/orc/stream"
	"github.com/patrickhuang888/goorc/pb/pb"
)

func NewDecimal64V2Writer(schema *api.TypeDescription, opts *config.WriterOptions) Writer {
	stats := &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64),
		DecimalStatistics: &pb.DecimalStatistics{Minimum: new(string), Maximum: new(string), Sum: new(string)}}
	var present stream.Writer
	if schema.HasNulls {
		*stats.HasNull = true
		present = stream.NewBoolWriter(schema.Id, pb.Stream_PRESENT, opts)
	}
	var indexStats *pb.ColumnStatistics
	var index *pb.RowIndex
	if opts.WriteIndex {
		indexStats = &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64),
			DecimalStatistics: &pb.DecimalStatistics{Minimum: new(string), Maximum: new(string), Sum: new(string)}}
		index = &pb.RowIndex{}
	}
	data := stream.NewVarint64V2Writer(schema.Id, pb.Stream_DATA, opts)
	secondary := stream.NewIntRLV2Writer(schema.Id, pb.Stream_SECONDARY, opts, true)
	return &decimalV2Writer{&writer{schema: schema, opts: opts, stats: stats, present: present, index: index,
		indexStats: indexStats}, data, secondary}
}

type decimalV2Writer struct {
	*writer
	data      stream.Writer
	secondary stream.Writer
}

func (w *decimalV2Writer) Write(value api.Value) error {
	if w.schema.HasNulls {
		if err := w.present.Write(!value.Null); err != nil {
			return err
		}
	}

	if !value.Null {
		d := value.V.(api.Decimal64)
		if err := w.data.Write(d.Precision); err != nil {
			return err
		}
		if err := w.secondary.Write(int64(d.Scale)); err != nil {
			return err
		}
		s := d.String()
		if s < w.stats.DecimalStatistics.GetMinimum() {
			*w.stats.DecimalStatistics.Minimum = s
		}
		if s > w.stats.DecimalStatistics.GetMaximum() {
			*w.stats.DecimalStatistics.Maximum = s
		}
		var sum float64
		var err error
		if w.stats.DecimalStatistics.GetSum()!="" {
			if sum, err = strconv.ParseFloat(w.stats.DecimalStatistics.GetSum(), 64);err != nil {
				return errors.WithStack(err)
			}
		}
		sum += d.Float64()
		*w.stats.DecimalStatistics.Sum = fmt.Sprintf("%f", sum)
		*w.stats.NumberOfValues++

		if w.opts.WriteIndex {
			w.indexInRows++

			if w.indexInRows >= w.opts.IndexStride {
				var pp []uint64
				if w.schema.HasNulls {
					pp = append(pp, w.present.GetPosition()...)
				}
				pp = append(pp, w.data.GetPosition()...)
				pp = append(pp, w.secondary.GetPosition()...)
				w.index.Entry = append(w.index.Entry, &pb.RowIndexEntry{Positions: pp, Statistics: w.indexStats})

				w.indexStats = &pb.ColumnStatistics{
					DecimalStatistics: &pb.DecimalStatistics{Maximum: new(string), Minimum: new(string), Sum: new(string)},
					NumberOfValues:    new(uint64), HasNull: new(bool)}
				if w.schema.HasNulls {
					*w.indexStats.HasNull = true
				}
				w.indexInRows = 0
			}

			if s < w.indexStats.DecimalStatistics.GetMinimum() {
				*w.indexStats.DecimalStatistics.Minimum = s
			}
			if s > w.indexStats.DecimalStatistics.GetMaximum() {
				*w.stats.DecimalStatistics.Maximum = s
			}
			indexSum, err := strconv.ParseFloat(w.indexStats.DecimalStatistics.GetSum(), 64)
			if err != nil {
				return errors.WithStack(err)
			}
			indexSum += d.Float64()
			*w.stats.DecimalStatistics.Sum = fmt.Sprintf("%f", indexSum)
			*w.indexStats.NumberOfValues++
		}
	}

	return nil
}

func (w *decimalV2Writer) Flush() error {
	if w.schema.HasNulls {
		if err := w.present.Flush(); err != nil {
			return err
		}
		*w.stats.BytesOnDisk = w.present.Info().GetLength()
	}
	if err := w.data.Flush(); err != nil {
		return err
	}
	if err := w.secondary.Flush(); err != nil {
		return err
	}

	*w.stats.BytesOnDisk += w.data.Info().GetLength()
	*w.stats.BytesOnDisk += w.secondary.Info().GetLength()

	w.flushed = true
	return nil
}

func (w *decimalV2Writer) WriteOut(out io.Writer) (n int64, err error) {
	if !w.flushed {
		return 0, errors.New("not flushed")
	}
	var np, nd, ns int64
	if w.schema.HasNulls {
		if np, err = w.present.WriteOut(out); err != nil {
			return
		}
	}
	if nd, err = w.data.WriteOut(out); err != nil {
		return
	}
	if ns, err = w.secondary.WriteOut(out); err != nil {
		return
	}
	n = np + nd + ns
	return
}

func (w decimalV2Writer) GetStreamInfos() []*pb.Stream {
	if w.schema.HasNulls {
		return []*pb.Stream{w.present.Info(), w.data.Info(), w.secondary.Info()}
	}
	return []*pb.Stream{w.data.Info(), w.secondary.Info()}
}

func (w decimalV2Writer) Reset() {
	w.reset()
	w.data.Reset()
	w.secondary.Reset()
}

func (w decimalV2Writer) Size() int {
	if w.schema.HasNulls {
		return w.present.Size() + w.data.Size() + w.secondary.Size()
	}
	return w.data.Size() + w.secondary.Size()
}

func NewDecimal64V2Reader(schema *api.TypeDescription, opts *config.ReaderOptions, f orcio.File) Reader {
	return &decimalV2Reader{reader: &reader{schema: schema, opts: opts, f: f}}
}

type decimalV2Reader struct {
	*reader
	data      *stream.Varint64Reader
	secondary *stream.IntRLV2Reader
}

func (r *decimalV2Reader) InitStream(info *pb.Stream, startOffset uint64) error {
	ic, err := r.f.Clone()
	if err != nil {
		return err
	}
	if _, err = ic.Seek(int64(startOffset), io.SeekStart); err != nil {
		return err
	}

	switch info.GetKind() {
	case pb.Stream_PRESENT:
		r.present = stream.NewBoolReader(r.opts, info, startOffset, ic)
	case pb.Stream_DATA:
		r.data = stream.NewVarIntReader(r.opts, info, startOffset, ic)
	case pb.Stream_SECONDARY:
		r.secondary = stream.NewIntRLV2Reader(r.opts, info, startOffset, true, ic)
	default:
		return errors.New("stream kind error")
	}
	return nil
}

func (r *decimalV2Reader) Next() (value api.Value, err error) {
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
		var precision int64
		if precision, err = r.data.NextInt64(); err != nil {
			return
		}
		var scale int64
		if scale, err = r.secondary.NextInt64(); err != nil {
			return
		}
		value.V = api.Decimal64{Precision: precision, Scale: int(scale)}
	}
	return
}

func (r *decimalV2Reader) NextBatch(batch *api.ColumnVector) error {
	var err error
	if err = r.checkInit(); err != nil {
		return err
	}

	if r.schema.Id != batch.Id {
		return errors.New("column error")
	}

	for i := 0; i < len(batch.Vector); i++ {
		if r.schema.HasNulls {
			var p bool
			if p, err = r.present.Next(); err != nil {
				return err
			}
			batch.Vector[i].Null = !p
		}
		if !batch.Vector[i].Null {
			var precision int64
			if precision, err = r.data.NextInt64(); err != nil {
				return err
			}
			var scale int64
			if scale, err = r.secondary.NextInt64(); err != nil {
				return err
			}
			batch.Vector[i].V = api.Decimal64{Precision: precision, Scale: int(scale)}
		}
	}
	return nil
}

func (r *decimalV2Reader) Seek(rowNumber uint64) error {
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

func (r *decimalV2Reader) seek(indexEntry *pb.RowIndexEntry) error {
	if r.schema.HasNulls {
		if err := r.seekPresent(indexEntry); err != nil {
			return err
		}
	}
	var dataChunk, dataChunkOffset, dataOffset uint64
	var secondaryChunk, secondaryChunkOffset, secondaryOffset uint64
	if indexEntry != nil {
		pos := indexEntry.Positions
		if r.opts.CompressionKind == pb.CompressionKind_NONE {
			if r.schema.HasNulls {
				dataChunkOffset = pos[3]
				dataOffset = pos[4]
				secondaryChunkOffset = pos[5]
				secondaryOffset = pos[6]
			} else {
				dataChunkOffset = pos[0]
				dataOffset = pos[1]
				secondaryChunkOffset = pos[2]
				secondaryOffset = pos[3]
			}
		} else {
			if r.schema.HasNulls {
				dataChunk = pos[4]
				dataChunkOffset = pos[5]
				dataOffset = pos[6]
				secondaryChunk = pos[7]
				secondaryChunkOffset = pos[8]
				secondaryOffset = pos[9]
			} else {
				dataChunk = pos[0]
				dataChunkOffset = pos[1]
				dataOffset = pos[2]
				secondaryChunk = pos[3]
				secondaryChunkOffset = pos[4]
				secondaryOffset = pos[5]
			}
		}
	}
	if err := r.data.Seek(dataChunk, dataChunkOffset, dataOffset); err != nil {
		return err
	}
	if err := r.secondary.Seek(secondaryChunk, secondaryChunkOffset, secondaryOffset); err != nil {
		return err
	}
	return nil
}

func (r *decimalV2Reader) Close() {
	if r.schema.HasNulls {
		r.present.Close()
	}
	r.data.Close()
	r.secondary.Close()
}

func (r decimalV2Reader) checkInit() error {
	if r.schema.HasNulls && r.present == nil {
		return errors.New("init error")
	}
	if r.data == nil || r.secondary == nil {
		return errors.New("init error")
	}
	return nil
}
