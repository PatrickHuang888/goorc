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

func NewFloatReader(schema *api.TypeDescription, opts *config.ReaderOptions, f orcio.File) Reader {
	return &floatReader{reader: &reader{schema: schema, opts: opts, f: f}}
}

type floatReader struct {
	*reader
	data *stream.FloatReader
}

func (r *floatReader) InitStream(info *pb.Stream, startOffset uint64) error {
	if info.GetKind() == pb.Stream_PRESENT {
		ic, err := r.f.Clone()
		if err != nil {
			return err
		}
		r.present = stream.NewBoolReader(r.opts, info, startOffset, ic)
		return nil
	}

	if info.GetKind() == pb.Stream_DATA {
		ic, err := r.f.Clone()
		if err != nil {
			return err
		}
		r.data = stream.NewFloatReader(r.opts, info, startOffset, ic)
		return nil
	}
	return errors.New("stream kind error")
}

func (r *floatReader) Next() (value api.Value, err error) {
	if err = r.checkStreams(); err != nil {
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
		value.V, err = r.data.Next()
		if err != nil {
			return
		}
	}
	return
}

func (r *floatReader) Seek(rowNumber uint64) error {
	// todo:
	return nil
}

func (r *floatReader) Close() {
	if r.schema.HasNulls {
		r.present.Close()
	}
	r.data.Close()
}

func (r floatReader) checkStreams() error {
	if r.data == nil {
		return errors.New("stream data not initialized!")
	}
	if r.schema.HasNulls && r.present == nil {
		return errors.New("stream present not initialized!")
	}
	return nil
}

func NewDoubleReader(schema *api.TypeDescription, opts *config.ReaderOptions, f orcio.File) Reader {
	return &doubleReader{reader: &reader{schema: schema, opts: opts, f: f}}
}

type doubleReader struct {
	*reader
	data *stream.DoubleReader
}

func (r *doubleReader) InitStream(info *pb.Stream, startOffset uint64) error {
	if info.GetKind() == pb.Stream_PRESENT {
		ic, err := r.f.Clone()
		if err != nil {
			return err
		}
		r.present = stream.NewBoolReader(r.opts, info, startOffset, ic)
		return nil
	}

	if info.GetKind() == pb.Stream_DATA {
		ic, err := r.f.Clone()
		if err != nil {
			return err
		}
		r.data = stream.NewDoubleReader(r.opts, info, startOffset, ic)
		return nil
	}

	return errors.New("stream kind error")
}

func (r *doubleReader) Next() (value api.Value, err error) {
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
		value.V, err = r.data.Next()
		if err != nil {
			return
		}
	}
	return
}

func (r *doubleReader) seek(indexEntry *pb.RowIndexEntry) error {
	// todo:
	/*pos := indexEntry.GetPositions()

	if r.present == nil {
		if r.opts.CompressionKind == pb.CompressionKind_NONE {
			return r.data.Seek(pos[0], 0, pos[1])
		}

		return r.data.Seek(pos[0], pos[1], pos[2])
	}

	if r.opts.CompressionKind == pb.CompressionKind_NONE {
		if err := r.present.Seek(pos[0], 0, pos[1]); err != nil {
			return err
		}
		if err := r.data.Seek(pos[2], 0, pos[3]); err != nil {
			return err
		}
		return nil
	}

	if err := r.present.Seek(pos[0], pos[1], pos[2]); err != nil {
		return err
	}
	if err := r.data.Seek(pos[3], pos[4], pos[5]); err != nil {
		return err
	}*/
	return nil
}

func (r *doubleReader) Seek(rowNumber uint64) error {
	if !r.opts.HasIndex {
		return errors.New("no index")
	}

	stride := rowNumber / uint64(r.opts.IndexStride)
	strideOffset := rowNumber % (stride * uint64(r.opts.IndexStride))

	if err := r.seek(r.index.GetEntry()[stride]); err != nil {
		return err
	}

	for i := 0; i < int(strideOffset); i++ {
		if _, err := r.Next(); err != nil {
			return err
		}
	}
	return nil
}

func (r *doubleReader) Close() {
	if r.schema.HasNulls {
		r.present.Close()
	}
	r.data.Close()
}

func (r doubleReader) checkInit() error {
	if r.data == nil {
		return errors.New("stream data not initialized!")
	}
	if r.schema.HasNulls && r.present == nil {
		return errors.New("stream present not initialized!")
	}
	return nil
}

func newFloatWriter(schema *api.TypeDescription, opts *config.WriterOptions) Writer {
	// todo:stats
	var present *stream.Writer
	if schema.HasNulls {
		present = stream.NewBoolWriter(schema.Id, pb.Stream_PRESENT, opts)
	}
	base := &writer{schema: schema, opts: opts, present: present}
	data := stream.NewFloatWriter(schema.Id, pb.Stream_DATA, opts)
	return &floatWriter{base, data}
}

func newDoubleWriter(schema *api.TypeDescription, opts *config.WriterOptions) Writer {
	// todo:stats
	var present *stream.Writer
	if schema.HasNulls {
		present = stream.NewBoolWriter(schema.Id, pb.Stream_PRESENT, opts)
	}
	base := &writer{schema: schema, opts: opts, present: present}
	data := stream.NewDoubleWriter(schema.Id, pb.Stream_DATA, opts)
	return &floatWriter{base, data}
}

type floatWriter struct {
	*writer
	data *stream.Writer
}

func (w *floatWriter) Write(value api.Value) error {
	if w.schema.HasNulls {
		if err := w.present.Write(!value.Null); err != nil {
			return err
		}
	}

	if value.Null {
		return nil
	}

	if err := w.data.Write(value.V); err != nil {
		return err
	}

	// todo: stats
	// todo: index
	return nil
}

func (w *floatWriter) Flush() error {
	w.flushed = true

	if w.schema.HasNulls {
		if err := w.present.Flush(); err != nil {
			return err
		}
	}
	if err := w.data.Flush(); err != nil {
		return err
	}

	// todo: stats
	// todo: index
	return nil
}

func (w *floatWriter) WriteOut(out io.Writer) (n int64, err error) {
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

func (w floatWriter) GetStreamInfos() []*pb.Stream {
	if w.schema.HasNulls {
		return []*pb.Stream{w.present.Info(), w.data.Info()}
	}
	return []*pb.Stream{w.data.Info()}
}

func (w *floatWriter) Reset() {
	w.reset()
	w.data.Reset()
}

func (w floatWriter) Size() int {
	if w.schema.HasNulls {
		return w.present.Size() + w.data.Size()
	}
	return w.data.Size()
}
