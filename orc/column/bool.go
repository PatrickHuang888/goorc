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

func newBoolWriter(schema *api.TypeDescription, opts *config.WriterOptions) Writer {
	var present *stream.Writer
	if schema.HasNulls {
		present = stream.NewBoolWriter(schema.Id, pb.Stream_PRESENT, opts)
	}
	base := &writer{schema: schema, opts: opts, present: present}
	data := stream.NewBoolWriter(schema.Id, pb.Stream_DATA, opts)
	return &boolWriter{base, data}
}

type boolWriter struct {
	*writer
	data *stream.Writer
}

func (w *boolWriter) Write(value api.Value) error {
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
	// todo: write stats
	// todo: write index
	return nil
}

func (w *boolWriter) Flush() error {
	if w.schema.HasNulls {
		if err := w.present.Flush(); err != nil {
			return err
		}
	}
	if err := w.data.Flush(); err != nil {
		return err
	}

	w.flushed = true
	return nil
}

func (w *boolWriter) WriteOut(out io.Writer) (n int64, err error) {
	if !w.flushed {
		err = errors.New("not flushed!")
		return
	}

	var pn, dn int64
	if w.schema.HasNulls {
		if pn, err = w.present.WriteOut(out); err != nil {
			return 0, err
		}
	}
	if dn, err = w.data.WriteOut(out); err != nil {
		return 0, err
	}
	return pn + dn, nil
}

func (w boolWriter) GetStreamInfos() []*pb.Stream {
	if w.schema.HasNulls {
		return []*pb.Stream{w.present.Info(), w.data.Info()}
	}
	return []*pb.Stream{w.data.Info()}
}

func (w *boolWriter) Reset() {
	w.reset()
	w.data.Reset()
}

func (w boolWriter) Size() int {
	if w.schema.HasNulls {
		return w.present.Size() + w.data.Size()
	}
	return w.data.Size()
}

func newBoolReader(schema *api.TypeDescription, opts *config.ReaderOptions, f orcio.File) Reader {
	return &boolReader{reader: &reader{opts: opts, schema: schema, f: f}}
}

type boolReader struct {
	*reader
	data *stream.BoolReader
}

func (r *boolReader) InitIndex(startOffset uint64, length uint64) error {
	// todo:
	return nil
}

func (r *boolReader) InitStream(info *pb.Stream, startOffset uint64) error {
	if info.GetKind() == pb.Stream_PRESENT {
		if !r.schema.HasNulls {
			return errors.New("column schema has no nulls")
		}
		ic, err := r.f.Clone()
		if err != nil {
			return err
		}
		r.present = stream.NewBoolReader(r.opts, info, startOffset, ic)
		ic.Seek(int64(startOffset), 0)
		return nil
	}
	if info.GetKind() == pb.Stream_DATA {
		ic, err := r.f.Clone()
		if err != nil {
			return err
		}
		r.data = stream.NewBoolReader(r.opts, info, startOffset, ic)
		ic.Seek(int64(startOffset), 0)
		return nil
	}
	return errors.New("stream kind error")
}

func (r *boolReader) Next() (value api.Value, err error) {
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

func (r *boolReader) Seek(rowNumber uint64) error {
	// todo:
	return nil
}

func (r *boolReader) Close() {
	if r.schema.HasNulls {
		r.present.Close()
	}
	r.data.Close()
}

func (r boolReader) checkInit() error {
	if r.data == nil {
		return errors.New("stream data not initialized!")
	}
	if r.schema.HasNulls && r.present == nil {
		return errors.New("stream present not initialized!")
	}
	return nil
}
