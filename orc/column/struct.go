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

type structReader struct {
	*reader
	//children []Reader
}

func NewStructReader(schema *api.TypeDescription, opts *config.ReaderOptions, f orcio.File) Reader {
	return &structReader{reader: &reader{opts: opts, schema: schema, f: f}}
}

func (c *structReader) InitStream(info *pb.Stream, startOffset uint64) error {
	if info.GetKind() == pb.Stream_PRESENT {
		ic, err := c.f.Clone()
		if err != nil {
			return err
		}
		if _, err := ic.Seek(int64(startOffset), io.SeekStart); err != nil {
			return err
		}
		c.present = stream.NewBoolReader(c.opts, info, startOffset, ic)
		return nil
	}

	return errors.New("struct column no stream other than present")
}

func (c *structReader) Next() (value api.Value, err error) {
	if err = c.checkInit(); err != nil {
		return
	}

	if c.schema.HasNulls {
		var p bool
		if p, err = c.present.Next(); err != nil {
			return
		}
		value.Null = !p
	}

	//c.cursor += uint64(len(values))

	/*pp := *presents

	if !pFromParent {
		if err = s.nextPresents(&pp); err != nil {
			return
		}
		*presents = pp
	}

	vector := (*vec).([]*api.ColumnVector)

	var rt int
	for i, child := range s.children {
		if !pFromParent && len(pp) != 0 {
			if rt, err = child.Next(&pp, true, &vector[i].Vector); err != nil {
				return
			}
		} else {
			if rt, err = child.Next(&pp, pFromParent, &vector[i].Vector); err != nil {
				return
			}
		}
	}

	// reassure: no present, so readrows using children readrows?
	if len(pp) == 0 {
		rows = rt
	} else {
		rows = len(pp)
	}

	s.cursor += uint64(rows)*/

	return
}

func (s *structReader) Seek(rowNumber uint64) error {
	if err := s.checkInit(); err != nil {
		return err
	}

	//todo: seek present

	/*for _, child := range s.children {
		if err := child.Seek(rowNumber); err != nil {
			return err
		}
	}*/
	return nil
}

func (c structReader) checkInit() error {
	if c.schema.HasNulls && c.present == nil {
		return errors.New("stream present not initialized!")
	}
	return nil
}

func (c *structReader) Close() {
	if c.schema.HasNulls {
		c.present.Close()
	}
}

func newStructWriter(schema *api.TypeDescription, opts *config.WriterOptions) Writer {
	// todo: struct stats
	//stats := &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64), BinaryStatistics: &pb.BinaryStatistics{Sum: new(int64)}}
	var present *stream.Writer
	if schema.HasNulls {
		//*stats.HasNull = true
		present = stream.NewBoolWriter(schema.Id, pb.Stream_PRESENT, opts)
	}
	return &structWriter{&writer{schema: schema, opts: opts, present: present}}
}

type structWriter struct {
	*writer
}

func (w *structWriter) Write(value api.Value) error {
	if w.schema.HasNulls {
		if err := w.present.Write(!value.Null); err != nil {
			return err
		}
		// todo: write stats?
		// todo: index
	}
	return nil
}

func (w *structWriter) Flush() error {
	if w.schema.HasNulls {
		if err:=w.present.Flush();err!=nil {
			return err
		}
		w.flushed= true
	}
	return nil
}

func (w *structWriter) WriteOut(out io.Writer) (n int64, err error) {
	if !w.flushed {
		err = errors.New("not flushed")
		return
	}

	if w.schema.HasNulls {
		if !w.flushed {
			err = errors.New("not flushed!")
			return
		}
		return w.present.WriteOut(out)
	}
	return 0, nil
}

func (w structWriter) GetStreamInfos() []*pb.Stream {
	if w.schema.HasNulls {
		return []*pb.Stream{w.present.Info()}
	}
	return nil
}

func (w structWriter) Reset() {
	if w.schema.HasNulls {
		w.reset()
	}
}

func (w structWriter) Size() int {
	if w.schema.HasNulls {
		return w.present.Size()
	}
	return 0
}
