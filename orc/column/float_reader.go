package column

import (
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/orc/stream"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
)

func NewDoubleReader(schema *api.TypeDescription, opts *config.ReaderOptions, in orcio.File, numberOfRows uint64) Reader {
	return &doubleReader{reader: &reader{schema: schema, opts: opts, in:in, numberOfRows: numberOfRows}}
}

type doubleReader struct {
	*reader
	data *stream.DoubleReader
}

func (c *doubleReader) InitStream(info *pb.Stream, encoding pb.ColumnEncoding_Kind, startOffset uint64) error {
	if info.GetKind() == pb.Stream_PRESENT {
		ic, err:= c.in.Clone()
		if err!=nil {
			return err
		}
		c.present = stream.NewBoolReader(c.opts, info, startOffset, ic)
		return nil
	}

	if info.GetKind() == pb.Stream_DATA {
		ic, err:= c.in.Clone()
		if err!=nil {
			return err
		}
		c.data = stream.NewDoubleReader(c.opts, info, startOffset, ic)
		return nil
	}

	return errors.New("stream kind error")
}

func (c *doubleReader) Next(presents *[]bool, pFromParent bool, vec *interface{}) (rows int, err error) {
	vector := (*vec).([]float64)
	vector = vector[:0]

	if !pFromParent {
		if err = c.nextPresents(presents); err != nil {
			return
		}
	}

	for i := 0; i < cap(vector) && c.cursor < c.numberOfRows; i++ {
		if len(*presents) == 0 || (len(*presents) != 0 && (*presents)[i]) {
			var v float64
			if v, err = c.data.Next(); err != nil {
				return
			}
			vector = append(vector, v)
		} else {
			vector = append(vector, 0)
		}

		c.cursor++
	}

	*vec = vector
	rows = len(vector)
	return
}

func (c *doubleReader) seek(indexEntry *pb.RowIndexEntry) error {
	pos := indexEntry.GetPositions()

	if c.present == nil {
		if c.opts.CompressionKind == pb.CompressionKind_NONE {
			return c.data.Seek(pos[0], 0, pos[1])
		}

		return c.data.Seek(pos[0], pos[1], pos[2])
	}

	if c.opts.CompressionKind == pb.CompressionKind_NONE {
		if err := c.present.Seek(pos[0], 0, pos[1]); err != nil {
			return err
		}
		if err := c.data.Seek(pos[2], 0, pos[3]); err != nil {
			return err
		}
		return nil
	}

	if err := c.present.Seek(pos[0], pos[1], pos[2]); err != nil {
		return err
	}
	if err := c.data.Seek(pos[3], pos[4], pos[5]); err != nil {
		return err
	}
	return nil
}

func (c *doubleReader) Seek(rowNumber uint64) error {
	if !c.opts.HasIndex {
		return errors.New("no index")
	}

	stride := rowNumber / c.opts.IndexStride
	strideOffset := rowNumber % (stride * c.opts.IndexStride)

	if err := c.seek(c.index.GetEntry()[stride]); err != nil {
		return err
	}

	c.cursor = stride * c.opts.IndexStride

	var pp []bool
	if c.present != nil {
		pp = make([]bool, strideOffset)
	}
	vv := make([]int64, strideOffset)
	var vec *interface{}
	*vec = vv

	if _, err := c.Next(&pp, false, vec); err != nil {
		return err
	}

	c.cursor += strideOffset
	return nil
}

func (c *doubleReader) Close() {
	if c.present != nil {
		c.present.Close()
	}
	c.data.Close()
}
