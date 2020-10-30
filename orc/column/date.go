package column

import (
	"errors"
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/orc/stream"
	"github.com/patrickhuang888/goorc/pb/pb"
)

func NewDateV2Reader(schema *api.TypeDescription, opts *config.ReaderOptions, f orcio.File) Reader {
	return &dateV2Reader{reader: &reader{schema: schema, opts: opts, f:f}}
}

type dateV2Reader struct {
	*reader
	present *stream.BoolReader
	data *stream.IntRLV2Reader
}

func (c *dateV2Reader) InitStream(info *pb.Stream,  startOffset uint64) error {

	if c.schema.Encoding == pb.ColumnEncoding_DIRECT {
		err := errors.New("encoding direct not impl")
		return err
	}

	if info.GetKind() == pb.Stream_PRESENT {
		ic, err:= c.f.Clone()
		if err!=nil {
			return err
		}
		c.present= stream.NewBoolReader(c.opts, info, startOffset, ic)
		return nil
	}

	if info.GetKind() == pb.Stream_DATA {
		ic, err:= c.f.Clone()
		if err!=nil {
			return err
		}
		c.data = stream.NewRLV2Reader(c.opts, info, startOffset, true, ic)
		return err
	}

	return errors.New("stream unknown")
}

func (c *dateV2Reader) Next(values []api.Value) error {
	/*vector := (*vec).([]api.Date)
	vector = vector[:0]

	if !pFromParent {
		if err = c.nextPresents(presents); err != nil {
			return
		}
	}

	for i := 0; i < cap(vector) && c.cursor < c.numberOfRows; i++ {

		if len(*presents) == 0 || (len(*presents) != 0 && (*presents)[i]) {
			var v int64
			v, err = c.data.NextInt64()
			if err != nil {
				return
			}
			vector = append(vector, api.FromDays(v))
		} else {
			vector = append(vector, api.Date{})
		}

		c.cursor++
	}

	*vec = vector
	rows = len(vector)*/
	return nil
}

func (c *dateV2Reader) seek(indexEntry *pb.RowIndexEntry) error {
	pos := indexEntry.GetPositions()

	if c.present == nil {
		if c.opts.CompressionKind == pb.CompressionKind_NONE {
			return c.data.Seek(pos[0], 0, pos[1])
		}

		return c.data.Seek(pos[0], pos[1], pos[2])
	}

	if c.opts.CompressionKind == pb.CompressionKind_NONE {
		if c.present != nil {
			if err := c.present.Seek(pos[0], 0, pos[1]); err != nil {
				return err
			}
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

func (c *dateV2Reader) Seek(rowNumber uint64) error {
	if !c.opts.HasIndex {
		return errors.New("no index")
	}

	stride := rowNumber / c.opts.IndexStride
	offsetInStride := rowNumber % (stride * c.opts.IndexStride)

	if err := c.seek(c.index.GetEntry()[stride]); err != nil {
		return err
	}

	//c.cursor = stride * c.opts.IndexStride

	for i := 0; i < int(offsetInStride); i++ {
		if c.present != nil {
			if _, err := c.present.Next(); err != nil {
				return err
			}
		}
		if _, err := c.data.NextInt64(); err != nil {
			return err
		}
		//c.cursor++
	}
	return nil
}

func (c *dateV2Reader) Close() {
	if c.present != nil {
		c.present.Close()
	}
	c.data.Close()
}
