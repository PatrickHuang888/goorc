package column

import (
	"errors"
	log "github.com/sirupsen/logrus"

	"github.com/patrickhuang888/goorc/orc"
	"github.com/patrickhuang888/goorc/orc/stream"
	"github.com/patrickhuang888/goorc/pb/pb"
)

func NewDateV2Reader(schema *orc.TypeDescription, opts *orc.ReaderOptions, path string, numberOfRows uint64) Reader {
	return &dateV2Reader{reader: &reader{schema: schema, opts: opts, path: path, numberOfRows: numberOfRows}}
}

type dateV2Reader struct {
	*reader
	data *stream.IntRLV2Reader
}

func (c *dateV2Reader) InitStream(kind pb.Stream_Kind, encoding pb.ColumnEncoding_Kind, startOffset uint64, info *pb.Stream, path string) error {
	var err error

	if encoding == pb.ColumnEncoding_DIRECT {
		err = errors.New("encoding direct not impl")
		return err
	}

	if kind == pb.Stream_PRESENT {
		c.present, err = stream.NewBoolReader(c.opts, info, startOffset, path)
		return err
	}

	if kind == pb.Stream_DATA {
		c.data, err = stream.NewRLV2Reader(c.opts, info, startOffset, true, path)
		return err
	}

	return errors.New("stream unknown")
}

func (c *dateV2Reader) Next(presents *[]bool, pFromParent bool, vec *interface{}) (rows int, err error) {
	vector := (*vec).([]orc.Date)
	vector = vector[:0]

	if !pFromParent {
		if err = c.nextPresents(presents); err != nil {
			return
		}
	}

	for i := 0; i < cap(vector) && c.cursor < c.numberOfRows; i++ {

		if len(*presents) == 0 || (len(*presents) != 0 && (*presents)[i]) {
			v, err := c.data.NextInt64()
			if err != nil {
				return
			}
			vector = append(vector, orc.FromDays(v))
		} else {
			vector = append(vector, orc.Date{})
		}

		c.cursor++
	}

	*vec = vector
	rows = len(vector)
	return
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
	offsetInStride := rowNumber % c.opts.IndexStride

	if err := c.seek(c.index.GetEntry()[stride]); err != nil {
		return err
	}

	c.cursor = stride * c.opts.IndexStride

	for i := 0; i < int(offsetInStride); i++ {
		if c.present != nil {
			if _, err := c.present.Next(); err != nil {
				return err
			}
		}
		if _, err := c.data.NextInt64(); err != nil {
			return err
		}
		c.cursor++
	}
	return nil
}

func (c *dateV2Reader) Close() error {
	if c.present != nil {
		if err := c.present.Close(); err != nil {
			log.Errorf("date present closing err", err)
		}
	}
	if err := c.data.Close(); err != nil {
		log.Errorf("date data closing err", err)
	}
	return nil
}
