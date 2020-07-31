package column

import (
	"github.com/patrickhuang888/goorc/orc"
	"github.com/patrickhuang888/goorc/orc/stream"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
)

type byteReader struct {
	*reader
	data *stream.ByteReader
}

func NewByteReader(schema *orc.TypeDescription, opts *orc.ReaderOptions, path string, numberOfRows uint64) Reader {
	return &byteReader{reader: &reader{opts: opts, schema: schema, path: path, numberOfRows: numberOfRows}}
}

func (c *byteReader) Close() error {
	c.present.Close()
	c.data.Close()
	return nil
}

// create a file for every stream
func (c *byteReader) InitStream(kind pb.Stream_Kind, encoding pb.ColumnEncoding_Kind, startOffset uint64, info *pb.Stream, path string) error {
	if kind == pb.Stream_PRESENT {
		var err error
		c.present, err = stream.NewBoolReader(c.opts, info, startOffset, path)
		return err
	}

	if kind == pb.Stream_DATA {
		var err error
		c.data, err= stream.NewByteReader(c.opts, info, startOffset, path)
		return err
	}

	return errors.New("stream kind error")
}

func (c *byteReader) Next(presents *[]bool, pFromParent bool, vec *interface{}) (rows int, err error) {
	pp := *presents

	vector := (*vec).([]byte)
	vector = vector[:0]

	if !pFromParent {
		if err := c.nextPresents(&pp); err != nil {
			return
		}
		*presents= pp
	}

	for i := 0; i < cap(vector) && c.cursor < c.numberOfRows; i++ {
		if len(pp) == 0 || (len(pp) != 0 && pp[i]) {
			v, err := c.data.Next()
			if err != nil {
				return
			}
			vector = append(vector, v)
		} else {
			vector = append(vector, 0)
		}

		c.cursor++
	}

	rows = len(vector)
	*vec = vector
	return
}

func (c *byteReader) seek(indexEntry *pb.RowIndexEntry) error {
	if !c.opts.HasIndex {
		return errors.New("no index")
	}

	pos := indexEntry.GetPositions()

	if c.present == nil {
		if c.compressionKind == pb.CompressionKind_NONE {
			if err := c.data.Seek(pos[0], 0, pos[1]); err != nil {
				return err
			}
			return nil
		}

		if err := c.data.Seek(pos[0], pos[1], pos[2]); err != nil {
			return err
		}
		return nil
	}

	if c.compressionKind == pb.CompressionKind_NONE {
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

func (c *byteReader) Seek(rowNumber uint64) error {
	if !c.opts.HasIndex {
		return errors.New("no index")
	}

	stride := rowNumber / c.opts.IndexStride
	strideOffset := rowNumber % c.opts.IndexStride

	if err := c.seek(c.index.GetEntry()[stride]); err != nil {
		return err
	}

	c.cursor = stride * c.opts.IndexStride

	for i := 0; i < int(strideOffset); i++ {
		if _, err := c.present.Next(); err != nil {
			return err
		}
		if _, err := c.data.Next(); err != nil {
			return err
		}
		c.cursor++
	}
	return nil
}
