package column

import (
	"fmt"
	"github.com/patrickhuang888/goorc/orc"
	"github.com/patrickhuang888/goorc/orc/stream"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"strings"
)

type reader struct {
	schema *orc.TypeDescription

	present *stream.BoolReader

	numberOfRows uint64 // from stripe information
	cursor       uint64

	compressionKind pb.CompressionKind

	readIndex bool
	indexStride uint64
}

func (r *reader) String() string {
	sb := strings.Builder{}
	fmt.Fprintf(&sb, "id %d, ", r.schema.Id)
	fmt.Fprintf(&sb, "kind %stream, ", r.schema.Kind.String())
	fmt.Fprintf(&sb, "encoding %s, ", r.schema.Encoding.String())
	fmt.Fprintf(&sb, "number of rows %d, ", r.numberOfRows)
	fmt.Fprintf(&sb, "read cursor %d", r.cursor)
	return sb.String()
}

func (r *reader) nextPresents(batch *orc.ColumnVector) error {
	// rethink: writer always init present stream writer first,
	// while reader's present stream init at prepare()
	if r.present != nil {
		if batch.Presents == nil {
			return errors.Errorf("reader has present stream, batch should have presents slice")
		}
		batch.Presents = batch.Presents[:0]

		count := r.cursor
		for i := 0; count < r.numberOfRows && i < cap(batch.Presents); i++ {
			v, err := r.present.Next()
			if err != nil {
				return err
			}
			batch.Presents = append(batch.Presents, v)
			count++
		}
		log.Debugf("column %d has read %d presents values", r.schema.Id, len(batch.Presents))
	}
	return nil
}

type ByteReader struct {
	*reader
	data *stream.ByteReader
}

func (c *ByteReader) Next(batch *orc.ColumnVector) error {
	vector := batch.Vector.([]byte)
	vector = vector[:0]

	if err := c.nextPresents(batch); err != nil {
		return err
	}

	for i := 0; i < cap(vector) && c.cursor < c.numberOfRows; i++ {

		if len(batch.Presents) == 0 || (len(batch.Presents) != 0 && batch.Presents[i]) {
			v, err := c.data.Next()
			if err != nil {
				return err
			}
			vector = append(vector, v)

		} else {
			vector = append(vector, 0)
		}

		c.cursor++
	}

	batch.Vector = vector
	batch.ReadRows = len(vector)
	return nil
}

func (c *ByteReader) Seek(indexEntry *pb.RowIndexEntry) error {
	if !c.readIndex {
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

func (c *ByteReader) Locate(rowNumber uint64, index *pb.RowIndex) error  {
	if !c.readIndex {
		return errors.New("no index")
	}

	stride:= int(rowNumber/c.indexStride)
	stridOffset:=int(rowNumber % c.indexStride)

	if err:=c.Seek(index.GetEntry()[stride]);err!=nil {
		return err
	}


	return nil
}
