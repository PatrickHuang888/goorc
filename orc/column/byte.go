package column

import (
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/orc/stream"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
)

type byteWriter struct {
	*writer
	data *stream.Writer
}

func (c *byteWriter) Write(value api.Value) error {
	var err error
	present:=true

	if c.schema.HasNulls {
		if err=c.present.Write(!value.Null);err!=nil {
			return err
		}
		if value.Null {
			present= false
		}
	}

	if present{
		if err = c.data.Write(value.V); err != nil {
			return err
		}
	}

	if c.opts.WriteIndex {
		c.doIndex()

		if c.indexStats.BinaryStatistics == nil {
			c.indexStats.BinaryStatistics = &pb.BinaryStatistics{Sum: new(int64)}
		}
		*c.indexStats.BinaryStatistics.Sum++
		*c.indexStats.NumberOfValues++
	}

	*c.stats.BinaryStatistics.Sum++
	*c.stats.NumberOfValues++
	return nil
}

func (c *byteWriter) Size() int {
	return c.present.Size() + c.data.Size()
}

func newByteWriter(schema *api.TypeDescription, opts *config.WriterOptions) Writer {
	stats := &pb.ColumnStatistics{NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64), BinaryStatistics: &pb.BinaryStatistics{Sum: new(int64)}}
	base := &writer{schema: schema, present: stream.NewBoolWriter(schema.Id, pb.Stream_PRESENT, opts), stats: stats}
	data := stream.NewByteWriter(schema.Id, pb.Stream_DATA, opts)
	return &byteWriter{base, data}
}

/*func (c *byteWriter) Write(presents []bool, presentsFromParent bool, vec interface{}) (rows int, err error) {
	vector := vec.([]byte)
	rows = len(vector)

	if len(presents) == 0 {
		for _, v := range vector {
			if err = c.writeValue(v); err != nil {
				return
			}
			if c.writeIndex {
				c.doIndex()
			}
		}
		return
	}

	if len(presents) != len(vector) {
		return 0, errors.New("presents error")
	}

	if !presentsFromParent {
		*c.stats.HasNull = true
	}

	for i, p := range presents {

		if !presentsFromParent {
			if err = c.present.Write(p); err != nil {
				return
			}
		}

		if p {
			if err = c.writeValue(vector[i]); err != nil {
				return
			}
		}

		if c.writeIndex {
			c.doIndex()
		}
	}

	return
}*/

func (c *byteWriter) doIndex() {
	c.indexInRows++

	if c.indexInRows >= c.opts.IndexStride {
		c.present.MarkPosition()
		c.data.MarkPosition()

		entry := &pb.RowIndexEntry{Statistics: c.indexStats}
		c.indexEntries = append(c.indexEntries, entry)
		c.indexStats = &pb.ColumnStatistics{BinaryStatistics: &pb.BinaryStatistics{Sum: new(int64)}, NumberOfValues: new(uint64), HasNull: new(bool), BytesOnDisk: new(uint64)}

		c.indexInRows = 0
	}
}

func (c *byteWriter) Flush() error {
	var err error

	if err = c.present.Flush(); err != nil {
		return err
	}
	if err = c.data.Flush(); err != nil {
		return err
	}

	*c.stats.BytesOnDisk += c.present.Info().GetLength()
	*c.stats.BytesOnDisk += c.data.Info().GetLength()
	return nil
}

func (c *byteWriter) GetStreamInfos() []*pb.Stream {
	var ss []*pb.Stream
	if c.present.Info().GetLength() != 0 {
		ss = append(ss, c.present.Info())
	}
	ss = append(ss, c.data.Info())
	return ss
}

func (c *byteWriter) GetStats() *pb.ColumnStatistics {
	return c.stats
}

func (c *byteWriter) Reset() {
	c.reset()
	c.data.Reset()
}

func (c *byteWriter) WriteOut(out io.Writer) (n int64, err error) {
	var np, nd int64
	if np, err = c.present.WriteOut(out); err != nil {
		return
	}
	if nd, err = c.data.WriteOut(out); err != nil {
		return
	}
	n = np + nd
	return
}

// after flush
func (c *byteWriter) GetIndex() *pb.RowIndex {
	if c.opts.WriteIndex {
		index := &pb.RowIndex{}

		pp := c.present.GetAndClearPositions()
		dp := c.data.GetAndClearPositions()

		if len(c.indexEntries) != len(pp) || len(c.indexEntries) != len(dp) {
			log.Errorf("index entry and position error")
			return nil
		}

		for i, e := range c.indexEntries {
			e.Positions = append(e.Positions, pp[i]...)
			e.Positions = append(e.Positions, dp[i]...)
		}
		index.Entry = c.indexEntries
		return index
	}

	return nil
}

type byteReader struct {
	*reader
	data *stream.ByteReader
}

func NewByteReader(schema *api.TypeDescription, opts *config.ReaderOptions, in orcio.File, numberOfRows uint64) Reader {
	return &byteReader{reader: &reader{opts: opts, schema: schema, in: in, numberOfRows: numberOfRows}}
}

// create a input for every stream
func (c *byteReader) InitStream(info *pb.Stream, encoding pb.ColumnEncoding_Kind, startOffset uint64) error {
	if info.GetKind() == pb.Stream_PRESENT {
		ic, err := c.in.Clone()
		if err != nil {
			return err
		}
		c.present = stream.NewBoolReader(c.opts, info, startOffset, ic)
		ic.Seek(int64(startOffset), 0)
		return nil
	}

	if info.GetKind() == pb.Stream_DATA {
		ic, err := c.in.Clone()
		if err != nil {
			return err
		}
		c.data = stream.NewByteReader(c.opts, info, startOffset, ic)
		ic.Seek(int64(startOffset), 0)
		return nil
	}

	return errors.New("stream kind error")
}

func (c *byteReader) Next(values *[]api.Value) error {

	if c.schema.HasNulls {
		c.present.Next()
	}

	for i := 0; i < cap(vector) && c.cursor < c.numberOfRows; i++ {
		if len(*presents) == 0 || (len(*presents) != 0 && (*presents)[i]) {
			var v byte
			v, err = c.data.Next()
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

func (c *byteReader) Seek(rowNumber uint64) error {
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

	vv := make([]byte, strideOffset)
	var v *interface{}
	*v = vv

	if _, err := c.Next(&pp, false, v); err != nil {
		return err
	}

	c.cursor += strideOffset
	return nil
}

func (c *byteReader) Close() {
	c.present.Close()
	c.data.Close()
}
