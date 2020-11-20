package column

import (
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	"github.com/patrickhuang888/goorc/orc/stream"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
	"io"
)

type Writer interface {
	Write(value api.Value) error

	// Flush flush stream(index, data) when write out stripe(reach stripe size), reach index stride or close file
	// update ColumnStats.BytesOnDisk and index before stripe written out
	// flush once before write out to store
	Flush() error

	// WriteOut to writer, should flush first, because index will be got after flush and
	// write out before data. n written total data length
	WriteOut(out io.Writer) (n int64, err error)

	//after flush
	GetIndex() *pb.RowIndex

	// GetStreamInfos get no-non streams, used for writing stripe footer after flush
	GetStreamInfos() []*pb.Stream

	// data will be updated after flush
	GetStats() *pb.ColumnStatistics

	// Reset for column writer reset after stripe write out
	Reset()

	//sum of streams size, used for stripe flushing condition, data size in memory
	Size() int
}

func CreateWriter(schema *api.TypeDescription, opts *config.WriterOptions) (w Writer, err error) {
	switch schema.Kind {
	case pb.Type_SHORT:
		fallthrough
	case pb.Type_INT:
		fallthrough
	case pb.Type_LONG:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			w = newIntV2Writer(schema, opts)
			break
		}
		return nil, errors.New("encoding not impl")

	case pb.Type_FLOAT:
		//writer = newFloatWriter(schema, opts)
	case pb.Type_DOUBLE:
		//writer = newDoubleWriter(schema, opts)

	case pb.Type_CHAR:
		fallthrough
	case pb.Type_VARCHAR:
		fallthrough
	case pb.Type_STRING:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			w = newStringDirectV2Writer(schema, opts)
			break
		}

		if schema.Encoding == pb.ColumnEncoding_DICTIONARY_V2 {
			//writer = newStringDictV2Writer(schema, opts)
			break
		}

		return nil, errors.New("encoding not impl")

	case pb.Type_BOOLEAN:
		if schema.Encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("encoding error")
		}
		//writer = newBoolWriter(schema, opts)

	case pb.Type_BYTE:
		if schema.Encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("encoding error")
		}
		w = newByteWriter(schema, opts)

	case pb.Type_BINARY:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			//writer = newBinaryDirectV2Writer(schema, opts)
			break
		}

		return nil, errors.New("encoding not impl")

	case pb.Type_DECIMAL:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			//writer = newDecimal64DirectV2Writer(schema, opts)
			break
		}
		return nil, errors.New("encoding not impl")

	case pb.Type_DATE:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			//writer = newDateDirectV2Writer(schema, opts)
			break
		}
		return nil, errors.New("encoding not impl")

	case pb.Type_TIMESTAMP:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			w = newTimestampV2Writer(schema, opts)
			break
		}
		return nil, errors.New("encoding not impl")

	case pb.Type_STRUCT:
		if schema.Encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("encoding error")
		}
		//writer, err = newStructWriter(schema, opts)

	case pb.Type_LIST:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			// todo:
			return nil, errors.New("not impl")
			break
		}
		return nil, errors.New("encoding error")

	case pb.Type_MAP:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			// todo:
			return nil, errors.New("not impl")
			break
		}
		return nil, errors.New("encoding not impl")

	case pb.Type_UNION:
		if schema.Encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("encoding error")
		}
		// todo:
		return nil, errors.New("not impl")

	default:
		return nil, errors.New("column kind unknown")
	}
	return
}

type writer struct {
	schema *api.TypeDescription
	opts   *config.WriterOptions

	stats *pb.ColumnStatistics

	present *stream.Writer

	children []Writer

	indexInRows int
	indexStats  *pb.ColumnStatistics
	index       *pb.RowIndex

	flushed bool
}

func (w *writer) reset() {
	w.flushed = false

	if w.schema.HasNulls {
		w.present.Reset()
	}

	if w.opts.WriteIndex {
		w.indexInRows = 0
		w.index.Reset()
		w.indexStats.Reset()
		w.indexStats.HasNull = new(bool)
		w.indexStats.NumberOfValues = new(uint64)
		w.indexStats.BytesOnDisk = new(uint64)
	}

	// stats will not reset, it's for whole writer
}

func (w writer) GetIndex() *pb.RowIndex {
	return w.index
}

func (w writer) GetStats() *pb.ColumnStatistics {
	return w.stats
}

