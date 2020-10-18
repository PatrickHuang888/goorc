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
	Write(presents []bool, presentsFromParent bool, vec interface{}) (rows int, err error)

	// Flush flush index, streams and update stats when stripe should be written out
	Flush() error

	// flush first then write out, because maybe there is index to write first
	WriteOut(out io.Writer) (n int64, err error)

	//after flush
	GetIndex() *pb.RowIndex

	// after flush, used for writing stripe footer
	GetStreamInfos() []*pb.Stream
	// after flush
	GetStats() *pb.ColumnStatistics

	// for stripe reset
	Reset()

	//sum of streams size, used for stripe flushing condition, data size in memory
	//Size() int
}

func CreateWriter(schema *api.TypeDescription, opts *config.WriterOptions) (w Writer, err error) {
	switch schema.Kind {
	case pb.Type_SHORT:
		fallthrough
	case pb.Type_INT:
		fallthrough
	case pb.Type_LONG:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			w = newLongV2Writer(schema, opts)
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
			//writer = newStringDirectV2Writer(schema, opts)
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
			//writer = newTimestampDirectV2Writer(schema, opts)
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
	stats  *pb.ColumnStatistics

	present stream.BoolWriter

	children []Writer

	writeIndex  bool
	indexStride int

	indexInRows int
	indexStats  *pb.ColumnStatistics
	indexEntries     []*pb.RowIndexEntry
}

func (w *writer) reset() {
	//w.present.Reset()

	w.indexInRows = 0
	w.indexEntries = w.indexEntries[:0]
	w.indexStats.Reset()
	w.indexStats.HasNull = new(bool)
	w.indexStats.NumberOfValues = new(uint64)
	w.indexStats.BytesOnDisk = new(uint64)

	// stats will not reset, it's for whole writer
}
