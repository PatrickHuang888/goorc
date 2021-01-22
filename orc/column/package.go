package column

import (
	"io"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/pb/pb"
)

var logger = log.New()

func SetLogLevel(level log.Level) {
	logger.SetLevel(level)
}

type Reader interface {
	//InitChildren(children []Reader) error
	InitIndex(startOffset uint64, length uint64) error
	InitStream(info *pb.Stream, startOffset uint64) error

	Next() (value api.Value, err error)

	NextBatch(vector []api.Value) error

	// Seek seek to row number offset to current stripe
	// if column is struct (or like)  children, and struct has present stream, then
	// seek to non-null row that is calculated by parent
	// next call on Next() will not include current row been seeked
	Seek(rowNumber uint64) error

	//Children() []Reader

	Close()
}

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

func NewReader(schema *api.TypeDescription, opts *config.ReaderOptions, in orcio.File) (Reader, error) {
	switch schema.Kind {
	case pb.Type_SHORT:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			return NewIntV2Reader(schema, opts, in, BitsSmallInt), nil
		}
		return nil, errors.New("not impl")
	case pb.Type_INT:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			return NewIntV2Reader(schema, opts, in, BitsInt), nil
		}
		return nil, errors.New("not impl")
	case pb.Type_LONG:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			return NewIntV2Reader(schema, opts, in, BitsBigInt), nil
		}
		return nil, errors.New("not impl")
	case pb.Type_FLOAT:
		if schema.Encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("column encoding error")
		}
		return NewFloatReader(schema, opts, in, false), nil

	case pb.Type_DOUBLE:
		if schema.Encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("column encoding error")
		}
		return NewFloatReader(schema, opts, in, true), nil

	case pb.Type_STRING:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			return NewStringDirectV2Reader(opts, schema, in), nil
		}
		if schema.Encoding == pb.ColumnEncoding_DICTIONARY_V2 {
			return NewStringDictionaryV2Reader(opts, schema, in), nil
		}
		return nil, errors.New("column encoding error")

	case pb.Type_BOOLEAN:
		if schema.Encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("bool column encoding error")
		}
		return NewBoolReader(schema, opts, in), nil

	case pb.Type_BYTE: // tinyint
		if schema.Encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("tinyint column encoding error")
		}
		return NewByteReader(schema, opts, in), nil

	case pb.Type_BINARY:
		if schema.Encoding == pb.ColumnEncoding_DIRECT {
			// todo:
			return nil, errors.New("not impl")
		}
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			return NewBinaryV2Reader(opts, schema, in), nil
		}
		return nil, errors.New("binary column encoding error")

	case pb.Type_DECIMAL:
		if schema.Encoding == pb.ColumnEncoding_DIRECT {
			// todo:
			return nil, errors.New("not impl")
		}
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			return NewDecimal64V2Reader(schema, opts, in), nil
		}
		return nil, errors.New("column encoding error")

	case pb.Type_DATE:
		if schema.Encoding == pb.ColumnEncoding_DIRECT {
			// todo:
			return nil, errors.New("not impl")
		}
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			return NewDateV2Reader(schema, opts, in), nil
		}
		return nil, errors.New("column encoding error")

	case pb.Type_TIMESTAMP:
		if schema.Encoding == pb.ColumnEncoding_DIRECT {
			// todo:
			return nil, errors.New("not impl")
		}

		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			// improve: local
			return NewTimestampV2Reader(schema, opts, in, time.Local), nil
		}
		return nil, errors.New("column encoding error")

	case pb.Type_STRUCT:
		if schema.Encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("encoding error")
		}
		return NewStructReader(schema, opts, in), nil

	case pb.Type_LIST:
		if schema.Encoding == pb.ColumnEncoding_DIRECT {
			// todo:
			return nil, errors.New("not impl")
		}
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			return NewListV2Reader(schema, opts, in), nil
		}
		return nil, errors.New("encoding error")

	case pb.Type_MAP:
		if schema.Encoding == pb.ColumnEncoding_DIRECT {
			// todo:
			return nil, errors.New("not impl")
		}
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			return NewMapV2Reader(schema, opts, in), nil
			break
		}
		return nil, errors.New("encoding error")

	case pb.Type_UNION:
		if schema.Encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("column encoding error")
		}
		//todo:
		return nil, errors.New("not impl")

	default:
		return nil, errors.New("type unknown")
	}

	return nil, nil
}

func NewWriter(schema *api.TypeDescription, opts *config.WriterOptions) (Writer, error) {
	switch schema.Kind {
	case pb.Type_SHORT:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			return newIntV2Writer(schema, opts, BitsSmallInt), nil
		}
		return nil, errors.New("encoding not impl")
	case pb.Type_INT:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			return newIntV2Writer(schema, opts, BitsInt), nil
		}
		return nil, errors.New("encoding not impl")
	case pb.Type_LONG:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			return newIntV2Writer(schema, opts, BitsBigInt), nil
		}
		return nil, errors.New("encoding not impl")

	case pb.Type_FLOAT:
		return newFloatWriter(schema, opts, false), nil
	case pb.Type_DOUBLE:
		return newFloatWriter(schema, opts, true), nil

	case pb.Type_CHAR:
		fallthrough
	case pb.Type_VARCHAR:
		fallthrough
	case pb.Type_STRING:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			return newStringDirectV2Writer(schema, opts), nil
		}
		if schema.Encoding == pb.ColumnEncoding_DICTIONARY_V2 {
			return NewStringDictionaryV2Writer(schema, opts), nil
		}
		return nil, errors.New("encoding not impl")

	case pb.Type_BOOLEAN:
		if schema.Encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("encoding error")
		}
		return newBoolWriter(schema, opts), nil

	case pb.Type_BYTE:
		if schema.Encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("encoding error")
		}
		return newByteWriter(schema, opts), nil

	case pb.Type_BINARY:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			return NewBinaryV2Writer(schema, opts), nil
		}
		return nil, errors.New("encoding not impl")

	case pb.Type_DECIMAL:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			return NewDecimal64V2Writer(schema, opts), nil
		}
		return nil, errors.New("encoding not impl")

	case pb.Type_DATE:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			return NewDateV2Writer(schema, opts), nil
		}
		return nil, errors.New("encoding not impl")

	case pb.Type_TIMESTAMP:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			return NewTimestampV2Writer(schema, opts), nil
		}
		return nil, errors.New("encoding not impl")

	case pb.Type_STRUCT:
		if schema.Encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("encoding error")
		}
		return newStructWriter(schema, opts), nil

	case pb.Type_LIST:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			// todo:
			return nil, errors.New("not impl")
		}
		return nil, errors.New("encoding error")

	case pb.Type_MAP:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			// todo:
			return nil, errors.New("not impl")
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
}
