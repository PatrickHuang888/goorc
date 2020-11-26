package column

import (
	"fmt"
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/orc/stream"
	"io"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
)

type Reader interface {
	//InitChildren(children []Reader) error
	InitIndex(startOffset uint64, length uint64) error
	InitStream(info *pb.Stream, startOffset uint64) error

	Next() (value api.Value, err error)

	// Seek seek to row number offset to current stripe
	// if column is struct (or like)  children, and struct has present stream, then
	// seek to non-null row that is calculated by parent
	Seek(rowNumber uint64) error

	//Children() []Reader

	Close()
}

type reader struct {
	f orcio.File
	schema *api.TypeDescription
	opts *config.ReaderOptions

	index *pb.RowIndex

	present *stream.BoolReader
}

func (r *reader) String() string {
	sb := strings.Builder{}
	fmt.Fprintf(&sb, "id %d, ", r.schema.Id)
	fmt.Fprintf(&sb, "kind %stream, ", r.schema.Kind.String())
	fmt.Fprintf(&sb, "encoding %s, ", r.schema.Encoding.String())
	//fmt.Fprintf(&sb, "number of rows %d, ", r.numberOfRows)
	//fmt.Fprintf(&sb, "read cursor %d", r.cursor)
	return sb.String()
}

func (r *reader) InitIndex(startOffset uint64, length uint64) error {
	var err error

	if _, err = r.f.Seek(int64(startOffset), io.SeekStart); err != nil {
		return err
	}

	var buf = make([]byte, length)
	if _, err = io.ReadFull(r.f, buf); err != nil {
		return err
	}

	return proto.Unmarshal(buf, r.index)
}

func NewReader(schema *api.TypeDescription, opts *config.ReaderOptions, in orcio.File) (Reader, error) {
	switch schema.Kind {
	case pb.Type_SHORT:
		fallthrough
	case pb.Type_INT:
		fallthrough
	case pb.Type_LONG:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			return &intV2Reader{reader:&reader{f:in, schema: schema, opts: opts}}, nil
		}
		return nil, errors.New("not impl")

	case pb.Type_FLOAT:
	// todo:

	case pb.Type_DOUBLE:
		if schema.Encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("column encoding error")
		}
		//return NewDoubleReader(schema, opts, in, numberOfRows), nil

	case pb.Type_STRING:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			return newStringDirectV2Reader(opts, schema, in), nil
			break
		}
		if schema.Encoding == pb.ColumnEncoding_DICTIONARY_V2 {
			return nil, errors.New("not impl")
			//s.columnReaders[schema.Id] = &stringDictV2Reader{treeReader: c}
			break
		}
		return nil, errors.New("column encoding error")

	case pb.Type_BOOLEAN:
		if schema.Encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("bool column encoding error")
		}
		return nil, errors.New("not impl")
		//s.columnReaders[schema.Id] = &boolReader{treeReader: c}

	case pb.Type_BYTE: // tinyint
		if schema.Encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("tinyint column encoding error")
		}
		return &byteReader{reader:&reader{f:in, schema: schema, opts: opts}}, nil

	case pb.Type_BINARY:
		if schema.Encoding == pb.ColumnEncoding_DIRECT {
			return nil, errors.New("not impl")
			break
		}
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			return nil, errors.New("not impl")
			//s.columnReaders[schema.Id] = &binaryV2Reader{treeReader: c}
			break
		}
		return nil, errors.New("binary column encoding error")

	case pb.Type_DECIMAL:
		if schema.Encoding == pb.ColumnEncoding_DIRECT {
			// todo:
			return nil, errors.New("not impl")
			break
		}
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			return nil, errors.New("not impl")
			//s.columnReaders[schema.Id] = &decimal64DirectV2Reader{treeReader: c}
			break
		}
		return nil, errors.New("column encoding error")

	case pb.Type_DATE:
		if schema.Encoding == pb.ColumnEncoding_DIRECT {
			// todo:
			return nil, errors.New("not impl")
		}
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			return nil, errors.New("not impl")
			//return NewDateV2Reader(schema, opts, in, numberOfRows), nil
		}
		return nil, errors.New("column encoding error")

	case pb.Type_TIMESTAMP:
		if schema.Encoding == pb.ColumnEncoding_DIRECT {
			// todo:
			return nil, errors.New("not impl")
			break
		}

		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			//s.columnReaders[schema.Id] = &timestampV2Reader{treeReader: c}
			return nil, errors.New("not impl")
			break
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
			// todo:

		}
		return nil, errors.New("encoding error")

	case pb.Type_MAP:
		if schema.Encoding == pb.ColumnEncoding_DIRECT {
			// todo:
			return nil, errors.New("not impl")
		}
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			// todo:
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
