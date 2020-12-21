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

type reader struct {
	f      orcio.File
	schema *api.TypeDescription
	opts   *config.ReaderOptions

	index *pb.RowIndex

	present *stream.BoolReader
}

func (r *reader) String() string {
	sb := strings.Builder{}
	fmt.Fprintf(&sb, "id %d, ", r.schema.Id)
	fmt.Fprintf(&sb, "kind %stream, ", r.schema.Kind.String())
	fmt.Fprintf(&sb, "encoding %s, ", r.schema.Encoding.String())
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

func (r *reader) getIndexEntryAndOffset(rowNumber uint64) (entry *pb.RowIndexEntry, offset uint64) {
	if rowNumber < uint64(r.opts.IndexStride) {
		offset = rowNumber
		return
	}
	stride := rowNumber / uint64(r.opts.IndexStride)
	offset = rowNumber % (stride * uint64(r.opts.IndexStride))
	entry = r.index.GetEntry()[stride-1]
	return
}

func NewReader(schema *api.TypeDescription, opts *config.ReaderOptions, in orcio.File) (Reader, error) {
	switch schema.Kind {
	case pb.Type_SHORT:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			return &intV2Reader{reader: &reader{f: in, schema: schema, opts: opts}, bits: BitsSmallInt}, nil
		}
		return nil, errors.New("not impl")
	case pb.Type_INT:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			return &intV2Reader{reader: &reader{f: in, schema: schema, opts: opts}, bits: BitsInt}, nil
		}
		return nil, errors.New("not impl")
	case pb.Type_LONG:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			return &intV2Reader{reader: &reader{f: in, schema: schema, opts: opts}, bits: BitsBigInt}, nil
		}
		return nil, errors.New("not impl")
	case pb.Type_FLOAT:
		if schema.Encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("column encoding error")
		}
		return &floatReader{reader: &reader{f: in, schema: schema, opts: opts}, is64: false}, nil

	case pb.Type_DOUBLE:
		if schema.Encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("column encoding error")
		}
		return &floatReader{reader: &reader{f: in, schema: schema, opts: opts}, is64: true}, nil

	case pb.Type_STRING:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			return newStringDirectV2Reader(opts, schema, in), nil
			break
		}
		if schema.Encoding == pb.ColumnEncoding_DICTIONARY_V2 {
			return nil, errors.New("not impl")
			// todo:
			break
		}
		return nil, errors.New("column encoding error")

	case pb.Type_BOOLEAN:
		if schema.Encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("bool column encoding error")
		}
		return &boolReader{reader:&reader{f:in, schema: schema, opts: opts}}, nil

	case pb.Type_BYTE: // tinyint
		if schema.Encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("tinyint column encoding error")
		}
		return &byteReader{reader: &reader{f: in, schema: schema, opts: opts}}, nil

	case pb.Type_BINARY:
		if schema.Encoding == pb.ColumnEncoding_DIRECT {
			return nil, errors.New("not impl")
			break
		}
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			return nil, errors.New("not impl")
			// todo:
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
			// todo:
			return nil, errors.New("not impl")
			break
		}
		return nil, errors.New("column encoding error")

	case pb.Type_STRUCT:
		if schema.Encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("encoding error")
		}
		// todo:
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
