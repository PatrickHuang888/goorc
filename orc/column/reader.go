package column

import (
	"fmt"
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"io"
	"os"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/patrickhuang888/goorc/orc/stream"
	"github.com/patrickhuang888/goorc/pb/pb"
)

type Reader interface {
	InitChildren(children []Reader) error
	InitIndex(startOffset uint64, length uint64, path string) error
	InitStream(kind pb.Stream_Kind, encoding pb.ColumnEncoding_Kind, startOffset uint64, info *pb.Stream) error

	Next(presents *[]bool, pFromParent bool, vec *interface{}) (int, error)

	// Seek seek to row number offset to current stripe
	// if column is struct (or like)  children, and struct has present stream, then
	// seek to non-null row that is calculated by parent
	Seek(rowNumber uint64) error

	Children() []Reader

	Close()
}

type reader struct {
	in orcio.File

	schema *api.TypeDescription

	present *stream.BoolReader

	numberOfRows uint64 // from stripe information
	cursor       uint64

	opts *config.ReaderOptions

	index *pb.RowIndex
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

func (r *reader) InitIndex(startOffset uint64, length uint64, path string) error {
	var err error
	var f *os.File
	if f, err = os.Open(path); err != nil {
		return err
	}
	defer f.Close()

	if _, err = f.Seek(int64(startOffset), io.SeekStart); err != nil {
		return err
	}

	var buf = make([]byte, length)
	if _, err = io.ReadFull(f, buf); err != nil {
		return err
	}

	return proto.Unmarshal(buf, r.index)
}

func (r *reader) nextPresents(presents *[]bool) error {
	// rethink: writer always init present stream writer first,
	// while reader's present stream init at prepare()
	if r.present == nil {
		return nil
	}

	pp := *presents

	count := r.cursor
	for i := 0; count < r.numberOfRows && i < cap(pp); i++ {
		v, err := r.present.Next()
		if err != nil {
			return err
		}
		pp = append(pp, v)
		count++
	}
	log.Debugf("column %d has read %d presents values", r.schema.Id, len(pp))

	*presents = pp
	return nil
}

func (r reader) InitChildren([]Reader) error {
	return errors.New("cannot init children")
}

func (r reader) Children() []Reader {
	return nil
}

func NewReader(schema *api.TypeDescription, opts *config.ReaderOptions, in orcio.File, numberOfRows uint64) (Reader, error) {
	switch schema.Kind {
	case pb.Type_SHORT:
		fallthrough
	case pb.Type_INT:
		fallthrough
	case pb.Type_LONG:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			return NewLongV2Reader(schema, opts, in, numberOfRows), nil
		}
		return nil, errors.New("not impl")

	case pb.Type_FLOAT:
	// todo:

	case pb.Type_DOUBLE:
		if schema.Encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("column encoding error")
		}
		return NewDoubleReader(schema, opts, in, numberOfRows), nil

	case pb.Type_STRING:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			//s.columnReaders[schema.Id] = &stringDirectV2Reader{treeReader: c}
			break
		}
		if schema.Encoding == pb.ColumnEncoding_DICTIONARY_V2 {
			//s.columnReaders[schema.Id] = &stringDictV2Reader{treeReader: c}
			break
		}
		return nil, errors.New("column encoding error")

	case pb.Type_BOOLEAN:
		if schema.Encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("bool column encoding error")
		}
		//s.columnReaders[schema.Id] = &boolReader{treeReader: c}

	case pb.Type_BYTE: // tinyint
		if schema.Encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("tinyint column encoding error")
		}
		return NewByteReader(schema, opts, in, numberOfRows), nil

	case pb.Type_BINARY:
		if schema.Encoding == pb.ColumnEncoding_DIRECT {
			return nil, errors.New("not impl")
			break
		}
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
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
			return NewDateV2Reader(schema, opts, in, numberOfRows), nil
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
			break
		}
		return nil, errors.New("column encoding error")

	case pb.Type_STRUCT:
		if schema.Encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("encoding error")
		}
		return NewStructReader(schema, opts, in, numberOfRows), nil

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
	// fixme: pb.Stream_DIRECT

	default:
		return nil, errors.New("type unwkone")
	}

	return nil, nil
}
