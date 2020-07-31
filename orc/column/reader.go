package column

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/patrickhuang888/goorc/orc"
	"github.com/patrickhuang888/goorc/orc/stream"
	"github.com/patrickhuang888/goorc/pb/pb"
)

type Reader interface {
	InitChildren(children []Reader)
	InitIndex(startOffset uint64, length uint64, path string) error
	InitStream(kind pb.Stream_Kind, encoding pb.ColumnEncoding_Kind, startOffset uint64, info *pb.Stream, path string) error

	Next(presents *[]bool, pFromParent bool, vec *interface{}) (int, error)
	Seek(rowNumber uint64) error

	Children() []Reader

	Close() error
}

type reader struct {
	path string

	schema *orc.TypeDescription

	present *stream.BoolReader

	numberOfRows uint64 // from stripe information
	cursor       uint64

	compressionKind pb.CompressionKind

	opts *orc.ReaderOptions

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
	if f, err = os.Open(path);err!=nil {
		return err
	}
	defer f.Close()

	if _, err= f.Seek(int64(startOffset), io.SeekStart);err!=nil {
		return err
	}

	var buf= make([]byte, length)
	if _, err=io.ReadFull(f, buf);err!=nil {
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

	*presents= pp
	return nil
}

func (r reader) InitChildren([]Reader)  {
	//
}

func (r reader) Children() []Reader {
	return nil
}

func NewReader(schema *orc.TypeDescription, opts *orc.ReaderOptions, path string, numberOfRows uint64) (Reader, error) {
	switch schema.Kind {
	case pb.Type_SHORT:
		fallthrough
	case pb.Type_INT:
		fallthrough
	case pb.Type_LONG:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			//s.columnReaders[schema.Id] = &longV2Reader{treeReader: c}
			break
		}
		return nil, errors.New("not impl")

	case pb.Type_FLOAT:
	// todo:

	case pb.Type_DOUBLE:
		if schema.Encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("column encoding error")
		}
		//s.columnReaders[schema.Id] = &doubleReader{treeReader: c}

	case pb.Type_STRING:
		if schema.Encoding == pb.ColumnEncoding_DIRECT_V2 {
			//s.columnReaders[schema.Id] = &stringDirectV2Reader{treeReader: c}
			break
		}
		if encoding == pb.ColumnEncoding_DICTIONARY_V2 {
			//s.columnReaders[schema.Id] = &stringDictV2Reader{treeReader: c}
			break
		}
		return nil, errors.New("column encoding error")

	case pb.Type_BOOLEAN:
		if encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("bool column encoding error")
		}
		//s.columnReaders[schema.Id] = &boolReader{treeReader: c}

	case pb.Type_BYTE: // tinyint
		if encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("tinyint column encoding error")
		}
		return NewByteReader(schema, opts, path, numberOfRows), nil

	case pb.Type_BINARY:
		if encoding == pb.ColumnEncoding_DIRECT {
			return nil, errors.New("not impl")
			break
		}
		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			//s.columnReaders[schema.Id] = &binaryV2Reader{treeReader: c}
			break
		}
		return nil, errors.New("binary column encoding error")

	case pb.Type_DECIMAL:
		if encoding == pb.ColumnEncoding_DIRECT {
			// todo:
			return nil, errors.New("not impl")
			break
		}
		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			//s.columnReaders[schema.Id] = &decimal64DirectV2Reader{treeReader: c}
			break
		}
		return nil, errors.New("column encoding error")

	case pb.Type_DATE:
		if encoding == pb.ColumnEncoding_DIRECT {
			// todo:
			return nil, errors.New("not impl")
			break
		}
		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			//s.columnReaders[schema.Id] = &dateV2Reader{treeReader: c}
			break
		}
		return nil, errors.New("column encoding error")

	case pb.Type_TIMESTAMP:
		if encoding == pb.ColumnEncoding_DIRECT {
			// todo:
			return nil, errors.New("not impl")
			break
		}

		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			//s.columnReaders[schema.Id] = &timestampV2Reader{treeReader: c}
			break
		}
		return nil, errors.New("column encoding error")

	case pb.Type_STRUCT:
		if encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("encoding error")
		}
		return  NewStructReader(schema, opts, path, numberOfRows)

	case pb.Type_LIST:
		if encoding == pb.ColumnEncoding_DIRECT {
			// todo:
			return nil, errors.New("not impl")
			break
		}
		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			// todo:
			break
		}
		return nil, errors.New("encoding error")

	case pb.Type_MAP:
		if encoding == pb.ColumnEncoding_DIRECT {
			// todo:
			return nil, errors.New("not impl")
			break
		}
		if encoding == pb.ColumnEncoding_DIRECT_V2 {
			// todo:
			break
		}
		return nil, errors.New("encoding error")

	case pb.Type_UNION:
		if encoding != pb.ColumnEncoding_DIRECT {
			return nil, errors.New("column encoding error")
		}
	//todo:
	// fixme: pb.Stream_DIRECT

	default:
		return nil, errors.New("type unwkone")
	}

	return nil, nil
}

