package orc

import (
	"fmt"
	. "github.com/PatrickHuang888/goorc/pb/pb"
	"github.com/pkg/errors"
)

type TypeDescription struct {
	Id            uint32
	Kind          Type_Kind
	ChildrenNames []string
	Children      []*TypeDescription
}

func (td *TypeDescription) Print() {
	fmt.Printf("Id: %d, Kind: %s\n", td.Id, td.Kind.String())
	fmt.Printf("ChildrenNames:%s\n", td.ChildrenNames)
	for _, n := range td.Children {
		fmt.Printf("Children of %d:", td.Id)
		n.Print()
	}
}

// normalize type description
func CreateSchema(td *TypeDescription) (*TypeDescription, error) {
	schema, err := walkSchema(td)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	for i := 0; i < len(schema); i++ {
		schema[i].Id = uint32(i)
	}
	return schema[0], nil
}

// pre-order traverse, turn type description tree into slice
func walkSchema(node *TypeDescription) (schema []*TypeDescription, err error) {
	if node.Kind == Type_STRUCT || node.Kind == Type_LIST {
		for _, td := range node.Children {
			ts, err := walkSchema(td)
			if err != nil {
				return nil, err
			}
			schema = append(schema, ts...)
		}
	} else if node.Kind == Type_UNION || node.Kind == Type_MAP {
		err = errors.New("type union or map no impl")
	} else {
		schema = []*TypeDescription{node}
	}
	return
}

/*func (td *TypeDescription) CreateRowBatch(maxSize int) (batch ColumnVector, err error) {
	if maxSize < DEFAULT_ROW_SIZE {
		maxSize = DEFAULT_ROW_SIZE
	}

	switch expr {

	}
	if td.Kind == Type_STRUCT {
		numCols := len(td.Children)
		cols := make([]hive.ColumnVector, numCols)
		vrb = &hive.VectorizedRowBatch{NumCols: numCols, Cols: cols}
		for i, v := range td.Children {
			cols[i], err = v.CreateColumn(maxSize)
			if err != nil {
				return nil, errors.WithStack(err)
			}
		}
	} else {
		cols := make([]hive.ColumnVector, 1)
		cols[0], err = td.CreateColumn(maxSize)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		vrb = &hive.VectorizedRowBatch{NumCols: 1, Cols: cols}
	}
	return
}
*/

func (td *TypeDescription) CreateVectorBatch(maxSize int) (cv ColumnVector, err error) {
	if maxSize < DEFAULT_ROW_SIZE {
		maxSize = DEFAULT_ROW_SIZE
	}

	switch td.Kind {
	case Type_BOOLEAN:
		fallthrough
	case Type_BYTE:
		fallthrough
	case Type_SHORT:
		fallthrough
	case Type_INT:
		fallthrough
	case Type_LONG:
		fallthrough
	case Type_DATE:
		cv = &LongColumnVector{columnVector: columnVector{id: td.Id}, vector: make([]int64, DEFAULT_ROW_SIZE, maxSize)}
	case Type_TIMESTAMP:
		cv = &TimestampColumnVector{columnVector: columnVector{id: td.Id},
			Vector: make([]uint64, DEFAULT_ROW_SIZE, maxSize)}
	case Type_FLOAT:
		fallthrough
	case Type_DOUBLE:
		cv = &DoubleColumnVector{columnVector: columnVector{id: td.Id},
			Vector: make([]float64, DEFAULT_ROW_SIZE, maxSize)}
	case Type_DECIMAL:
	// todo:
	case Type_STRING:
		fallthrough
	case Type_BINARY:
		fallthrough
	case Type_CHAR:
		fallthrough
	case Type_VARCHAR:
		cv = &BytesColumnVector{columnVector: columnVector{id: td.Id},
			vector: make([][]byte, DEFAULT_ROW_SIZE, maxSize)}
	case Type_STRUCT:
		f := make([]ColumnVector, len(td.Children))
		for i, v := range td.Children {
			f[i], err = v.CreateVectorBatch(maxSize)
			if err != nil {
				return nil, errors.WithStack(err)
			}
		}
		cv = &StructColumnVector{columnVector: columnVector{id: td.Id}, fields: f}
	case Type_UNION:
	// todo:
	case Type_LIST:
	// todo:
	case Type_MAP:
		// todo:
	default:
		return nil, errors.Errorf("unknown type %s", td.Kind.String())
	}
	return cv, err
}
