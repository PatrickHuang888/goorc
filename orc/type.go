package orc

import (
	"fmt"
	. "github.com/PatrickHuang888/goorc/pb/pb"
	"github.com/pkg/errors"
)

const (
	ORIGINAL RowBatchVersion = iota
	DECIMAL64
)

// type Category
type Category Type_Kind

type catData struct {
	name        string
	isPrimitive bool
}

var cat = []catData{
	{"boolean", true},
	{"tinyint", true},
	{"smallint", true},
	{"int", true},
	{"bigint", true},
	{"float", true},
	{"double", true},
	{"string", true},
	{"date", true},
	{"timestamp", true},
	{"binary", true},
	{"decimal", true},
	{"varchar", true},
	{"char", true},
	{"array", false},
	{"map", false},
	{"struct", false},
	{"uniontype", false},
}

func (c Category) Name() string {
	return cat[c].name
}

func (c Category) IsPrimitive() bool {
	return cat[c].isPrimitive
}

type RowBatchVersion int

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
		cv = &BytesColumnVector{columnVector: columnVector{id:td.Id},
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
