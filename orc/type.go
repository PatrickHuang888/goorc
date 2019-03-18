package orc

import (
	"fmt"
	"github.com/PatrickHuang888/goorc/hive"
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

func (td *TypeDescription) CreateRowBatch(maxSize int) (vrb *hive.VectorizedRowBatch, err error) {
	if maxSize < hive.DEFAULT_ROW_SIZE {
		maxSize = hive.DEFAULT_ROW_SIZE
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

func (td *TypeDescription) CreateColumn(maxSize int) (cv hive.ColumnVector, err error) {
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
		cv = &hive.LongColumnVector{Vector: make([]int64, hive.DEFAULT_ROW_SIZE, maxSize)}
	case Type_TIMESTAMP:
		cv = &hive.TimestampColumnVector{Vector: make([]uint64, hive.DEFAULT_ROW_SIZE, maxSize)}
	case Type_FLOAT:
		fallthrough
	case Type_DOUBLE:
		cv = &hive.DoubleColumnVector{Vector: make([]float64, hive.DEFAULT_ROW_SIZE, maxSize)}
	case Type_DECIMAL:
	// todo:
	case Type_STRING:
		fallthrough
	case Type_BINARY:
		fallthrough
	case Type_CHAR:
		fallthrough
	case Type_VARCHAR:
		cv = &hive.BytesColumnVector{Vector: make([][]byte, hive.DEFAULT_ROW_SIZE, maxSize)}
	case Type_STRUCT:
		f := make([]hive.ColumnVector, len(td.Children))
		for i, v := range td.Children {
			f[i], err = v.CreateColumn(maxSize)
			if err != nil {
				return nil, errors.WithStack(err)
			}
		}
		cv = hive.NewStructColumnVector(maxSize, f...)
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
