package orc

import (
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

type TypeDescription interface {
	CreateRowBatch(ver RowBatchVersion, size int) (*hive.VectorizedRowBatch, error)
	AddField(name string, td TypeDescription)
	CreateColumn(ver RowBatchVersion, maxSize int) (hive.ColumnVector, error)
}

type typeDesc struct {
	category Type_Kind
	children []TypeDescription
	names    []string
}

func NewTypeDescription(cat Type_Kind) TypeDescription {
	return &typeDesc{category: cat}
}

func (td *typeDesc) AddField(name string, typeDesc TypeDescription) {
	td.children = append(td.children, typeDesc)
	td.names = append(td.names, name)
}

func (td *typeDesc) CreateRowBatch(ver RowBatchVersion, maxSize int) (vrb *hive.VectorizedRowBatch, err error) {
	if maxSize < hive.DEFAULT_ROW_SIZE {
		maxSize = hive.DEFAULT_ROW_SIZE
	}
	if td.category == Type_STRUCT {
		numCols := len(td.children)
		cols := make([]hive.ColumnVector, numCols)
		vrb = &hive.VectorizedRowBatch{NumCols: numCols, Cols: cols}
		for i, v := range td.children {
			cols[i], err = v.CreateColumn(ver, maxSize)
			if err != nil {
				return nil, errors.WithStack(err)
			}
		}
	} else {
		cols := make([]hive.ColumnVector, 1)
		cols[0], err = td.CreateColumn(ver, maxSize)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		vrb = &hive.VectorizedRowBatch{NumCols: 1, Cols: cols}
	}
	return
}

func (td *typeDesc) CreateColumn(ver RowBatchVersion, maxSize int) (cv hive.ColumnVector, err error) {
	switch td.category {
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
		f := make([]hive.ColumnVector, len(td.children))
		for i, v := range td.children {
			f[i], err = v.CreateColumn(ver, maxSize)
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
		return nil, errors.Errorf("unknown type %s", td.category.String())
	}
	return cv, err
}
