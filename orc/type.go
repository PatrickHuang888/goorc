package orc

import (
	"github.com/PatrickHuang888/goorc/hive"
	"github.com/pkg/errors"
)

const (
	ORIGINAL RowBatchVersion = iota
	DECIMAL64

	BOOLEAN Category = iota
	BYTE
	SHORT
	INT
	LONG
	FLOAT
	DOUBLE
	STRING
	DATE
	TIMESTAMP
	BINARY
	DECIMAL
	VARCHAR
	CHAR  // 13
	LIST
	MAP
	STRUCT
	UNION
)

// type Category
type Category int

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
	category Category
	children []TypeDescription
	names    []string
}

func NewTypeDescription(cat Category) TypeDescription {
	return &typeDesc{category: cat}
}

func (td *typeDesc) AddField(name string, typeDesc TypeDescription) {
	td.children = append(td.children, typeDesc)
	td.names = append(td.names, name)
}

func (td *typeDesc) CreateRowBatch(ver RowBatchVersion, size int) (vrb *hive.VectorizedRowBatch, err error) {
	if td.category == STRUCT {
		numCols := len(td.children)
		cols := make([]hive.ColumnVector, numCols)
		vrb = &hive.VectorizedRowBatch{NumCols: numCols, Size: size, Cols: cols}
		for i, v := range td.children {
			cols[i], err = v.CreateColumn(ver, size)
			if err != nil {
				return nil, errors.WithStack(err)
			}
		}
	} else {
		cols := make([]hive.ColumnVector, 1)
		cols[0], err = td.CreateColumn(ver, size)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		vrb = &hive.VectorizedRowBatch{NumCols: 1, Size: size, Cols: cols}
	}
	return
}

func (td *typeDesc) CreateColumn(ver RowBatchVersion, maxSize int) (cv hive.ColumnVector, err error) {
	switch td.category {
	case BOOLEAN:
		fallthrough
	case BYTE:
		fallthrough
	case SHORT:
		fallthrough
	case INT:
		fallthrough
	case LONG:
		fallthrough
	case DATE:
		cv = hive.NewLongColumnVector(maxSize)
	case TIMESTAMP:
		cv = hive.NewTimestampColumnVector(maxSize)
	case FLOAT:
		fallthrough
	case DOUBLE:
		cv = hive.NewDoubleColumnVector(maxSize)
	case DECIMAL:
	// todo:
	case STRING:
		fallthrough
	case BINARY:
		fallthrough
	case CHAR:
		fallthrough
	case VARCHAR:
		cv = hive.NewBytesColumnVector(maxSize)
	case STRUCT:
		f := make([]hive.ColumnVector, len(td.children))
		for i, v := range td.children {
			f[i], err = v.CreateColumn(ver, maxSize)
			if err != nil {
				return nil, errors.WithStack(err)
			}
		}
		cv = hive.NewStructColumnVector(maxSize, f...)
	case UNION:
	// todo:
	case LIST:
	// todo:
	case MAP:
		// todo:
	default:
		return nil, errors.Errorf("unknown type %s", td.category.Name())
	}
	return cv, err
}
