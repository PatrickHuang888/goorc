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

type TypeDescription struct {
	Category   Category
	Children   []*TypeDescription
	FieldNames []string
}

func (td *TypeDescription) NewDefaultRowBatch() (*hive.VectorizedRowBatch, error) {
	return td.CreateRowBatch(ORIGINAL, hive.DEFAULT_ROW_SIZE)
}

func (td *TypeDescription) CreateRowBatch(ver RowBatchVersion, size int) (vrb *hive.VectorizedRowBatch, err error) {
	if td.Category == STRUCT {
		numCols := len(td.Children)
		cols := make([]hive.ColumnVector, numCols)
		vrb = &hive.VectorizedRowBatch{NumCols: numCols, Size: size, Cols: cols}
		for i, v := range td.Children {
			cols[i], err = v.createColumn(ver, size)
			if err != nil {
				return nil, errors.WithStack(err)
			}
		}
	} else {
		cols := make([]hive.ColumnVector, 1)
		cols[0], err = td.createColumn(ver, size)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		vrb = &hive.VectorizedRowBatch{NumCols: 1, Size: size, Cols: cols}
	}
	return vrb, err
}

func (td *TypeDescription) createColumn(ver RowBatchVersion, maxSize int) (cv hive.ColumnVector, err error) {
	switch td.Category {
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
		cv = hive.NewDoubleColumnVector(maxSize)
	case STRUCT:
		f := make([]hive.ColumnVector, len(td.Children))
		for i, v := range td.Children {
			f[i], err = v.createColumn(ver, maxSize)
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
		return nil, errors.Errorf("unknown type %s", td.Category.Name())
	}
	return cv, err
}
