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

// type category
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
	category Category
	children []*TypeDescription
}

func (td *TypeDescription) NewDefaultRowBatch() (*hive.VectorizedRowBatch, error) {
	return td.CreateRowBatch(ORIGINAL, hive.VectorizedRowBatch_DEFAULT_SIZE)
}

func (td *TypeDescription) CreateRowBatch(ver RowBatchVersion, size int) (vrb *hive.VectorizedRowBatch, err error) {
	if td.category == STRUCT {
		numCols := len(td.children)
		cols := make([]hive.ColumnVector, numCols)
		vrb = &hive.VectorizedRowBatch{NumCols: numCols, Size: size, Cols: cols}
		for i, v := range td.children {

		}
	}
	return vrb, nil
}

func (td *TypeDescription) createColumn(ver RowBatchVersion, maxSize int) (cv *hive.ColumnVector, err error) {
	switch td.category {
	case BOOLEAN
	default:
		return nil, errors.Errorf("unknown type %s", td.category.Name())
	}
	return cv, err
}
