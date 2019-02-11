package orc

import "github.com/PatrickHuang888/goorc/hive"

const (
	RowBatchVersion_ORIGINAL = iota
	RowBatchVersion_USE_DECIMAL64
)

type TypeDescription struct {
	category Category
	children []*TypeDescription
}

func (td *TypeDescription) NewDefaultRowBatch() (*hive.VectorizedRowBatch, error) {
	return td.CreateRowBatch(RowBatchVersion_ORIGINAL, hive.VectorizedRowBatch_DEFAULT_SIZE)
}

func (td *TypeDescription) CreateRowBatch(ver int, size int) (vrb *hive.VectorizedRowBatch, err error) {
	if td.category == Struct {
		vrb= &hive.VectorizedRowBatch{}
		for i, v := range td.children {

		}
	}
}

type Category interface {
	Name() string
	IsPrimitive() bool
}

type s struct{}

func (s) Name() string {
	return "struct"
}
func (s) IsPrimitive() bool {
	return false
}

var Struct = &s{}
