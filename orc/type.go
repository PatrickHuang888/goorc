package orc

import (
	"fmt"

	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
)

type TypeDescription struct {
	Id            uint32
	Kind          pb.Type_Kind
	ChildrenNames []string
	Children      []*TypeDescription
	Encoding      pb.ColumnEncoding_Kind

	HasNulls bool // for writing
	HasFather bool
}

func (td *TypeDescription) Print() {
	fmt.Printf("Id %d, Kind %stream\n", td.Id, td.Kind.String())
	if td.ChildrenNames != nil && len(td.ChildrenNames) != 0 {
		fmt.Println("ChildrenNames: ")
	}
	for i, cn := range td.ChildrenNames {

		if i == len(td.ChildrenNames)-1 {
			fmt.Printf("%stream \n", cn)
		} else {
			fmt.Printf("%stream, ", cn)
		}
	}
	for _, n := range td.Children {
		fmt.Printf("Children of %d:", td.Id)
		n.Print()
	}
}

// set ids of schema, flat the schema tree to slice
func (td *TypeDescription) normalize() (schemas []*TypeDescription) {
	var id uint32
	if err := walkSchema(&schemas, td, id); err != nil {
		fmt.Printf("%v+", err)
		return nil
	}
	return
}

// pre-order traverse
func walkSchema(schemas *[]*TypeDescription, node *TypeDescription, id uint32) error {
	node.Id = id
	*schemas = append(*schemas, node)

	if node.Kind == pb.Type_STRUCT || node.Kind == pb.Type_LIST {
		for _, td := range node.Children {
			id++
			if err := walkSchema(schemas, td, id); err != nil {
				return errors.WithStack(err)
			}
		}
	} else if node.Kind == pb.Type_UNION || node.Kind == pb.Type_MAP {
		return errors.New("type union or map no impl")
	}
	return nil
}

func schemasToTypes(schemas []*TypeDescription) []*pb.Type {
	t := make([]*pb.Type, len(schemas))
	for i, v := range schemas {
		t[i] = &pb.Type{Subtypes: make([]uint32, len(v.Children)),
			FieldNames: make([]string, len(v.ChildrenNames))}
		t[i].Kind = &v.Kind
		copy(t[i].FieldNames, v.ChildrenNames)
		for j, vc := range v.Children {
			t[i].Subtypes[j] = vc.Id
		}
	}
	return t
}

func (td *TypeDescription) CreateReaderBatch(opts *ReaderOptions) (batch *ColumnVector) {
	var vector interface{}
	switch td.Kind {
	case pb.Type_BOOLEAN:
		vector = make([]bool, 0, opts.RowSize)

	case pb.Type_BYTE:
		vector = make([]byte, 0, opts.RowSize)

	case pb.Type_SHORT:
		fallthrough
	case pb.Type_INT:
		fallthrough
	case pb.Type_LONG:
		vector = make([]int64, 0, opts.RowSize)

	case pb.Type_FLOAT:
		// todo:

	case pb.Type_DOUBLE:
		vector = make([]float64, 0, opts.RowSize)

	case pb.Type_DECIMAL:
		vector = make([]Decimal64, 0, opts.RowSize)

	case pb.Type_DATE:
		vector = make([]Date, 0, opts.RowSize)

	case pb.Type_TIMESTAMP:
		vector = make([]Timestamp, 0, opts.RowSize)

	case pb.Type_BINARY:
		vector = make([][]byte, 0, opts.RowSize)

	case pb.Type_STRING:
		fallthrough
	case pb.Type_CHAR:
		fallthrough
	case pb.Type_VARCHAR:
		vector = make([]string, 0, opts.RowSize)

	case pb.Type_STRUCT:
		var children []*ColumnVector
		for _, v := range td.Children {
			children = append(children, v.CreateReaderBatch(opts))
		}
		vector = children

	case pb.Type_UNION:
		//todo:
		fallthrough
	case pb.Type_MAP:
		// todo:
		fallthrough
	case pb.Type_LIST:
		// todo:
		panic("not impl")

	default:
		panic("unknown type")
	}

	batch= &ColumnVector{Id: td.Id, Vector: vector}
	if opts.HasNulls {
		batch.Presents= make([]bool, 0, opts.RowSize)
	}
	return
}

// vector data provided by writer
func (td TypeDescription) CreateWriterBatch(opts *WriterOptions) (batch *ColumnVector) {
	if td.Kind == pb.Type_STRUCT {
		var vector []*ColumnVector
		for _, v := range td.Children {
			vector = append(vector, v.CreateWriterBatch(opts))
		}
		return &ColumnVector{Id: td.Id, Vector: vector}
	}
	return &ColumnVector{Id: td.Id}
}
