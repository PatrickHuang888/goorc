package orc

import (
	"fmt"
	"strings"

	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
)

type TypeDescription struct {
	Id            uint32
	Kind          pb.Type_Kind
	ChildrenNames []string
	Children      []*TypeDescription

	// although encoding is stripe related, but I think this should be set in schema
	// maybe changed at nextStripe?
	Encoding pb.ColumnEncoding_Kind

	HasNulls  bool
	HasFather bool
}

func (td TypeDescription) String() string{
	sb:= strings.Builder{}
	sb.WriteString(fmt.Sprintf("Id %d, Kind %s ", td.Id, td.Kind.String()))
	if td.ChildrenNames != nil && len(td.ChildrenNames) != 0 {
		sb.WriteString(fmt.Sprint("ChildrenNames: "))
	}

	for i, cn := range td.ChildrenNames {
		if i == len(td.ChildrenNames)-1 {
			sb.WriteString(fmt.Sprintf("%s ", cn))
		} else {
			sb.WriteString(fmt.Sprintf("%s, ", cn))
		}
	}

	for _, n := range td.Children {
		sb.WriteString(fmt.Sprintf("Children of %d:", td.Id))
		sb.WriteString(n.String())
	}

	return sb.String()
}

func doId(node *TypeDescription) error {
	nodeId++
	node.Id = nodeId
	return nil
}

func doFatherFlag(node *TypeDescription) error {
	node.HasFather = true
	return nil
}

func leafNoNulls(node *TypeDescription) error {
	if node.Children==nil && node.HasNulls{
		return errors.Errorf("leaft node %d cannot has nulls", node.Id)
	}
	return nil
}

type action func(*TypeDescription) error

// pre-order traverse
func traverse(node *TypeDescription, do action) error {
	if node.Kind == pb.Type_STRUCT || node.Kind == pb.Type_LIST {
		for _, td := range node.Children {
			if err := do(td); err != nil {
				return err
			}
			if err := traverse(td, do); err != nil {
				return err
			}
		}
	} else if node.Kind == pb.Type_UNION || node.Kind == pb.Type_MAP {
		return errors.New("type union or map no impl")
	}
	return nil
}

var nodeId uint32

func normalizeSchema(root *TypeDescription) error {
	// set id for write
	if len(root.Children)!=0 && root.Children[0].Id==0 {
		nodeId = 0
		if err := traverse(root, doId); err != nil {
			return err
		}
	}

	if err := traverse(root, doFatherFlag); err != nil {
		return err
	}

	if err:=traverse(root, leafNoNulls); err!=nil {
		return err
	}

	return nil
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
	// refactor: how and when normalized
	//td.normalize()

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
		vector = make([]float32, 0, opts.RowSize)

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

	batch = &ColumnVector{Id: td.Id, Vector: vector}
	if td.HasNulls {
		batch.Presents = make([]bool, 0, opts.RowSize)
	}
	return
}

// should normalize first
func (td TypeDescription) CreateWriterBatch(opts *WriterOptions) (batch *ColumnVector) {
	// refactor: how and when normalized
	td.normalize()

	if td.Kind == pb.Type_STRUCT {
		var vector []*ColumnVector
		for _, v := range td.Children {
			vector = append(vector, v.CreateWriterBatch(opts))
		}
		return &ColumnVector{Id: td.Id, Kind: td.Kind, Vector: vector}
	}
	return &ColumnVector{Id: td.Id, Kind: td.Kind}
}
