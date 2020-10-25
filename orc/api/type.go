package api

import (
	"fmt"
	"github.com/patrickhuang888/goorc/orc/config"
	"strings"

	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
)

// todo: check reader, if parent has present stream, children should not have present stream
type TypeDescription struct {
	Id            uint32
	Kind          pb.Type_Kind
	ChildrenNames []string
	Children      []*TypeDescription

	// although encoding is stripe related, but I think this should be set in schema
	// maybe changed at nextStripe?
	Encoding pb.ColumnEncoding_Kind

	HasNulls  bool  // create initial batch presents vector on this when reading
}

func (td TypeDescription) String() string {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("id %d, kind %s: ", td.Id, td.Kind.String()))

	if len(td.Children) != 0 {
		sb.WriteString(fmt.Sprintf("\n"))
	}

	for i, name := range td.ChildrenNames {
			sb.WriteString(fmt.Sprintf("child %s: ", name))
			sb.WriteString(td.Children[i].String())
	}

	sb.WriteString(fmt.Sprintf("\n"))

	return sb.String()
}

var nodeId uint32
func doId(node *TypeDescription) error {
	nodeId++
	node.Id = nodeId
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


// set ids, flat the schema tree to slice
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

func (td *TypeDescription) CreateReaderBatch(opts *config.ReaderOptions) *ColumnVector {
	batch :=&ColumnVector{Id: td.Id, Kind: td.Kind, Vector: make([]Value, 0, opts.RowSize)}

	if td.Children!=nil {
		for _, v := range td.Children {
			batch.Children = append(batch.Children, v.CreateReaderBatch(opts))
		}
	}

	return batch
}

// should normalize first
func (td TypeDescription) CreateWriterBatch() *ColumnVector {
	// set id
	td.normalize()

	if td.Kind == pb.Type_STRUCT {
		var vector []*ColumnVector
		for _, v := range td.Children {
			vector = append(vector, v.CreateWriterBatch())
		}
		return &ColumnVector{Id: td.Id, Kind: td.Kind, Vector: vector}
	}
	return &ColumnVector{Id: td.Id, Kind: td.Kind}
}
