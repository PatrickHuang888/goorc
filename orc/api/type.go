package api

import (
	"fmt"
	"github.com/patrickhuang888/goorc/orc/config"
	"strings"

	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
)

type TypeDescription struct {
	Id            uint32
	Kind          pb.Type_Kind
	ChildrenNames []string
	Children      []*TypeDescription

	Encoding pb.ColumnEncoding_Kind

	HasNulls bool
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

type action func(node *TypeDescription) error

func checkId(node *TypeDescription) error {
	if node.Id == 0 {
		return errors.New("id ==0, not inited?")
	}
	return nil
}

func CheckId(root *TypeDescription) error {
	if len(root.Children) == 0 {
		for _, c := range root.Children {
			if err := traverse(c, checkId); err != nil {
				return err
			}
		}
	}
	return nil
}

func addId(node *TypeDescription) error {
	node.Id = _nodeId
	_nodeId++
	return nil
}

var _nodeId uint32

func SetId(root *TypeDescription) error {
	_nodeId = uint32(0)
	return traverse(root, addId)
}

func CheckNulls(root *TypeDescription) error {
	if root.HasNulls {
		checkNulls(root)
	}
	return nil
}

func checkNulls(node *TypeDescription) error {
	if node.HasNulls {
		for _, c := range node.Children {
			traverse(c, checkNoNull)
		}
	}
	return nil
}

func checkNoNull(node *TypeDescription) error {
	if node.HasNulls {
		return errors.Errorf("node %d has null", node.Id)
	}
	return nil
}

var _schemas []*TypeDescription

func (root *TypeDescription) Flat() (schemas []*TypeDescription, err error) {
	if err = CheckId(root); err != nil {
		return
	}
	_schemas = nil
	if err = traverse(root, addSchema); err != nil {
		return
	}
	schemas= _schemas
	return
}

func addSchema(node *TypeDescription) error {
	_schemas = append(_schemas, node)
	return nil
}

// pre-order traverse
func traverse(node *TypeDescription, do action) error {
	if err := do(node); err != nil {
		return err
	}

	if node.Kind != pb.Type_STRUCT && len(node.Children) != 0 {
		return errors.New("kind other than STRUCT must not have children")
	}

	for _, c := range node.Children {
		if err := traverse(c, do); err != nil {
			return err
		}
	}

	if node.Kind == pb.Type_UNION || node.Kind == pb.Type_MAP || node.Kind == pb.Type_LIST {
		return errors.New("no impl")
	}
	return nil
}

func NormalizeSchema(root *TypeDescription) error {
	if err := CheckNulls(root); err != nil {
		return err
	}
	return SetId(root)
}

func setId(node *TypeDescription, id uint32) error {
	node.Id = id

	if node.Kind == pb.Type_STRUCT || node.Kind == pb.Type_LIST {
		for _, td := range node.Children {
			id++
			if err := setId(td, id); err != nil {
				return errors.WithStack(err)
			}
		}
	} else if node.Kind == pb.Type_UNION || node.Kind == pb.Type_MAP {
		return errors.New("type union or map no impl")
	}
	return nil
}

func SchemasToTypes(schemas []*TypeDescription) []*pb.Type {
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

func CreateReaderBatch(td TypeDescription, opts config.ReaderOptions) ColumnVector {
	batch := ColumnVector{Id: td.Id, Kind: td.Kind, Vector: make([]Value, 0, opts.RowSize)}
	for _, v := range td.Children {
		batch.Children = append(batch.Children, CreateReaderBatch(*v, opts))
	}
	return batch
}

// should normalize first
func CreateWriterBatch(td TypeDescription, opts config.WriterOptions, createVector bool) (batch ColumnVector, err error) {
	if err = CheckId(&td); err != nil {
		return
	}

	if err = config.CheckWriteOpts(&opts); err != nil {
		return
	}

	if td.Kind == pb.Type_STRUCT {
		var children []ColumnVector
		for _, c := range td.Children {
			var cb ColumnVector
			if cb, err = CreateWriterBatch(*c, opts, createVector); err != nil {
				return
			}
			children = append(children, cb)
		}
		if createVector {
			return ColumnVector{Id: td.Id, Kind: td.Kind, Vector: make([]Value, 0, opts.RowSize), Children: children}, nil
		} else {
			return ColumnVector{Id: td.Id, Kind: td.Kind, Children: children}, nil
		}
	}

	if createVector {
		return ColumnVector{Id: td.Id, Kind: td.Kind, Vector: make([]Value, 0, opts.RowSize)}, nil
	} else {
		return ColumnVector{Id: td.Id, Kind: td.Kind}, nil
	}
}
