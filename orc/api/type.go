package api

import (
	"fmt"
	"strings"
	"time"

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

func (td *TypeDescription) AddChild(name string, value *TypeDescription) {
	td.ChildrenNames = append(td.ChildrenNames, name)
	td.Children = append(td.Children, value)
}

func (td TypeDescription) CreateVector(opt *BatchOption) (*ColumnVector, error) {
	if opt.RowSize == 0 {
		return nil, errors.New("RowSize == 0")
	}
	if err := td.verifySchema(); err != nil {
		return nil, err
	}

	return td.createVector(opt), nil
}

func (td TypeDescription) createStructVector(opt *BatchOption) *ColumnVector {
	var cv *ColumnVector
	if td.HasNulls && !opt.NotCreateVector {
		cv = &ColumnVector{Id: td.Id, Kind: td.Kind, Vector: make([]Value, opt.RowSize)}
	} else {
		cv = &ColumnVector{Id: td.Id, Kind: td.Kind}
	}
	for _, c := range td.Children {
		childVec := c.createVector(opt)
		if childVec != nil {
			cv.Children = append(cv.Children, childVec)
		}
	}
	return cv
}

func (td TypeDescription) createListVector(opt *BatchOption) *ColumnVector {
	cv := &ColumnVector{Id: td.Id, Kind: td.Kind, Vector: make([]Value, opt.RowSize)}
	cv.Children = append(cv.Children, td.Children[0].createVector(opt))
	return cv
}

func (td TypeDescription) createMapVector(opt *BatchOption) *ColumnVector {
	cv := &ColumnVector{Id: td.Id, Kind: td.Kind, Vector: make([]Value, opt.RowSize)}
	cv.Children = append(cv.Children, td.Children[0].createVector(opt))
	cv.Children = append(cv.Children, td.Children[1].createVector(opt))
	return cv
}

func (td TypeDescription) createPrimaryVector(opt *BatchOption) *ColumnVector {
	return &ColumnVector{Id: td.Id, Kind: td.Kind, Vector: make([]Value, opt.RowSize)}
}

func (td TypeDescription) createVector(opt *BatchOption) *ColumnVector {
	create := len(opt.Includes) == 0

	if !create {
		for _, id := range opt.Includes {
			if td.Id == id {
				create = true
				break
			}
		}
	}

	if create {
		switch td.Kind {
		case pb.Type_STRUCT:
			return td.createStructVector(opt)
		case pb.Type_LIST:
			return td.createListVector(opt)
		case pb.Type_MAP:
			return td.createMapVector(opt)
		case pb.Type_UNION:
			panic("not impl")
		default:
			return td.createPrimaryVector(opt)
		}
	}
	return nil
}

func (td TypeDescription) verifySchema() error {
	if td.Kind == pb.Type_STRUCT || td.Kind == pb.Type_UNION || td.Kind == pb.Type_MAP || td.Kind == pb.Type_LIST {
		for _, c := range td.Children {
			if td.HasNulls && c.HasNulls {
				return errors.New("struct column has nulls cannot have nulls children")
			}
			return c.verifySchema()
		}
	}
	return nil
}

const DefaultRowSize = 10_000

type BatchOption struct {
	// includes column id, nil means all column
	Includes        []uint32
	RowSize         int
	Loc             *time.Location
	NotCreateVector bool
}

type action func(node *TypeDescription) error

func checkId(node *TypeDescription) error {
	if node.Id == 0 {
		return errors.New("id ==0, not inited?")
	}
	return nil
}

func CheckId(root *TypeDescription) error {
	for _, c := range root.Children {
		if err := traverse(c, checkId); err != nil {
			return err
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
		if err := checkNulls(root); err != nil {
			return err
		}
	}
	return nil
}

func checkNulls(node *TypeDescription) error {
	if node.HasNulls {
		for _, c := range node.Children {
			if err := traverse(c, checkNoNull); err != nil {
				return err
			}
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
	schemas = _schemas
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

/*func CreateVector(td *TypeDescription, rowSize int) (batch ColumnVector, err error) {
	if err = verifySchema(td); err != nil {
		return
	}

	batch = ColumnVector{Id: td.Id, Kind: td.Kind, Vector: make([]Value, 0, rowSize)}
	for _, v := range td.Children {
		var b ColumnVector
		if b, err = CreateVector(v, rowSize); err != nil {
			return
		}
		batch.Children = append(batch.Children, &b)
	}
	return
}*/

func verifySchema(schema *TypeDescription) error {
	if schema.Kind == pb.Type_STRUCT {
		for _, c := range schema.Children {
			if schema.HasNulls && c.HasNulls {
				return errors.New("struct column has nulls cannot have nulls children")
			}
		}
	}
	return nil
}

// should normalize first
/*func CreateWriterBatch(td *TypeDescription, opts config.WriterOptions) (batch ColumnVector, err error) {
	if err = CheckId(td); err != nil {
		return
	}

	if err = config.CheckWriteOpts(&opts); err != nil {
		return
	}

	// todo: other type
	if td.Kind == pb.Type_STRUCT {
		var children []*ColumnVector
		for _, c := range td.Children {
			var cb ColumnVector
			if cb, err = CreateWriterBatch(c, opts); err != nil {
				return
			}
			children = append(children, &cb)
		}
		if td.HasNulls && opts.CreateVector {
			return ColumnVector{Id: td.Id, Kind: td.Kind, Vector: make([]Value, opts.RowSize), Children: children}, nil
		}
		return ColumnVector{Id: td.Id, Kind: td.Kind, Children: children}, nil
	}

	if opts.CreateVector {
		return ColumnVector{Id: td.Id, Kind: td.Kind, Vector: make([]Value, opts.RowSize)}, nil
	} else {
		return ColumnVector{Id: td.Id, Kind: td.Kind}, nil
	}
}*/
