package orc

import (
	"fmt"
	. "github.com/PatrickHuang888/goorc/pb/pb"
	"github.com/pkg/errors"
)

type TypeDescription struct {
	Id            uint32
	Kind          Type_Kind
	ChildrenNames []string
	Children      []*TypeDescription
}

func (td *TypeDescription) Print() {
	fmt.Printf("Id: %d, Kind: %s\n", td.Id, td.Kind.String())
	fmt.Printf("ChildrenNames:%s\n", td.ChildrenNames)
	for _, n := range td.Children {
		fmt.Printf("Children of %d:", td.Id)
		n.Print()
	}
}

// normalize type description
func CreateSchema(td *TypeDescription) (*TypeDescription, error) {
	schema, err := walkSchema(td)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	for i := 0; i < len(schema); i++ {
		schema[i].Id = uint32(i)
	}
	return schema[0], nil
}

func (td *TypeDescription) normalize() []*TypeDescription {
	ss, err := walkSchema(td)
	if err != nil {
		fmt.Printf("%v+", err)
		return nil
	}
	return ss
}

// pre-order traverse, turn type description tree into slice
func walkSchema(node *TypeDescription) (schema []*TypeDescription, err error) {
	if node.Kind == Type_STRUCT || node.Kind == Type_LIST {
		for _, td := range node.Children {
			ts, err := walkSchema(td)
			if err != nil {
				return nil, err
			}
			schema = append(schema, ts...)
		}
	} else if node.Kind == Type_UNION || node.Kind == Type_MAP {
		err = errors.New("type union or map no impl")
	} else {
		schema = []*TypeDescription{node}
	}
	return
}

func schemasToTypes(schemas []*TypeDescription) []*Type {
	t := make([]*Type, len(schemas))
	for i, v := range schemas {
		t[i] = &Type{Subtypes: make([]uint32, len(v.Children)),
			FieldNames: make([]string, len(v.ChildrenNames))}
		t[i].Kind = &v.Kind
		copy(t[i].FieldNames, v.ChildrenNames)
		for j, vc := range v.Children {
			t[i].Subtypes[j] = vc.Id
		}
	}
	return t
}

/*func (td *TypeDescription) CreateRowBatch(maxSize int) (batch ColumnVector, err error) {
	if maxSize < DEFAULT_ROW_SIZE {
		maxSize = DEFAULT_ROW_SIZE
	}

	switch expr {

	}
	if td.Kind == Type_STRUCT {
		numCols := len(td.Children)
		cols := make([]hive.ColumnVector, numCols)
		vrb = &hive.VectorizedRowBatch{NumCols: numCols, Cols: cols}
		for i, v := range td.Children {
			cols[i], err = v.CreateColumn(maxSize)
			if err != nil {
				return nil, errors.WithStack(err)
			}
		}
	} else {
		cols := make([]hive.ColumnVector, 1)
		cols[0], err = td.CreateColumn(maxSize)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		vrb = &hive.VectorizedRowBatch{NumCols: 1, Cols: cols}
	}
	return
}
*/

func (td *TypeDescription) CreateVectorBatch() (cv ColumnVector, err error) {
	return td.newColumn(false)
}

func (td *TypeDescription) newColumn(nullable bool) (cv ColumnVector, err error) {
	switch td.Kind {
	case Type_BOOLEAN:
		cv = &BoolColumn{column: column{id: td.Id, nullable: nullable}}

	case Type_BYTE:
		cv = &TinyIntColumn{column: column{id: td.Id, nullable: nullable}}

	case Type_SHORT:
		fallthrough
	case Type_INT:
		fallthrough
	case Type_LONG:
		cv = &LongColumn{column: column{id: td.Id, nullable: nullable}}

	case Type_FLOAT:
		cv = &FloatColumn{column: column{id: td.Id, nullable: nullable}}

	case Type_DOUBLE:
		cv = &DoubleColumn{column: column{id: td.Id, nullable: nullable}}

	case Type_DECIMAL:
		// todo:
		//cv = &DecimalColumn{column: column{id: td.Id, nullable: false}}
		return nil, errors.New("not impl")

	case Type_DATE:
		cv = &DateColumn{column: column{id: td.Id, nullable: nullable}}

	case Type_TIMESTAMP:
		cv = &TimestampColumn{column: column{id: td.Id, nullable: nullable}}

	case Type_BINARY:
		cv = &BinaryColumn{column: column{id: td.Id, nullable: nullable}}

	case Type_STRING:
		fallthrough
	case Type_CHAR:
		fallthrough
	case Type_VARCHAR:
		cv = &StringColumn{column: column{id: td.Id, nullable: nullable}}

	case Type_STRUCT:
		f := make([]ColumnVector, len(td.Children))
		for i, v := range td.Children {
			f[i], err = v.newColumn(true)
			if err != nil {
				return nil, errors.WithStack(err)
			}
		}
		cv = &StructColumn{column: column{id: td.Id, nullable: nullable}, Fields: f}

	case Type_UNION:
		// todo:
		return nil, errors.New("not impl")
	case Type_LIST:
		assert(len(td.Children) == 1)
		c, err := td.Children[0].newColumn(true)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		cv = &ListColumn{column: column{id: td.Id, nullable: nullable}, Child: c}

	case Type_MAP:
		// todo:
		return nil, errors.New("not impl")

	default:
		return nil, errors.Errorf("unknown type %s", td.Kind.String())
	}
	return cv, err
}
