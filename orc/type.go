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

	HasNulls bool  // for writing
}

func (td *TypeDescription) Print() {
	fmt.Printf("Id %d, Kind %s\n", td.Id, td.Kind.String())
	if td.ChildrenNames != nil && len(td.ChildrenNames) != 0 {
		fmt.Println("ChildrenNames: ")
	}
	for i, cn := range td.ChildrenNames {

		if i == len(td.ChildrenNames)-1 {
			fmt.Printf("%s \n", cn)
		} else {
			fmt.Printf("%s, ", cn)
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
	var vector []*ColumnVector
	if td.Kind == pb.Type_STRUCT {
		for _, v := range td.Children {
			vector= append(vector, v.CreateReaderBatch(opts))
		}
	}

	return &ColumnVector{Id:td.Id, Size:opts.RowSize}
}

// vector provided by writer
func (td TypeDescription) CreateWriterBatch(opts *WriterOptions) (batch *ColumnVector) {
	var vector []*ColumnVector
	if td.Kind == pb.Type_STRUCT {
		for _, v := range td.Children {
			vector= append(vector, v.CreateWriterBatch(opts))
		}
	}
	return &ColumnVector{Id:td.Id, Size:opts.RowSize}
}

/*func (td *TypeDescription) newColumn(rowSize int, nullable bool, createVector bool) (batch *ColumnVector, err error) {
	batch = &ColumnVector{Id:td.Id, Size:rowSize}
	switch td.Kind {
	case Type_BOOLEAN:
		c := &Column{col:col{Id:td.Id}}
		if createVector {
			c.Vector = make([]bool, rowSize)
		}
		return c, nil

	case Type_BYTE:
		c := &TinyIntColumn{batch: batch{id: td.Id, nullable: nullable}}
		if createVector {
			c.Vector = make([]byte, rowSize)
		}
		return c, nil

	case Type_SHORT:
		fallthrough
	case Type_INT:
		fallthrough
	case Type_LONG:
		c := &LongColumn{batch: batch{id: td.Id, nullable: nullable}}
		if createVector {
			c.Vector = make([]int64, rowSize)
		}
		return c, nil

	case Type_FLOAT:
		// todo:

	case Type_DOUBLE:
		c := &DoubleColumn{batch: batch{id: td.Id, nullable: nullable}}
		if createVector {
			c.Vector = make([]float64, rowSize)
		}
		return c, nil

	case Type_DECIMAL:
		c := &Decimal64Column{batch: batch{id: td.Id, nullable: false}}
		if createVector {
			c.Vector = make([]Decimal64, rowSize)
		}
		return c, nil

	case Type_DATE:
		c := &DateColumn{batch: batch{id: td.Id, nullable: nullable}}
		if createVector {
			c.Vector = make([]Date, rowSize)
		}
		return c, nil

	case Type_TIMESTAMP:
		c := &TimestampColumn{batch: batch{id: td.Id, nullable: nullable}}
		if createVector {
			c.Vector = make([]Timestamp, rowSize)
		}
		return c, nil

	case Type_BINARY:
		c := &BinaryColumn{batch: batch{id: td.Id, nullable: nullable}}
		if createVector {
			c.Vector = make([][]byte, rowSize)
		}
		return c, nil

	case Type_STRING:
		fallthrough
	case Type_CHAR:
		fallthrough
	case Type_VARCHAR:
		c := &StringColumn{batch: batch{id: td.Id, nullable: nullable}}
		if createVector {
			c.Vector = make([]string, rowSize)
		}
		return c, nil

	case Type_STRUCT:
		f := make([]ColumnVector, len(td.Children))
		for i, v := range td.Children {
			var err error
			f[i], err = v.newColumn(rowSize, true, createVector)
			if err != nil {
				return nil, errors.WithStack(err)
			}
		}
		c := &StructColumn{batch: batch{id: td.Id, nullable: nullable}, Fields: f}
		return c, nil

	case Type_UNION:
		// todo:
		return nil, errors.New("not impl")

	case Type_LIST:
		assertx(len(td.Children) == 1)
		c, err := td.Children[0].newColumn(rowSize, true, createVector)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		lc := &ListColumn{batch: batch{id: td.Id, nullable: nullable}, Child: c}
		return lc, nil

	case Type_MAP:
		// todo:
		return nil, errors.New("not impl")

	default:
		return nil, errors.Errorf("unknown type %s", td.Kind.String())
	}
	return nil, nil
}
*/