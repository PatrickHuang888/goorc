package orc

import (
	"fmt"
	"github.com/PatrickHuang888/goorc/pb/pb"
	"github.com/golang/protobuf/jsonpb"
	"testing"
)

func TestMarshal(t *testing.T) {

	names := []string{"int", "map"}
	children := []*TypeDescription{{Kind: pb.Type_INT, Id: 1}, {Kind: pb.Type_MAP, Id: 2}}
	root := &TypeDescription{Kind: pb.Type_STRUCT, Id: 0,
		ChildrenNames: names, Children: children}
	root.Print()

	msl := new(jsonpb.Marshaler)
	pbTypes := marshallSchema(root)
	fmt.Println(len(pbTypes))

	for _, t := range pbTypes {
		s, err := msl.MarshalToString(t)
		if err != nil {
			fmt.Printf("Error: %+v", err)
		}
		fmt.Println(s)
	}

	root = unmarshallSchema(pbTypes)
	root.Print()

}
