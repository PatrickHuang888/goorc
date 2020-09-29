package orc

import (
	"fmt"
	"github.com/golang/protobuf/jsonpb"
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/pb/pb"
	"testing"
)

func TestMarshal(t *testing.T) {

	names := []string{"int", "map"}
	children := []*api.TypeDescription{{Kind: pb.Type_INT, Id: 1}, {Kind: pb.Type_MAP, Id: 2}}
	root := &api.TypeDescription{Kind: pb.Type_STRUCT, Id: 0,
		ChildrenNames: names, Children: children}

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

	root = unmarshallSchema(pbTypes)[0]

}
