package main

import (
	"fmt"
	"github.com/patrickhuang888/goorc/orc"
	"github.com/patrickhuang888/goorc/pb/pb"
	"os"
	"strconv"
)

func main() {
	schema := &orc.TypeDescription{Kind:pb.Type_STRUCT}
	x := &orc.TypeDescription{Kind:pb.Type_INT}
	y := &orc.TypeDescription{Kind: pb.Type_STRING}
	schema.ChildrenNames= []string{"x", "y"}
	schema.Children=[]*orc.TypeDescription{x, y}

	opts := orc.DefaultWriterOptions()
	writer, err := orc.NewWriter("my-file-w.orc", schema, opts)
	if err != nil {
		fmt.Printf("create writer error %+v", err)
		os.Exit(1)
	}

	batch, err := schema.CreateWriterBatch(opts)
	if err != nil {
		fmt.Printf("got error when create row batch %v+", err)
		os.Exit(1)
	}

	vint:=make([]int64, 1500)
	vstr:= make([]string, 1500)
	for i:=0; i<1500; i++ {
		vint[i]= int64(i)
		vstr[i] = fmt.Sprintf("string-%s", strconv.Itoa(i))
	}
	batch.(*orc.StructColumn).Fields[0].(*orc.LongColumn).Vector= vint
	batch.(*orc.StructColumn).Fields[1].(*orc.StringColumn).Vector= vstr

	if err := writer.Write(batch); err != nil {
		fmt.Printf("write error %+v", err)
		os.Exit(1)
	}

	if err := writer.Close(); err != nil {
		fmt.Printf("close error %+v", err)
	}
}
