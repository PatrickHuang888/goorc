package main

import (
	"fmt"
	"github.com/PatrickHuang888/goorc/orc"
	"github.com/PatrickHuang888/goorc/pb/pb"
	"os"
)

func main() {
	//schema := &orc.TypeDescription{Kind:pb.Type_STRUCT}
	x := &orc.TypeDescription{Kind:pb.Type_INT}
	//y := &orc.TypeDescription{Kind:pb.Type_STRING}
	//schema.ChildrenNames= []string{"x", "y"}
	//schema.Children=[]*orc.TypeDescription{x, y}
	schema, err:= orc.CreateSchema(x)
	if err!=nil {
		fmt.Printf("create schema error %v+", err)
		os.Exit(1)
	}

	writer, err := orc.NewWriter("my-file.orc", schema)
	if err != nil {
		fmt.Printf("create writer error %v+", err)
		os.Exit(1)
	}

	batch, err := schema.CreateVectorBatch(orc.DEFAULT_ROW_SIZE)
	if err != nil {
		fmt.Printf("got error when create row batch %v+", err)
		os.Exit(1)
	}

	v:=make([]int64, 3000)
	for i := 0; i < 3000; i++ {
		v[i]= int64(i)
	}
	batch.(*orc.LongColumnVector).SetVector(v)
	// fixme: write directly or cached in buffer ?
	writer.AddRowBatch(batch)
	//writer.WriteBatch()
	batch.(*orc.LongColumnVector).SetVector(v[:1000])
	writer.AddRowBatch(batch)
	//writer.Flush()
	writer.Close()
}
