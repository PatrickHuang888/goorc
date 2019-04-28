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
	writer := orc.NewWriter("my-file.orc", x)

	batch, err := x.CreateVectorBatch(orc.DEFAULT_ROW_SIZE)
	if err != nil {
		fmt.Printf("got error when create row batch %v+", err)
		os.Exit(1)
	}

	for r := 0; r < 10000; r++ {

		batch.(*orc.LongColumnVector).Vector[row] = int64(r)
		//vy.Vector[row] = "Last-" + string(r*3)

		if batch.Len() == cap() {
			writer.AddRowBatch(batch)
			batch.reset()
		}
	}

	if batch.size != 0 {
		writer.AddRowBatch(batch)
	}
	writer.Close()
}
