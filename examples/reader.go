package main

import (
	"fmt"
	"os"

	"github.com/PatrickHuang888/goorc/orc"
)

func main() {
	reader, err := orc.CreateReader("/u01/apache/orc/java/examples/my-file.orc")
	if err != nil {
		fmt.Printf("create reader error: %+v", err)
		os.Exit(1)
	}
	fmt.Printf("row count: %d\n", reader.NumberOfRows())

	/*schema := orc.NewTypeDescription(pb.Type_STRUCT)
	xtd := orc.NewTypeDescription(pb.Type_INT)
	ytd := orc.NewTypeDescription(pb.Type_STRING)
	schema.AddField("x", xtd)
	schema.AddField("y", ytd)*/

	schema, err := reader.GetColumnSchema(1)
	if err != nil {
		fmt.Printf("get schema error %+v", err)
		os.Exit(1)
	}
	batch, err := schema.CreateVectorBatch(orc.DEFAULT_ROW_SIZE)
	if err != nil {
		fmt.Printf("create row batch error %+v", err)
		os.Exit(1)
	}
	it, err := reader.Stripes()
	if err != nil {
		fmt.Printf("%+v", err)
	}

	/*x := batch.Cols[0].(*hive.LongColumnVector)
	y := batch.Cols[1].(*hive.BytesColumnVector)*/

	for it.NextStripe() {
		for {
			next := it.NextBatch(batch)
			data := batch.(*orc.LongColumnVector).Vector
			for i := 0; i < len(data); i++ {
				x := data[i]
				fmt.Println(x)
			}
			if !next {
				break
			}
		}
		if err = it.Err(); err != nil {
			fmt.Printf("%+v", err)
		}
	}

	it.Close()

	/*for it.NextBatch(batch) {
		for row := 0; row < batch.Size; row++ {
			xRow := 0
			if !x.Repeating {
				xRow = row
			}
			fmt.Printf("x: %d\n", x.Vector[xRow])
			fmt.Printf("y: %s\n", string(y.Vector[row]))
		}
	}
	it.Close()*/
}
