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

	schema, err := reader.GetColumnSchema(0)
	if err != nil {
		fmt.Printf("get schema error %+v", err)
		os.Exit(1)
	}
	it, err := reader.Stripes()
	if err != nil {
		fmt.Printf("%+v", err)
	}
	batch, err := schema.CreateVectorBatch(orc.DEFAULT_ROW_SIZE)
	if err != nil {
		fmt.Printf("create row batch error %+v", err)
		os.Exit(1)
	}
	for it.NextStripe() {
		// fixme: should be iterating on struct batch?
		for ; it.NextBatch(batch); {
			data := batch.(*orc.StructColumnVector).Fields
			xs:= data[0].(*orc.LongColumnVector)
			for i := 0; i < xs.Len(); i++ {
				x := xs.Vector[i]
				fmt.Println(x)
			}
			ys:= data[1].(*orc.BytesColumnVector)
			for i := 0; i < ys.Len(); i++ {
				y := ys.Vector[i]
				fmt.Println(string(y))
			}
		}
		if err = it.Err(); err != nil {
			fmt.Printf("%+v", err)
		}
	}

	/*schema1, err := reader.GetColumnSchema(1)
	schema2, err := reader.GetColumnSchema(2)
	if err != nil {
		fmt.Printf("get schema error %+v", err)
		os.Exit(1)
	}
	batch1, err := schema1.CreateVectorBatch(orc.DEFAULT_ROW_SIZE)
	if err != nil {
		fmt.Printf("create row batch error %+v", err)
		os.Exit(1)
	}
	batch2, err := schema2.CreateVectorBatch(orc.DEFAULT_ROW_SIZE)
	if err != nil {
		fmt.Printf("create row batch error %+v", err)
		os.Exit(1)
	}
	it, err := reader.Stripes()
	if err != nil {
		fmt.Printf("%+v", err)
	}

	for it.NextStripe() {
		for ; it.NextBatch(batch1); {
			data := batch1.(*orc.LongColumnVector).Vector
			for i := 0; i < batch1.Len(); i++ {
				x := data[i]
				fmt.Println(x)
			}
		}
		if err = it.Err(); err != nil {
			fmt.Printf("%+v", err)
		}

		for ; it.NextBatch(batch2); {
			data := batch2.(*orc.BytesColumnVector).Vector
			for i := 0; i < batch2.Len(); i++ {
				x := data[i]
				fmt.Println(string(x))
			}
		}
		if err = it.Err(); err != nil {
			fmt.Printf("%+v", err)
		}
	}*/

	it.Close()
}
