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

	//schema, err := reader.GetColumnSchema(1)
	schema, err := reader.GetColumnSchema(2)
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

	for it.NextStripe() {
		for ; it.NextBatch(batch); {
			//data := batch.(*orc.LongColumnVector).Vector
			data := batch.(*orc.BytesColumnVector).Vector
			for i := 0; i < batch.Len(); i++ {
				x := data[i]
				fmt.Println(x)
			}
		}
		if err = it.Err(); err != nil {
			fmt.Printf("%+v", err)
		}
	}

	it.Close()
}
