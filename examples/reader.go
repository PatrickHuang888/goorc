package main

import (
	"fmt"
	"os"

	"github.com/patrickhuang888/goorc/orc"
)

func main() {

	opts := orc.DefaultReaderOptions()
	reader, err := orc.NewReader("my-file-w.orc", opts)
	if err != nil {
		fmt.Printf("create reader error: %+v", err)
		os.Exit(1)
	}

	schema := reader.GetSchema()
	stripes, err := reader.Stripes()
	if err != nil {
		fmt.Printf("%+v", err)
	}

	for _, stripe := range stripes {
		batch, err := schema.CreateReaderBatch(opts)
		if err != nil {
			fmt.Printf("create row batch error %+v", err)
			os.Exit(1)
		}

		for next := true; next; {
			next, err = stripe.NextBatch(batch)
			if err != nil {
				fmt.Printf("%+v", err)
				break
			}

			data := batch.(*orc.StructColumn).Fields
			x := data[0].(*orc.LongColumn)
			y := data[1].(*orc.StringColumn)
			for i:=0; i<batch.Rows(); i++ {
				if x.HasNulls() && x.Nulls[i]{
						fmt.Println("x: null")
				}else {
					fmt.Printf("x: %d, ", x.Vector[i])
				}
				if y.HasNulls() && y.Nulls[i] {
					fmt.Println("y: null")
				}else {
					fmt.Printf("y: %s\n", y.Vector[i])
				}
			}

		}
	}

	reader.Close()
}
