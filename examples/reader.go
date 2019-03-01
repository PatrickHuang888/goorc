package main

import (
	"fmt"
	"os"

	"github.com/PatrickHuang888/goorc/orc"
)

func main() {
	_, err := orc.CreateReader("/u01/apache/orc/java/examples/my-file.orc")
	if err != nil {
		fmt.Printf("create reader error: %+v", err)
		os.Exit(1)
	}
	//fmt.Printf("row count: %d\n", reader.NumberOfRows())

	schema := orc.NewTypeDescription(orc.STRUCT)
	xtd := orc.NewTypeDescription(orc.INT)
	ytd := orc.NewTypeDescription(orc.STRING)
	schema.AddField("x", xtd)
	schema.AddField("y", ytd)

	/*batch, err := schema.CreateRowBatch(orc.ORIGINAL, hive.DEFAULT_ROW_SIZE)
	if err != nil {
		fmt.Printf("create row batch error %v+", err)
		os.Exit(1)
	}*/
	/*rowIter:= reader.Rows()

	x := batch.Cols[0].(*hive.LongColumnVector)
	y := batch.Cols[1].(*hive.BytesColumnVector)*/

	/*for ; rowIter.NextBatch(batch); {
		for row := 0; row < batch.Size; row++ {
			xRow := 0
			if !x.Repeating {
				xRow = row
			}
			fmt.Printf("x: %d\n", x.Vector[xRow])
			fmt.Printf("y: %s\n", string(y.Vector[row]))
		}
	}
	rowIter.Close()*/
}
