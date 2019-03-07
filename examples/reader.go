package main

import (
	"fmt"
	"github.com/PatrickHuang888/goorc/hive"
	"github.com/PatrickHuang888/goorc/pb/pb"
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

	schema := orc.NewTypeDescription(pb.Type_STRUCT)
	xtd := orc.NewTypeDescription(pb.Type_INT)
	ytd := orc.NewTypeDescription(pb.Type_STRING)
	schema.AddField("x", xtd)
	schema.AddField("y", ytd)

	_, err = schema.CreateRowBatch(orc.ORIGINAL, hive.DEFAULT_ROW_SIZE)
	if err != nil {
		fmt.Printf("create row batch error %+v", err)
		os.Exit(1)
	}
	_, err= reader.Rows()
	if err!=nil {
		fmt.Printf("%+v", err)
	}

	/*x := batch.Cols[0].(*hive.LongColumnVector)
	y := batch.Cols[1].(*hive.BytesColumnVector)

	for ; rowIter.NextBatch(batch); {
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
