package main

import (
	"fmt"
	"github.com/PatrickHuang888/goorc/hive"
	"github.com/PatrickHuang888/goorc/orc"
	"os"
)

func main() {
	schema := orc.NewTypeDescription(orc.STRUCT)
	x := orc.NewTypeDescription(orc.INT)
	y := orc.NewTypeDescription(orc.STRING)
	schema.AddField("x", x)
	schema.AddField("y", y)
	opts := &orc.WriterOptions{Schema: schema}
	writer := orc.NewWriter("my-file.orc", opts)

	batch, err := schema.CreateRowBatch(orc.ORIGINAL, hive.DEFAULT_ROW_SIZE)
	if err != nil {
		fmt.Printf("got error when create row batch %v+", err)
		os.Exit(1)
	}
	vx := batch.Cols[0]
	vy := batch.Cols[1]

	for r := 0; r < 10000; r++ {
		row := batch.size
		batch.size += 1
		vx.Vector[row] = r
		vy.Vector[row] = "Last-" + string(r*3)

		if batch.size == MAXSIZE {
			writer.AddRowBatch(batch)
			batch.reset()
		}
	}

	if batch.size != 0 {
		writer.AddRowBatch(batch)
	}
	writer.Close()
}
