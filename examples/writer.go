package main

import (
	"github.com/PatrickHuang888/goorc/orc"
)

func main() {
	schema:= orc.NewTypeDescription(orc.STRUCT)
	x := orc.NewTypeDescription{orc.INT}
	y := orc.NewTypeDescription{orc.STRING}
	schema.AddField("x", x)
	schema.AddField("y", y)
	opts:= orc.NewWriterOptions()
	opts.setSchema(schema)
	writer:= orc.NewWriter("my-file.orc", opts)

	batch:= schema.CreateRowBatch()
	vx:= batch[0]
	vy:= batch[1]

	for r := 0; r<10000; r++ {
		row:= batch.size
		batch.size+=1
		vx.Vector[row]= r
		vy.Vector[row]= "Last-"+ string(r*3)

		if batch.size==MAXSIZE {
			writer.AddRowBatch(batch)
			batch.reset()
		}
	}

	if batch.size!=0 {
		writer.AddRowBatch(batch)
	}
	writer.Close()
}
