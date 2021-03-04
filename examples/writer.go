package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/patrickhuang888/goorc/orc"
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	"github.com/patrickhuang888/goorc/pb/pb"
)

func main() {
	schema := &api.TypeDescription{Kind: pb.Type_STRUCT}
	x := &api.TypeDescription{Kind: pb.Type_INT}
	x.Encoding = pb.ColumnEncoding_DIRECT_V2
	y := &api.TypeDescription{Kind: pb.Type_STRING}
	y.Encoding = pb.ColumnEncoding_DIRECT_V2
	schema.ChildrenNames = []string{"x", "y"}
	schema.Children = []*api.TypeDescription{x, y}

	opts := config.DefaultWriterOptions()
	opts.RowSize = 1500

	writer, err := orc.NewOSFileWriter("my-file-w.orc", schema, opts)
	if err != nil {
		fmt.Printf("create writer error %+v\n", err)
		os.Exit(1)
	}

	batch, err := api.CreateWriterBatch(schema, opts)
	if err != nil {
		fmt.Printf("got error when create row batch %v+", err)
		os.Exit(1)
	}

	for i := 0; i < opts.RowSize; i++ {
		batch.Children[0].Vector[i].V = int32(i)
		batch.Children[1].Vector[i].V = fmt.Sprintf("string-%s", strconv.Itoa(i))
	}

	if err := writer.Write(&batch); err != nil {
		fmt.Printf("write error %+v\n", err)
		os.Exit(1)
	}

	if err := writer.Close(); err != nil {
		fmt.Printf("close error %+v\n", err)
	}
}
