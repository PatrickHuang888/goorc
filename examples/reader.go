package main

import (
	"fmt"
	"os"

	"github.com/patrickhuang888/goorc/orc"
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
)

func main() {

	opts := config.DefaultReaderOptions()
	reader, err := orc.NewOSFileReader("my-file-w.orc", opts)
	if err != nil {
		fmt.Printf("create reader error: %+v", err)
		os.Exit(1)
	}

	schema := reader.GetSchema()
	batch, err := api.CreateReaderBatch(schema, opts)
	if err != nil {
		fmt.Printf("%+v", err)
		os.Exit(1)
	}

	if err:= reader.Next(&batch);err!=nil {
		fmt.Printf("%+v", err)
		os.Exit(1)
	}

	for i:=0; i<batch.Len(); i++ {
		fmt.Printf("x %d, ", batch.Children[0].Vector[i].V)
		fmt.Printf("y %s \n", batch.Children[1].Vector[i].V)
	}

	reader.Close()
}
