package orc

import (
	"fmt"
	"github.com/PatrickHuang888/goorc/pb/pb"
	"os"
	"testing"
)

var workDir = os.TempDir() + string(os.PathSeparator)+"test"+string(os.PathSeparator)


func TestDecimalWriter(t *testing.T) {
	path:= workDir + "TestOrcDecimal.orc"

	schema := &TypeDescription{Kind:pb.Type_STRUCT}
	x := &TypeDescription{Kind:pb.Type_DECIMAL}
	schema.ChildrenNames= []string{"x"}
	schema.Children=[]*TypeDescription{x}
	opts := DefaultWriterOptions()
	writer, err := NewWriter(path, schema, opts)
	if err != nil {
		fmt.Printf("create writer error %+v", err)
		os.Exit(1)
	}

	batch, err := schema.CreateWriterBatch(opts)
	if err != nil {
		fmt.Printf("got error when create row batch %v+", err)
		os.Exit(1)
	}


}
