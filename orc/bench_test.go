package orc

import (
	"fmt"
	"github.com/patrickhuang888/goorc/orc/api"
	"os"
	"testing"
)

func BenchmarkScan(b *testing.B) {
	path := "/u01/apache/orc/java/bench/data/generated/taxi/orc.gz"

	reader, err := NewOSFileReader(path)
	if err != nil {
		fmt.Printf("create reader error: %+v", err)
		os.Exit(1)
	}

	schema := reader.GetSchema()
	bopt := &api.BatchOption{RowSize: 10_000}

	batch, err := schema.CreateVector(bopt)
	if err != nil {
		fmt.Printf("%+v", err)
		os.Exit(1)
	}

	br, err := reader.CreateBatchReader(bopt)
	if err != nil {
		fmt.Printf("%+v", err)
		os.Exit(1)
	}

	var rows int
	var end bool

	for !end {
		if end, err = br.Next(batch); err != nil {
			fmt.Printf("%+v", err)
			os.Exit(1)
		}

		rows += batch.Len()
	}
	fmt.Printf("total rows %d", rows)
}

