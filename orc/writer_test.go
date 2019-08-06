package orc

import (
	"fmt"
	"github.com/PatrickHuang888/goorc/pb/pb"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

var workDir = os.TempDir() + string(os.PathSeparator)

func TestDecimalWriter(t *testing.T) {
	path := workDir + "TestOrcDecimal.orc"

	schema := &TypeDescription{Kind: pb.Type_STRUCT}
	x := &TypeDescription{Kind: pb.Type_DECIMAL}
	schema.ChildrenNames = []string{"x"}
	schema.Children = []*TypeDescription{x}
	wopts := DefaultWriterOptions()
	writer, err := NewWriter(path, schema, wopts)
	if err != nil {
		fmt.Printf("create writer error %+v", err)
		os.Exit(1)
	}

	batch, err := schema.CreateWriterBatch(wopts)
	if err != nil {
		fmt.Printf("got error when create row batch %v+", err)
		os.Exit(1)
	}

	vector := make([]int64, 19)
	vector[0] = 1
	for i := 1; i < 18; i++ {
		vector[i] = vector[i-1] * 10
	}
	vector[18] = -2000

	column := batch.(*StructColumn).Fields[0].(*Decimal64Column)
	column.Vector = vector
	column.Scale = 3

	if err := writer.Write(batch); err != nil {
		t.Fatalf("%+v", err)
	}

	writer.Close()

	ropts := DefaultReaderOptions()
	reader, err := NewReader(path, ropts)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	schema = reader.GetSchema()
	stripes, err := reader.Stripes()
	if err != nil {
		fmt.Printf("%+v", err)
	}

	for _, stripe := range stripes {
		batch, err := schema.CreateReaderBatch(ropts)
		if err != nil {
			fmt.Printf("create row batch error %+v", err)
			os.Exit(1)
		}

		_, err = stripe.NextBatch(batch)
		if err != nil {
			fmt.Printf("%+v", err)
			break
		}

		assert.Equal(t, 19, batch.Rows())
		x := batch.(*StructColumn).Fields[0].(*Decimal64Column)
		assert.Equal(t, 3, int(x.Scale))
		assert.Equal(t, 1, int(x.Vector[0]), "row 0")
		for i := 1; i < 18; i++ {
			assert.Equal(t, 10*x.Vector[i-1], x.Vector[i])
		}

		assert.Equal(t, -2000, int(x.Vector[18]))
	}

	reader.Close()
}
