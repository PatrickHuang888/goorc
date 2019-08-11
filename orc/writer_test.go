package orc

import (
	"fmt"
	"github.com/PatrickHuang888/goorc/pb/pb"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
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

func TestTimestamp(t *testing.T) {
	schema := &TypeDescription{Kind: pb.Type_TIMESTAMP}
	wopts := DefaultWriterOptions()
	writer, err := NewWriter(workDir+"testTimestamp.orc", schema, wopts)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	batch, err := schema.CreateWriterBatch(wopts)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	var vector []Timestamp
	layout:= "2006-01-01 00:00:00.999999999"
	v, _ := time.Parse(layout, "2037-01-01 00:00:00.000999")
	vector = append(vector, Timestamp(v))
	v, _ = time.Parse(layout, "2003-01-01 00:00:00.000000222")
	vector = append(vector, Timestamp(v))
	v, _ = time.Parse(layout, "2003-01-01 00:00:00.000000222")
	vector = append(vector, Timestamp(v))
	v, _ = time.Parse(layout, "1995-01-01 00:00:00.688888888")
	vector = append(vector, Timestamp(v))
	v, _ = time.Parse(layout, "2002-01-01 00:00:00.1")
	vector = append(vector, Timestamp(v))
	v, _ = time.Parse(layout, "2010-03-02 00:00:00.000009001")
	vector = append(vector, Timestamp(v))
	v, _ = time.Parse(layout, "2005-01-01 00:00:00.000002229")
	vector = append(vector, Timestamp(v))
	v, _ = time.Parse(layout, "2006-01-01 00:00:00.900203003")
	vector = append(vector, Timestamp(v))
	v, _ = time.Parse(layout, "2003-01-01 00:00:00.800000007")
	vector = append(vector, Timestamp(v))
	v, _ = time.Parse(layout, "1996-08-02 00:00:00.723100809")
	vector = append(vector, Timestamp(v))
	v, _ = time.Parse(layout, "1998-11-02 00:00:00.857340643")
	vector = append(vector, Timestamp(v))
	v, _ = time.Parse(layout, "2008-10-02 00:00:00")
	vector = append(vector, Timestamp(v))

	batch.(*TimestampColumn).Vector = vector
	if err := writer.Write(batch); err != nil {
		t.Fatalf("%+v", err)
	}
	writer.Close()
}
