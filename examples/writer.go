package main

import (
	"fmt"
	"github.com/PatrickHuang888/goorc/orc"
	"github.com/PatrickHuang888/goorc/pb/pb"
	"github.com/sirupsen/logrus"
	"os"
	"strconv"
)

func init() {
	logrus.SetLevel(logrus.TraceLevel)
}

func main() {
	//schema := &orc.TypeDescription{Kind:pb.Type_STRUCT}
	//x := &orc.TypeDescription{Kind:pb.Type_INT}
	x := &orc.TypeDescription{Kind: pb.Type_STRING}
	//schema.ChildrenNames= []string{"x", "y"}
	//schema.Children=[]*orc.TypeDescription{x, y}
	/*schema, err:= orc.CreateSchema(x)
	if err!=nil {
		fmt.Printf("create schema error %v+", err)
		os.Exit(1)
	}*/

	opts := orc.DefaultWriterOptions()
	writer, err := orc.NewWriter("my-file-w.orc", x, opts)
	if err != nil {
		fmt.Printf("create writer error %+v", err)
		os.Exit(1)
	}

	batch, err := x.CreateVectorBatch(orc.DEFAULT_ROW_SIZE)
	if err != nil {
		fmt.Printf("got error when create row batch %v+", err)
		os.Exit(1)
	}

	v := make([]string, 3000)
	for i := 0; i < 3000; i++ {
		v[i] = fmt.Sprintf("string-%s", strconv.Itoa(i))
	}
	//batch.(*orc.LongColumnVector).SetVector(v)
	batch.(*orc.StringColumn).SetVector(v)
	if err := writer.Write(batch); err != nil {
		fmt.Printf("write error %+v", err)
		os.Exit(1)
	}
	//writer.WriteBatch()
	//batch.(*orc.LongColumnVector).SetVector(v[:1000])
	//writer.AddRowBatch(batch)
	//writer.Flush()
	if err := writer.Close(); err != nil {
		fmt.Printf("close error %+v", err)
	}
}
