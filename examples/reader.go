package main

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"os"

	"github.com/PatrickHuang888/goorc/orc"
)

func main() {
	//reader, err := orc.CreateReader("/u01/apache/orc/java/examples/my-file.orc")
	logrus.SetLevel(logrus.DebugLevel)

	opts := orc.DefaultReaderOptions()
	reader, err := orc.CreateReader("my-file-w.orc", opts)
	if err != nil {
		fmt.Printf("create reader error: %+v", err)
		os.Exit(1)
	}
	fmt.Printf("row count: %d\n", reader.NumberOfRows())

	schema := reader.GetSchema()
	stripes, err := reader.Stripes()
	if err != nil {
		fmt.Printf("%+v", err)
	}
	for _, stripe := range stripes {
		batch, err := schema.CreateReaderBatch(opts)
		if err != nil {
			fmt.Printf("create row batch error %+v", err)
			os.Exit(1)
		}

		for next := true; next; {
			next, err = stripe.NextBatch(batch)
			if err != nil {
				fmt.Printf("%+v", err)
				break
			}

			//data := batch.(*orc.StructColumnVector).GetFields()
			x := batch.(*orc.StringColumn)
			//x:= data[0].(*orc.LongColumnVector)
			if !x.HasNulls() {
				for _, v := range x.Vector {
					fmt.Println(v)
				}
			}
			/*y:= data[1].(*orc.BytesColumnVector)
			for _,v := range y.GetVector() {
				fmt.Println(string(v))
			}*/
		}
	}

	reader.Close()
}
