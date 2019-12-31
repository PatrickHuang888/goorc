package main

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"

	"github.com/patrickhuang888/goorc/orc"
)

func init() {
	log.SetLevel(log.InfoLevel)
}

func main() {
	path := "/u01/apache/orc/java/bench/data/generated/taxi/orc.gz"

	opts := orc.DefaultReaderOptions()
	reader, err := orc.NewReader(path, opts)
	if err != nil {
		fmt.Printf("create reader error: %+v", err)
		os.Exit(1)
	}

	schema := reader.GetSchema()
	schema.Print()
	//schema.Children = schema.Children[:9]

	stripes, err := reader.Stripes()
	if err != nil {
		fmt.Printf("%+v", err)
	}

	batch := schema.CreateReaderBatch(opts)

	var rows int
	var batchSize int

	for i, stripe := range stripes {

		for {
			err = stripe.NextBatch(batch)
			if err != nil {
				fmt.Printf("%+v", err)
				os.Exit(1)
			}

			columnes := batch.Vector.([]*orc.ColumnVector)

			values := columnes[0].Vector.([]int64)
			batchSize = len(values)
			if batchSize == 0 {
				break
			}
			fmt.Println("vendor id:")
			for j, v := range values {
				fmt.Printf("%d \n", v)
				if j > 10 {
					break
				}
			}

			/*if f.ColumnId() ==2 {
				fmt.Printf("pick-up time: %d\n", f.Rows())
				for j, v := range f.(*orc.TimestampColumn).Vector {
					fmt.Printf("%s\n", orc.GetTime(v).String())
					if j > 10 {
						break
					}
				}

			}

			if f.ColumnId() ==3 {
				fmt.Printf("drop-off time: %d\n", f.Rows())
				for j, v := range f.(*orc.TimestampColumn).Vector {
					fmt.Printf("%s\n", orc.GetTime(v).String())
					if j > 10 {
						break
					}
				}

			}

			if f.ColumnId()== 4 {
				fmt.Printf("passenger_count: %d\n", f.Rows())
				for j, v := range f.(*orc.LongColumn).Vector {
					fmt.Printf("%d \n", v)
					if j > 10 {
						break
					}
				}
			}

			if f.ColumnId()==5 {
				fmt.Printf("trip_distance: %d\n", f.Rows())
				for j, v := range f.(*orc.DoubleColumn).Vector {
					fmt.Printf("%f \n", v)
					if j > 10 {
						break
					}
				}
			}

			if f.ColumnId()==6 {
				fmt.Printf("pickup_longitude: %d\n", f.Rows())
				for j, v := range f.(*orc.DoubleColumn).Vector {
					fmt.Printf("%f \n", v)
					if j > 10 {
						break
					}
				}
			}

			if f.ColumnId()==7 {
				fmt.Printf("pickup_latitude: %d\n", f.Rows())
				for j, v := range f.(*orc.DoubleColumn).Vector {
					fmt.Printf("%f \n", v)
					if j > 10 {
						break
					}
				}
			}

			if f.ColumnId()==8 {
				fmt.Printf("ratecode_id: %d\n", f.Rows())
				for j, v := range f.(*orc.LongColumn).Vector {
					fmt.Printf("%d \n", v)
					if j > 10 {
						break
					}
				}
			}

			if f.ColumnId()==9 {
				fmt.Printf("store_and_fwd_flag rows: %d\n", f.Rows())
				for j, v := range f.(*orc.StringColumn).Vector {
					fmt.Printf("%s \n", v)
					if j > 10 {
						break
					}
				}
			}

			if f.ColumnId()==10 {
				fmt.Printf(" dropoff_longitude: %d\n", f.Rows())
				for j, v := range f.(*orc.DoubleColumn).Vector {
					fmt.Printf("%f \n", v)
					if j > 10 {
						break
					}
				}
			}

			if f.ColumnId()==11 {
				fmt.Printf("dropoff_latitude: %d\n", f.Rows())
				for j, v := range f.(*orc.DoubleColumn).Vector {
					fmt.Printf("%f \n", v)
					if j > 10 {
						break
					}
				}
			}

			if f.ColumnId()==12 {
				fmt.Printf("payment_type: %d\n", f.Rows())
				for j, v := range f.(*orc.LongColumn).Vector {
					fmt.Printf("%d \n", v)
					if j > 10 {
						break
					}
				}
			}

			if f.ColumnId()==13 {
				fmt.Printf("fare_amount: %d\n", f.Rows())
				for j, v := range f.(*orc.Decimal64Column).Vector {
					fmt.Printf("%s \n", v.String())
					if j > 10 {
						break
					}
				}
			}

			if f.ColumnId()==14 {
				fmt.Printf("extra: %d\n", f.Rows())
				for j, v := range f.(*orc.Decimal64Column).Vector {
					fmt.Printf("%s \n", v.String())
					if j > 10 {
						break
					}
				}
			}

			if f.ColumnId()==15 {
				fmt.Printf("mta_tax: %d\n", f.Rows())
				for j, v := range f.(*orc.Decimal64Column).Vector {
					fmt.Printf("%s \n", v.String())
					if j > 10 {
						break
					}
				}
			}*/

			rows += batchSize
			fmt.Printf("current stripe %d, rows now: %d\n", i, rows)
		}
	}
	fmt.Printf("total rows %d", rows)
}
