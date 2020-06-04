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
	opts.RowSize= 100_000

	reader, err := orc.NewFileReader(path, opts)
	if err != nil {
		fmt.Printf("create reader error: %+v", err)
		os.Exit(1)
	}

	schema := reader.GetSchema()
	log.Infof("schema: %s", schema.String())

	for i, stat:= range reader.GetStatistics(){
		log.Infof("id %d, stat: %s", i, stat.String())
	}

	batch := schema.CreateReaderBatch(opts)

	var rows int

	for {
		err = reader.Next(batch)
		if err != nil {
			fmt.Printf("%+v", err)
			os.Exit(1)
		}

		if batch.ReadRows == 0 {
			break
		}

		columnes := batch.Vector.([]*orc.ColumnVector)

		vendorId := columnes[0].Vector.([]int64)
		fmt.Printf("vendor id %d, ", vendorId[0])

		pickupTime:= columnes[1].Vector.([]orc.Timestamp)
		fmt.Printf("pick-up time %s,", pickupTime[0].Time(nil).String())

		dropTime:= columnes[2].Vector.([]orc.Timestamp)
			fmt.Printf("drop-off time: %s, ", dropTime[0].Time(nil).String())


		passengerCount:= columnes[3].Vector.([]int64)
			fmt.Printf("passenger_count: %d, ", passengerCount[0])


		tripDistance:= columnes[4].Vector.([]float64)
			fmt.Printf("trip_distance: %f, ", tripDistance[0])

		/*if f.ColumnId()==6 {
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

		rows += batch.ReadRows
		fmt.Println("rows now: ", rows)
	}
	fmt.Printf("total rows %d", rows)
}
