package main

import (
	"fmt"
	"github.com/patrickhuang888/goorc/orc/api"
	"os"

	log "github.com/sirupsen/logrus"

	"github.com/patrickhuang888/goorc/orc"
)

func init() {
	log.SetLevel(log.InfoLevel)
	//orc.SetLogLevel(log.DebugLevel)
}

func main() {
	path := "/u01/apache/orc/java/bench/data/generated/taxi/orc.gz"

	reader, err := orc.NewOSFileReader(path)
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
		if end, err = br.Next(batch);err != nil {
			fmt.Printf("%+v", err)
			os.Exit(1)
		}


		/*columnes := batch.Children

		vendorId := columnes[0].Vector
		fmt.Printf("vendor id %d, ", vendorId[0].V)

		pickupTime := columnes[1].Vector
		fmt.Printf("pick-up time %s,", pickupTime[0].V.(api.Timestamp).Time().String())

		dropTime := columnes[2].Vector
		fmt.Printf("drop-off time: %s, ", dropTime[0].V.(api.Timestamp).Time().String())

		passengerCount := columnes[3].Vector
		fmt.Printf("passenger_count: %d, ", passengerCount[0].V.(int32))

		tripDistance := columnes[4].Vector
		fmt.Printf("trip_distance: %f, ", tripDistance[0].V.(float64))

		storeAndFwdFlag := columnes[8].Vector
		fmt.Printf("store_and_fwd_flag %s, ", storeAndFwdFlag[0].V.(string))

		extra := columnes[13].Vector
		fmt.Printf("extra %f, ", extra[0].V.(api.Decimal64).Float64())
*/
		/*pickupLongitude := columnes[5].Vector
		fmt.Printf("pickup_longitude: %f, ", pickupLongitude[0].V.(float64))

		pickLatitude := columnes[6].Vector
		fmt.Printf("pickup_latitude: %f, ", pickLatitude[0].V.(float64))

		rateCodeId := columnes[7].Vector
		fmt.Printf("ratecode_id: %d, ", rateCodeId[0].V.(int32))*/


		/*dropoffLongitude := columnes[9].Vector.([]float64)
		fmt.Printf(" dropoff_longitude %f, ", dropoffLongitude[0])

		dropoffLatitude := columnes[10].Vector.([]float64)
		fmt.Printf("dropoff_latitude %f, ", dropoffLatitude[0])

		paymentType := columnes[11].Vector.([]int64)
		fmt.Printf("payment_type %d, ", paymentType[0])*/

		/*fareAmount := columnes[12].Vector
		fmt.Printf("fare_amount %f, ", fareAmount[0].V.(api.Decimal64).Float64())*/


		/*mtaTax := columnes[14].Vector.([]api.Decimal64)
		fmt.Printf("mta_tax %f, ", mtaTax[0].Float64())

		tipAmount := columnes[15].Vector.([]api.Decimal64)
		fmt.Printf("tip_amount %f, ", tipAmount[0].Float64())

		trollsAmount := columnes[16].Vector.([]api.Decimal64)
		fmt.Printf("trolls_amount %f, ", trollsAmount[0].Float64())

		improvSurchage := columnes[17].Vector.([]api.Decimal64)
		fmt.Printf("improvement_surcharge %f, ", improvSurchage[0].Float64())

		totalAmount := columnes[18].Vector.([]api.Decimal64)
		fmt.Printf("total_amount %f, ", totalAmount[0].Float64())*/

		rows += batch.Len()
		fmt.Println(" rows now: ", rows)
	}
	fmt.Printf("total rows %d", rows)
}
