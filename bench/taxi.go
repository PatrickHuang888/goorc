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
}

func main() {
	path := "/u01/apache/orc/java/bench/data/generated/taxi/orc.gz"

	opts := orc.DefaultReaderOptions()
	opts.RowSize = 100_000

	reader, err := orc.NewFileReader(path, opts)
	if err != nil {
		fmt.Printf("create reader error: %+v", err)
		os.Exit(1)
	}

	schema := reader.GetSchema()
	log.Infof("schema: %s", schema.String())

	for i, stat := range reader.GetStatistics() {
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

		columnes := batch.Vector.([]*api.ColumnVector)

		vendorId := columnes[0].Vector.([]int64)
		fmt.Printf("vendor id %d, ", vendorId[0])

		pickupTime := columnes[1].Vector.([]api.Timestamp)
		fmt.Printf("pick-up time %s,", pickupTime[0].Time(nil).String())

		dropTime := columnes[2].Vector.([]api.Timestamp)
		fmt.Printf("drop-off time: %s, ", dropTime[0].Time(nil).String())

		passengerCount := columnes[3].Vector.([]int64)
		fmt.Printf("passenger_count: %d, ", passengerCount[0])

		tripDistance := columnes[4].Vector.([]float64)
		fmt.Printf("trip_distance: %f, ", tripDistance[0])

		pickupLongitude := columnes[5].Vector.([]float64)
		fmt.Printf("pickup_longitude: %f, ", pickupLongitude[0])

		pickLatitude := columnes[6].Vector.([]float64)
		fmt.Printf("pickup_latitude: %f, ", pickLatitude[0])

		rateCodeId := columnes[7].Vector.([]int64)
		fmt.Printf("ratecode_id: %d, ", rateCodeId[0])

		storeAndFwdFlag := columnes[8].Vector.([]string)
		fmt.Printf("store_and_fwd_flag %s, ", storeAndFwdFlag[0])

		dropoffLongitude := columnes[9].Vector.([]float64)
		fmt.Printf(" dropoff_longitude %f, ", dropoffLongitude[0])

		dropoffLatitude := columnes[10].Vector.([]float64)
		fmt.Printf("dropoff_latitude %f, ", dropoffLatitude[0])

		paymentType := columnes[11].Vector.([]int64)
		fmt.Printf("payment_type %d, ", paymentType[0])

		fareAmount := columnes[12].Vector.([]api.Decimal64)
		fmt.Printf("fare_amount %f, ", fareAmount[0].Float64())

		extra := columnes[13].Vector.([]api.Decimal64)
		fmt.Printf("extra %f, ", extra[0].Float64())

		mtaTax := columnes[14].Vector.([]api.Decimal64)
		fmt.Printf("mta_tax %f, ", mtaTax[0].Float64())

		tipAmount := columnes[15].Vector.([]api.Decimal64)
		fmt.Printf("tip_amount %f, ", tipAmount[0].Float64())

		trollsAmount := columnes[16].Vector.([]api.Decimal64)
		fmt.Printf("trolls_amount %f, ", trollsAmount[0].Float64())

		improvSurchage := columnes[17].Vector.([]api.Decimal64)
		fmt.Printf("improvement_surcharge %f, ", improvSurchage[0].Float64())

		totalAmount := columnes[18].Vector.([]api.Decimal64)
		fmt.Printf("total_amount %f, ", totalAmount[0].Float64())

		rows += batch.ReadRows
		fmt.Println(" rows now: ", rows)
	}
	fmt.Printf("total rows %d", rows)
}
