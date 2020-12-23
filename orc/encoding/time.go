package encoding

import (
	"bytes"
	"github.com/patrickhuang888/goorc/orc/api"
)

func NewDateV2Encoder(doPositioning bool) Encoder {
	return &dateV2Encoder{NewIntRLV2(true, doPositioning)}
}

type dateV2Encoder struct {
	intEncoder Encoder
}

func (e *dateV2Encoder) Encode(v interface{}, out *bytes.Buffer) error {
	date := v.(api.Date)
	days := api.ToDays(date)
	if err := e.intEncoder.Encode(days, out); err != nil {
		return err
	}
	return nil
}

func (e *dateV2Encoder) Flush(out *bytes.Buffer) error {
	return e.intEncoder.Flush(out)
}

func (e dateV2Encoder) GetPosition() []uint64 {
	return e.intEncoder.GetPosition()
}

func NewDateV2DeCoder() DateDecoder {
	return &dateV2Decoder{NewIntDecoder(true)}
}

type dateV2Decoder struct {
	intDecoder IntDecoder
}

func (d *dateV2Decoder) Decode(in BufferedReader) (dates []api.Date, err error) {
	days, err := d.intDecoder.DecodeInt(in)
	if err != nil {
		return
	}
	for _, v := range days {
		dates = append(dates, api.FromDays(int32(v)))
	}
	return
}
