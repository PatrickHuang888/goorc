package encoding

import (
	"bytes"
	"github.com/patrickhuang888/goorc/orc/api"
)

type dateV2Encoder struct {
	intEncoder *intRLV2Encoder
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
