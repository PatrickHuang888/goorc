package orc

import (
	"testing"
)

func TestNoCompression(t *testing.T)  {
	opts := DefaultReaderOptions()
	reader, err := NewReader("../testing/basicLongNoCompression.orc", opts)
	if err != nil {
		t.Errorf("create reader error: %+v", err)
	}

	schema := reader.GetSchema()
	stripes, err := reader.Stripes()
	if err != nil {
		t.Fatalf("%+v", err)
	}

	for _, stripe := range stripes {
		batch, err := schema.CreateReaderBatch(opts)
		if err != nil {
			t.Errorf("create row batch error %+v", err)
		}

		for next := true; next; {
			next, err = stripe.NextBatch(batch)
			if err != nil {
				t.Fatalf("%+v", err)
			}
		}
	}
}
