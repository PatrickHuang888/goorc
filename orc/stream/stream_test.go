package stream

import (
	"bytes"
	"github.com/patrickhuang888/goorc/orc/config"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/stretchr/testify/assert"
	"testing"
)

type bufSeeker struct {
	*bytes.Buffer
}

func (bs *bufSeeker) Seek(offset int64, whence int) (int64, error) {
	return offset, nil
}

func TestStreamReadWriteNoCompression(t *testing.T) {
	num:= 200
	data := make([]byte, num)
	for i := 0; i < 200; i++ {
		data[i]= byte(1)
	}

	// with no compression, default chunksize is buffer size
	wopts := &config.WriterOptions{CompressionKind: pb.CompressionKind_NONE}
	id := uint32(0)
	sw:= NewByteWriter(id, pb.Stream_DATA, wopts).(*encodingWriter)

	for _, v := range data {
		if err := sw.Write(v); err != nil {
			t.Fatal(err)
		}
	}
	if err:= sw.Flush();err!=nil {
		t.Fatal(err)
	}

	out := &bufSeeker{&bytes.Buffer{}}
	if _, err := sw.WriteOut(out); err != nil {
		t.Fatal(err)
	}

	vs := make([]byte, 500)
	ropts := &config.ReaderOptions{ChunkSize: 60, CompressionKind: pb.CompressionKind_NONE}
	ropts.MockTest= true

	sr, err:= NewByteReader(ropts, sw.info, 0, "/test")
	sr.stream.buf= sw.buf
	if err!=nil {
		t.Fatal(err)
	}

	for i:=0; i< num; i++ {
		data[i], err = sr.Next()
		if err != nil {
			t.Fatalf("%+v", err)
		}
	}
	if !sr.Finished() {
		t.Fatal("reader should be finished")
	}
	assert.Equal(t, data, vs)
}

