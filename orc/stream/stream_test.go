package stream

import (
	"bytes"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStreamReadWriteNoCompression(t *testing.T) {
	var err error

	num := 200
	data := make([]byte, num)
	for i := 0; i < 200; i++ {
		data[i] = byte(1)
	}

	// with no compression, default chunksize is buffer size
	wopts := &config.WriterOptions{CompressionKind: pb.CompressionKind_NONE}
	id := uint32(0)
	sw := NewByteWriter(id, pb.Stream_DATA, wopts).(*encodingWriter)

	for _, v := range data {
		if err = sw.Write(v); err != nil {
			t.Fatal(err)
		}
	}
	if err = sw.Flush(); err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 500)
	f := orcio.NewMockFile(buf)
	if _, err := sw.WriteOut(f); err != nil {
		t.Fatal(err)
	}

	ropts := &config.ReaderOptions{ChunkSize: 60, CompressionKind: pb.CompressionKind_NONE}

	sr := NewByteReader(ropts, sw.info, 0, f)
	sr.stream.buf = sw.buf


	vs := make([]byte, 200)
	for i := 0; i < num; i++ {
		vs[i], err = sr.Next()
		if err != nil {
			t.Fatalf("%+v", err)
		}
	}

	if !sr.Finished() {
		t.Fatal("reader should be finished")
	}
	assert.Equal(t, data, vs[:num])
}


func TestStreamReadWriteMultipleChunksWithCompression(t *testing.T) {
	var err error

	data := &bytes.Buffer{}
	for i := 0; i < 100; i++ {
		data.WriteByte(byte(1))
	}
	for i := 0; i < 200; i++ {
		data.WriteByte(byte(i))
	}
	bs := data.Bytes()

	// expand to several chunks
	opts := &config.WriterOptions{ChunkSize: 100, CompressionKind: pb.CompressionKind_ZLIB}
	id := uint32(0)
	w := NewByteWriter(id, pb.Stream_DATA, opts).(*encodingWriter)

	for _, b := range bs {
		if err=w.Write(b);err!=nil {
			t.Fatal(err)
		}
	}
	
	if _, err := sw.write(bs);err != nil {
		t.Fatal(err)
	}

	out := &bufSeeker{&bytes.Buffer{}}
	if _, err := sw.writeOut(out); err != nil {
		t.Fatal(err)
	}

	vs := make([]byte, 500)
	ropts := &orc.ReaderOptions{ChunkSize: 100, CompressionKind: pb.CompressionKind_ZLIB}
	*info.Length= uint64(out.Len())
	sr := &orc.streamReader{info: info, opts: ropts, buf: &bytes.Buffer{}, in: out}

	n, err := sr.Read(vs)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, 300, n)
	assert.Equal(t, bs, vs[:n])
}
