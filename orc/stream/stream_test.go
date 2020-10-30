package stream

import (
	"bytes"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/pb/pb"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
)

func init() {
	log.SetLevel(log.TraceLevel)
}

func TestNoCompression(t *testing.T) {
	var err error

	num := 200
	data := make([]byte, num)
	for i := 0; i < 200; i++ {
		data[i] = byte(1)
	}

	wopts := &config.WriterOptions{ChunkSize: 60, CompressionKind: pb.CompressionKind_NONE}
	id := uint32(0)
	sw := NewByteWriter(id, pb.Stream_DATA, wopts)

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
	assert.Equal(t, data, vs)
}

func TestCompression(t *testing.T) {
	var err error

	buf := &bytes.Buffer{}
	for i := 0; i < 100; i++ {
		buf.WriteByte(byte(1))
	}
	for i := 0; i < 200; i++ {
		buf.WriteByte(byte(i))
	}
	data := buf.Bytes()

	opts := config.DefaultWriterOptions()
	id := uint32(0)
	w := NewByteWriter(id, pb.Stream_DATA, &opts)

	for _, v := range data {
		if err = w.Write(v); err != nil {
			t.Fatal(err)
		}
	}

	if err = w.Flush(); err != nil {
		t.Fatal(err)
	}

	bb := make([]byte, 500)
	f := orcio.NewMockFile(bb)
	if _, err = w.WriteOut(f); err != nil {
		t.Fatalf("%+v", err)
	}

	ropts := config.DefaultReaderOptions()
	sr := NewByteReader(&ropts, w.info, 0, f)

	vv := make([]byte, 300)
	for i := 0; i < 300; i++ {
		vv[i], err = sr.Next()
		if err != nil {
			t.Fatalf("%+v", err)
		}
	}

	if !sr.Finished() {
		t.Fatal("reader should be finished")
	}
	assert.Equal(t, data, vv)
}

func TestBoolWithCompression(t *testing.T) {
	var err error

	rows := 100
	data := make([]bool, rows)
	for i := 0; i < rows; i++ {
		data[i] = true
	}
	data[0] = false
	data[45] = false
	data[98] = false

	opts := config.DefaultWriterOptions()
	id := uint32(0)
	w := NewBoolWriter(id, pb.Stream_DATA, &opts)

	for _, v := range data {
		if err = w.Write(v); err != nil {
			t.Fatal(err)
		}
	}

	if err = w.Flush(); err != nil {
		t.Fatal(err)
	}

	bb := make([]byte, 500)
	f := orcio.NewMockFile(bb)
	if _, err = w.WriteOut(f); err != nil {
		t.Fatalf("%+v", err)
	}

	ropts := config.DefaultReaderOptions()
	sr := NewBoolReader(&ropts, w.info, 0, f)

	vv := make([]bool, rows)
	for i := 0; i < rows; i++ {
		vv[i], err = sr.Next()
		if err != nil {
			t.Fatalf("%+v", err)
		}
	}

	assert.Equal(t, data, vv)
}

func TestMultiChunkWithCompression(t *testing.T) {
	var err error

	buf := &bytes.Buffer{}
	for i := 0; i < 100; i++ {
		buf.WriteByte(byte(1))
	}
	for i := 0; i < 200; i++ {
		buf.WriteByte(byte(i))
	}
	data := buf.Bytes()

	// expand to several chunks
	opts := &config.WriterOptions{ChunkSize: 60, CompressionKind: pb.CompressionKind_ZLIB}
	id := uint32(0)
	w := NewByteWriter(id, pb.Stream_DATA, opts)

	for _, v := range data {
		if err = w.Write(v); err != nil {
			t.Fatal(err)
		}
	}

	if err = w.Flush(); err != nil {
		t.Fatal(err)
	}

	bb := make([]byte, 500)
	f := orcio.NewMockFile(bb)
	if _, err = w.WriteOut(f); err != nil {
		t.Fatalf("%+v", err)
	}

	ropts := &config.ReaderOptions{ChunkSize: 60, CompressionKind: pb.CompressionKind_ZLIB}
	sr := NewByteReader(ropts, w.info, 0, f)

	vv := make([]byte, 300)
	for i := 0; i < 300; i++ {
		vv[i], err = sr.Next()
		if err != nil {
			t.Fatalf("%+v", err)
		}
	}

	if !sr.Finished() {
		t.Fatal("reader should be finished")
	}
	assert.Equal(t, data, vv)

	// todo: test above always write original, no compressing selected
}
