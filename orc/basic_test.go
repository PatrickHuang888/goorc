package orc

import (
	"bytes"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/patrickhuang888/goorc/pb/pb"
)

func init() {
	log.SetLevel(log.TraceLevel)
}

type bufSeeker struct {
	*bytes.Buffer
}

func (bs *bufSeeker) Seek(offset int64, whence int) (int64, error) {
	return offset, nil
}

func TestStreamReadWrite(t *testing.T) {
	data := &bytes.Buffer{}
	for i := 0; i < 100; i++ {
		data.WriteByte(byte(1))
	}
	for i := 0; i < 100; i++ {
		data.WriteByte(byte(i))
	}
	bs:= data.Bytes()

	opts := &WriterOptions{ChunkSize: 60, CompressionKind: pb.CompressionKind_ZLIB}
	k := pb.Stream_DATA
	id := uint32(0)
	l := uint64(0)
	info := &pb.Stream{Kind: &k, Column: &id, Length: &l}
	sw := &streamWriter{info: info, buf: &bytes.Buffer{}, opts: opts}
	_, err := sw.write(data)
	if err != nil {
		t.Fatal(err)
	}

	in:= &bufSeeker{&bytes.Buffer{}}
	if _, err:= sw.flush(in);err!=nil {
		t.Fatal(err)
	}

	vs := make([]byte, 500)
	ropts := &ReaderOptions{ChunkSize: 60, CompressionKind: pb.CompressionKind_ZLIB}
	sr := &streamReader{info: info, opts: ropts, buf: &bytes.Buffer{}, in: in}

	n, err := sr.Read(vs)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	assert.Equal(t, 200, n)
	assert.Equal(t, bs, vs[:n])
}
