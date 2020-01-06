package orc

import (
	"bytes"
	"github.com/patrickhuang888/goorc/pb/pb"
	log "github.com/sirupsen/logrus"
	"testing"
)

func init() {
	log.SetLevel(log.TraceLevel)
}


func TestStreamReadWrite(t *testing.T) {
	data:= &bytes.Buffer{}
	for i:=0; i<=150; i++ {
		data.WriteByte(byte(1))
	}

	opts:= &WriterOptions{ChunkSize:100, CMPKind:pb.CompressionKind_ZLIB}
	k:= pb.Stream_DATA
	id:= uint32(0)
	l:= uint64(0)
	info:= &pb.Stream{Kind:&k, Column:&id, Length:&l}
	sw:= streamWriter{info:info, buf: &bytes.Buffer{}, opts:opts}
	_, err:= sw.write(data)
	if err!=nil {
		t.Fatal(err)
	}


}




