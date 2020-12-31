package column

import (
	"bytes"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/common"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/orc/stream"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
	"io"
	"strings"
)

type reader struct {
	f      orcio.File
	schema *api.TypeDescription
	opts   *config.ReaderOptions

	index *pb.RowIndex

	present *stream.BoolReader
}

func (r *reader) String() string {
	sb := strings.Builder{}
	fmt.Fprintf(&sb, "id %d, ", r.schema.Id)
	fmt.Fprintf(&sb, "kind %stream, ", r.schema.Kind.String())
	fmt.Fprintf(&sb, "encoding %s, ", r.schema.Encoding.String())
	return sb.String()
}

func (r *reader) InitIndex(startOffset uint64, length uint64) error {
	if _, err := r.f.Seek(int64(startOffset), io.SeekStart); err != nil {
		return errors.WithStack(err)
	}

	var buf []byte
	tBuf := make([]byte, length)
	if _, err := io.ReadFull(r.f, tBuf); err != nil {
		return errors.WithStack(err)
	}
	if r.opts.CompressionKind==pb.CompressionKind_NONE {
		buf= tBuf
	}else {
		cBuf:= &bytes.Buffer{}
		if  err:=common.DecompressChunks(r.opts.CompressionKind, cBuf, bytes.NewBuffer(tBuf));err!=nil {
			return err
		}
		buf= cBuf.Bytes()
	}

	// unmarshal will call Reset on index first
	r.index = &pb.RowIndex{}
	if err := proto.Unmarshal(buf, r.index); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (r *reader) getIndexEntryAndOffset(rowNumber uint64) (entry *pb.RowIndexEntry, offset uint64) {
	if rowNumber < uint64(r.opts.IndexStride) {
		offset = rowNumber
		return
	}
	stride := rowNumber / uint64(r.opts.IndexStride)
	offset = rowNumber % (stride * uint64(r.opts.IndexStride))
	entry = r.index.GetEntry()[stride-1]
	return
}

func (r *reader) seekPresent(indexEntry *pb.RowIndexEntry) error {
	var chunk, chunkOffset, offset1, offset2 uint64
	if indexEntry != nil {
		pos := indexEntry.Positions
		if r.opts.CompressionKind == pb.CompressionKind_NONE {
			chunkOffset = pos[0]
			offset1 = pos[1]
			offset2 = pos[2]
		} else {
			chunk = pos[0]
			chunkOffset = pos[1]
			offset1 = pos[2]
			offset2 = pos[3]
		}
	}
	return r.present.Seek(chunk, chunkOffset, offset1, offset2)
}
