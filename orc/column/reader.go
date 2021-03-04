package column

import (
	"bytes"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/common"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
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
	if r.opts.CompressionKind == pb.CompressionKind_NONE {
		buf = tBuf
	} else {
		cBuf := &bytes.Buffer{}
		if err := common.DecompressChunks(r.opts.CompressionKind, cBuf, bytes.NewBuffer(tBuf)); err != nil {
			return err
		}
		buf = cBuf.Bytes()
	}

	// unmarshal will call Reset on index first
	r.index = &pb.RowIndex{}
	if err := proto.Unmarshal(buf, r.index); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (r *reader) getStridePositions(stride int) (pos []uint64, err error) {
	if r.index == nil {
		err = errors.New("no index")
		return
	}
	if stride < 1 {
		err = errors.New("stride < 1")
		return
	}
	if stride > len(r.index.Entry) {
		err = errors.Errorf("stride %d does not exist", stride)
	}
	pos = r.index.Entry[stride-1].Positions
	return
}

/*func (r *reader) close() {
	if err := r.f.Close(); err != nil {
		logger.Warn(err)
	}
}*/

func seek(rowNumber uint64, strideSize int, seekStride func(int) error, skip func(uint64) error) error {
	var offset uint64

	if rowNumber < uint64(strideSize) {
		offset = rowNumber
	} else {
		stride := rowNumber / uint64(strideSize)
		offset = rowNumber % (stride * uint64(strideSize))
		//entry = s.index.Entry[stride-1]

		if err := seekStride(int(stride)); err != nil {
			return err
		}
	}

	if err := skip(offset); err != nil {
		return err
	}
	return nil
}
