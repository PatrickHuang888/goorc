package column

import (
	"github.com/patrickhuang888/goorc/orc/api"
	"github.com/patrickhuang888/goorc/orc/config"
	orcio "github.com/patrickhuang888/goorc/orc/io"
	"github.com/patrickhuang888/goorc/orc/stream"
	"github.com/patrickhuang888/goorc/pb/pb"
	"github.com/pkg/errors"
	"io"
)

func NewMapV2Reader(schema *api.TypeDescription, opts *config.ReaderOptions, f orcio.File) Reader {
	return &MapV2Reader{reader: reader{schema: schema, opts: opts, f: f}}
}

type MapV2Reader struct {
	reader

	present *stream.BoolReader
	length  *stream.IntRLV2Reader

	key   Reader
	value Reader
}

func (r *MapV2Reader) SetKey(key Reader) {
	r.key = key
}

func (r *MapV2Reader) SetValue(value Reader) {
	r.value = value
}

func (r *MapV2Reader) InitStream(info *pb.Stream, startOffset uint64) error {
	f, err := r.f.Clone()
	if err != nil {
		return err
	}
	if _, err := f.Seek(int64(startOffset), io.SeekStart); err != nil {
		return err
	}

	if info.GetKind() == pb.Stream_PRESENT {
		if !r.schema.HasNulls {
			return errors.New("schema has no nulls")
		}
		r.present = stream.NewBoolReader(r.opts, info, startOffset, f)
		return nil
	}
	if info.GetKind() == pb.Stream_LENGTH {
		r.length = stream.NewIntRLV2Reader(r.opts, info, startOffset, false, f)
		return nil
	}
	return errors.New("struct column no stream other than present")
}

func (r *MapV2Reader) NextBatch(vec *api.ColumnVector) error {
	var err error

	for i := 0; i < len(vec.Vector); i++ {
		if r.present != nil {
			var p bool
			if p, err = r.present.Next(); err != nil {
				return err
			}
			vec.Vector[i].Null = !p
		}

		if !vec.Vector[i].Null {
			v, err := r.length.NextUInt64()
			if err != nil {
				return err
			}
			vec.Vector[i].V = int(v)
		}

		// prepare key/value vector
		vec.Children[0].Vector = vec.Children[0].Vector[:0]
		vec.Children[1].Vector = vec.Children[1].Vector[:0]
		if vec.Vector[i].Null {
			vec.Children[0].Vector = append(vec.Children[0].Vector, api.Value{Null: true})
			vec.Children[1].Vector = append(vec.Children[1].Vector, api.Value{Null: true})
		} else {
			for j := 0; j < vec.Vector[i].V.(int); j++ {
				vec.Children[0].Vector = append(vec.Children[0].Vector, api.Value{})
				vec.Children[1].Vector = append(vec.Children[1].Vector, api.Value{})
			}
		}

		if err = r.key.NextBatch(vec.Children[0]); err != nil {
			return err
		}
		if err = r.value.NextBatch(vec.Children[1]); err != nil {
			return err
		}
	}
	return nil
}

func (r *MapV2Reader) Skip(rows uint64) error {
	var err error
	p := true

	for i := 0; i < int(rows); i++ {
		if r.present != nil {
			if p, err = r.present.Next(); err != nil {
				return err
			}
		}

		if p {
			l, err := r.length.NextUInt64()
			if err != nil {
				return err
			}
			for j := 0; j < int(l); j++ {
				if err := r.key.Skip(l); err != nil {
					return err
				}
				if err := r.value.Skip(l); err != nil {
					return err
				}
			}
		}
	}
	return err
}

func (r *MapV2Reader) SeekStride(stride int) error {
	if stride ==0 {
		if r.present != nil {
			if err := r.present.Seek(0, 0, 0, 0); err != nil {
				return err
			}
		}
		if err := r.length.Seek(0, 0, 0); err != nil {
			return err
		}

		if err := r.key.SeekStride(0); err != nil {
			return err
		}
		if err := r.value.SeekStride(0); err != nil {
			return err
		}
		return nil
	}

	var lengthChunk, lengthChunkOffset, lengthOffset uint64

	pos, err := r.getStridePositions(stride)
	if err != nil {
		return err
	}

	if r.present != nil {
		var pChunk, pChunkOffset, pOffset1, pOffset2 uint64
		if r.opts.CompressionKind == pb.CompressionKind_NONE {
			pChunkOffset = pos[0]
			pOffset1 = pos[1]
			pOffset2 = pos[2]

			lengthChunkOffset = pos[3]
			lengthOffset = pos[4]

		} else {
			pChunk = pos[0]
			pChunkOffset = pos[1]
			pOffset1 = pos[2]
			pOffset2 = pos[3]

			lengthChunk = pos[4]
			lengthChunkOffset = pos[5]
			lengthOffset = pos[6]
		}

		if err = r.present.Seek(pChunk, pChunkOffset, pOffset1, pOffset2); err != nil {
			return err
		}

	}else {

		if r.opts.CompressionKind == pb.CompressionKind_NONE {
			lengthChunkOffset = pos[0]
			lengthOffset = pos[1]

		} else {
			lengthChunk = pos[0]
			lengthChunkOffset = pos[1]
			lengthOffset = pos[2]
		}
	}

	if err = r.length.Seek(lengthChunk, lengthChunkOffset, lengthOffset); err != nil {
		return err
	}

	if err = r.key.SeekStride(stride); err != nil {
		return err
	}
	if err = r.value.SeekStride(stride); err != nil {
		return err
	}
	return nil
}

func (r *MapV2Reader) Close() {
	if r.present != nil {
		r.present.Close()
	}
	r.key.Close()
	r.value.Close()
}
