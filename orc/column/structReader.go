package column

import (
	"github.com/patrickhuang888/goorc/orc/stream"
	"github.com/pkg/errors"
	"github.com/patrickhuang888/goorc/orc"
	"github.com/patrickhuang888/goorc/pb/pb"
)

type structReader struct {
	*reader

	children []Reader
}

func NewStructReader(schema *orc.TypeDescription, opts *orc.ReaderOptions, path string, numberOfRows uint64) Reader {
	return &structReader{reader: &reader{opts: opts, schema: schema, path: path, numberOfRows: numberOfRows}}
}

func (s *structReader) InitChildren(children []Reader) error {
	s.children = children
	return nil
}

func (s structReader) Children() []Reader {
	return s.children
}

func (s *structReader) InitStream(kind pb.Stream_Kind, encoding pb.ColumnEncoding_Kind, startOffset uint64, info *pb.Stream, path string) error {
	if kind == pb.Stream_PRESENT {
		var err error
		s.present, err = stream.NewBoolReader(s.opts, info, startOffset, path)
		return err
	}

	return errors.New("struct column no stream other than present")
}

func (s *structReader) Next(presents *[]bool, pFromParent bool, vec *interface{}) (rows int, err error) {
	pp := *presents

	if !pFromParent {
		if err = s.nextPresents(&pp); err != nil {
			return
		}
		*presents = pp
	}

	vector := (*vec).([]*orc.ColumnVector)

	var rt int
	for i, child := range s.children {
		if !pFromParent && len(pp) != 0 {
			if rt, err = child.Next(&pp, true, &vector[i].Vector); err != nil {
				return
			}
		} else {
			if rt, err = child.Next(&pp, pFromParent, &vector[i].Vector); err != nil {
				return
			}
		}
	}

	// reassure: no present, so readrows using children readrows?
	if len(pp) == 0 {
		rows = rt
	} else {
		rows = len(pp)
	}

	s.cursor += uint64(rows)

	return
}

func (s *structReader) Seek(rowNumber uint64) error {
	//todo: seek present

	for _, child := range s.children {
		if err := child.Seek(rowNumber); err != nil {
			return err
		}
	}
	return nil
}

func (s structReader) Close() error {
	return s.present.Close()
}
