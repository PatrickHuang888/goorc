package io

import (
	"github.com/patrickhuang888/goorc/orc/config"
	"github.com/pkg/errors"
	"io"
	"os"
)

type File interface {
	io.ReadSeeker
	io.Closer
	Size() (int64, error)
	Clone() (File, error)
}

type fileFile struct {
	f *os.File
}

func (fr fileFile) Clone() (File, error) {
	panic("implement me")
}

func (fr fileFile) Size() (int64, error) {
	fi, err := fr.f.Stat()
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return fi.Size(), nil
}

func (fr fileFile) Read(p []byte) (n int, err error) {
	n, err = fr.f.Read(p)
	if err != nil {
		return n, errors.WithStack(err)
	}
	return
}

func (fr fileFile) Seek(offset int64, whence int) (int64, error) {
	return fr.f.Seek(offset, whence)
}

func (fr fileFile) Close() error {
	if err := fr.f.Close(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func OpenOsFile(f *os.File) File {
	return &fileFile{f: f}
}

func Open(opts config.ReaderOptions, path string) (in File, err error) {
	var f *os.File
	if f, err = os.Open(path); err != nil {
		return nil, errors.WithStack(err)
	}
	return OpenOsFile(f), nil
}

type MockFile struct {
	buf       []byte
	offset    int
	waterMark int
}

func NewMockFile(bb []byte) *MockFile {
	return &MockFile{buf: bb}
}
func (m *MockFile) Clone() (File, error) {
	return &MockFile{buf: m.buf}, nil
}

func (m *MockFile) Close() error {
	m.offset = 0
	m.waterMark = 0
	return nil
}

func (m *MockFile) Size() (int64, error) {
	return int64(m.waterMark), nil
}

func (m *MockFile) Seek(offset int64, whence int) (int64, error) {
	if whence != io.SeekStart {
		return 0, errors.New("only SeekStart")
	}
	if int(offset) >= len(m.buf) {
		return 0, errors.New("offset invalid")
	}
	m.offset = int(offset)
	return offset, nil
}

func (m *MockFile) Read(p []byte) (n int, err error) {
	n = copy(p, m.buf[m.offset:m.waterMark])
	m.offset += n
	if m.offset == len(m.buf)-1 || n == 0 {
		err = io.EOF
	}
	return
}

func (m *MockFile) Write(p []byte) (n int, err error) {
	n = copy(m.buf[m.waterMark:], p)
	m.waterMark += n

	if n < len(p) {
		err = errors.New("no enough buf in mock file")
	}

	return
}
