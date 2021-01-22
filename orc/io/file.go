package io

import (
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
)

var logger = log.New()

func SetLogLevel(level log.Level) {
	logger.SetLevel(level)
}

type File interface {
	io.ReadSeeker
	io.Closer
	Write(p []byte) (n int, err error)
	Size() (int64, error)
	Clone() (File, error)
}

type osFile struct {
	path string
	f    *os.File
}

func OpenFileForRead(path string) (File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &osFile{f: f, path: path}, nil
}

// should not exist, because cannot APPEND
func OpenFileForWrite(path string) (File, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &osFile{f: f, path: path}, nil
}

func (f *osFile) Write(p []byte) (n int, err error) {
	return f.f.Write(p)
}

func (f *osFile) Clone() (File, error) {
	r, err := os.Open(f.path)
	if err != nil {
		return nil, err
	}
	return &osFile{f.path, r}, nil
}

func (f *osFile) Size() (int64, error) {
	fi, err := f.f.Stat()
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return fi.Size(), nil
}

func (f *osFile) Read(p []byte) (int, error) {
	n, err := f.f.Read(p)
	if err != nil {
		return n, errors.WithStack(err)
	}
	return n, nil
}

func (f *osFile) Seek(offset int64, whence int) (int64, error) {
	return f.f.Seek(offset, whence)
}

func (f *osFile) Close() error {
	if err := f.f.Close(); err != nil {
		return errors.WithStack(err)
	}
	return nil
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
	return &MockFile{buf: m.buf, offset: m.offset, waterMark: m.waterMark}, nil
}

func (m *MockFile) Close() error {
	/*m.offset = 0
	m.waterMark = 0*/
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
	log.Tracef("mock file seek to %d", offset)
	return offset, nil
}

func (m *MockFile) Read(p []byte) (n int, err error) {
	n = copy(p, m.buf[m.offset:m.waterMark])
	m.offset += n
	if m.offset == len(m.buf)-1 || n == 0 {
		err = io.EOF
	}
	logger.Tracef("mock file read %d, offset %d, watermark %d", n, m.offset, m.waterMark)
	return
}

func (m *MockFile) Write(p []byte) (n int, err error) {
	n = copy(m.buf[m.waterMark:], p)
	m.waterMark += n

	if n < len(p) {
		err = errors.New("no enough buf in mock file")
	}

	log.Tracef("mock file write %d, offset %d, watermakr %d", n, m.offset, m.waterMark)
	return
}
