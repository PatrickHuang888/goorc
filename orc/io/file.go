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
}

type fileFile struct {
	f *os.File
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

func Open(opts *config.ReaderOptions, path string) (in File, err error) {
	if opts.MockTest {

	}

	var f *os.File
	if f, err = os.Open(path); err != nil {
		return nil, errors.WithStack(err)
	}
	return OpenOsFile(f), nil
}
