package hadoop

import "io"

type Writable interface {
	Write(out io.Writer) error
	ReadFields(in io.Reader) error
}
