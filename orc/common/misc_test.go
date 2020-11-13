package common

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEncHeader(t *testing.T)  {
	l:= 100_000
	result:= []byte{0x40, 0x0d, 0x03}
	enc:= encChunkHeader(l, false)
	assert.Equal(t, result, enc)

	r, org:= DecChunkHeader(enc)
	assert.Equal(t, l, r)
	assert.Equal(t, false, org)
}

