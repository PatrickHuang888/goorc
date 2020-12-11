package encoding

import (
	"bytes"
	"encoding/binary"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
)

var logger= log.New()

func SetLogLevel(level log.Level)  {
	logger.SetLevel(level)
}

const (
	Encoding_SHORT_REPEAT = byte(0)
	Encoding_DIRECT       = byte(1)
	Encoding_PATCHED_BASE = byte(2)
	Encoding_DELTA        = byte(3)
	Encoding_UNSET        = byte(255)

	BITS_SLOTS = 65
	MAX_SCOPE  = 512

	BYTES = 8
)

type Encoder interface {
	// Encode may returns no data due to buffer and algorithm need more v to encoding
	Encode(v interface{}, out *bytes.Buffer) error

	// BufferedSize get encoded data sized buffered, not including no encoded data
	//BufferedSize() int

	// Flush flush remaining data, make sure there is only one position mark in one flush
	// data should be used immediately, it's from encoder's buffer, maybe changed next encode
	// encoder reset after flush
	Flush(out *bytes.Buffer) error

	ResetPosition()

	// GetPositions get current position count
	GetPosition() []uint64

	Reset()
}

type BufferedReader interface {
	io.ByteReader
	io.Reader
}

/*type intRunLengthV1 struct {
	signed      bool
	numLiterals int
	literals    []int64
	uliterals   []uint64
}

func (irl *intRunLengthV1) readValues(in *bytes.Buffer) error {
	for in.Len() > 0 {
		control, err := in.ReadByte()
		if err != nil {
			return errors.WithStack(err)
		}
		mark := irl.numLiterals
		if control < 0x80 { // run
			num := int(control) + MIN_REPEAT_SIZE
			irl.numLiterals += num
			if irl.signed {
				ls := make([]int64, irl.numLiterals)
				copy(ls, irl.literals)
				irl.literals = ls
			} else {
				ls := make([]uint64, irl.numLiterals)
				copy(ls, irl.uliterals)
				irl.uliterals = ls
			}
			// delta
			d, err := in.ReadByte()
			if err != nil {
				return errors.WithStack(err)
			}
			delta := int64(int8(d))
			if irl.signed {
				v, err := binary.ReadVarint(in)
				if err != nil {
					return errors.WithStack(err)
				}
				for i := 0; i < num; i++ {
					irl.literals[mark+i] = v + delta
				}
			} else {
				v, err := binary.ReadUvarint(in)
				if err != nil {
					return errors.WithStack(err)
				}
				for i := 0; i < num; i++ {
					if delta > 0 {
						irl.uliterals[mark+i] = v + uint64(delta)
					} else {
						irl.uliterals[mark+i] = v - uint64(-delta)
					}
				}
			}

		} else {
			num := -int(int8(control))
			irl.numLiterals += num
			if irl.signed {
				ls := make([]int64, irl.numLiterals)
				copy(ls, irl.literals)
				irl.literals = ls
			} else {
				ls := make([]uint64, irl.numLiterals)
				copy(ls, irl.uliterals)
				irl.uliterals = ls
			}
			if irl.signed {
				for i := 0; i < num; i++ {
					v, err := binary.ReadVarint(in)
					if err != nil {
						return errors.WithStack(err)
					}
					irl.literals[mark+i] = v
				}
			} else {
				for i := 0; i < num; i++ {
					v, err := binary.ReadUvarint(in)
					if err != nil {
						return errors.WithStack(err)
					}
					irl.uliterals[mark+i] = v
				}
			}
		}
	}
	return nil
}

func (irl *intRunLengthV1) reset() {
	irl.numLiterals = 0
	irl.signed = false
}

func (irl *intRunLengthV1) Encode(out *bytes.Buffer) error {
	if irl.numLiterals != 0 {

	}
	return nil
}*/


// string contents, decoding need length decoder
type BytesContent struct {
}

func (e *BytesContent) MarkPosition() {
	//
}

func (e *BytesContent) GetAndClearPositions() []uint64 {
	return nil
}

func (e *BytesContent) Reset() {
	//
}

func (e *BytesContent) Encode(v interface{}) (data []byte, err error) {
	data = v.([]byte)
	return
}

func (e BytesContent) Flush() (data []byte, err error) {
	//
	return
}

// write out content do not base length field, just base on len of content
func encodeBytesContent(out *bytes.Buffer, values [][]byte) error {
	for _, v := range values {
		if _, err := out.Write(v); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

type varInt64 struct {
}

func (e *varInt64) MarkPosition() {
	//
}

func (e *varInt64) GetAndClearPositions() []uint64 {
	return nil
}

func (e *varInt64) Reset() {
	//
}

func (e *varInt64) Encode(v interface{}) (data []byte, err error) {
	value := v.(int64)
	bb := make([]byte, 10)
	c := binary.PutVarint(bb, value)
	return bb[:c], nil
}

func (e varInt64) Flush() (data []byte, err error) {
	return nil, nil
}

func DecodeVarInt64(in BufferedReader) (value int64, err error) {
	value, err = binary.ReadVarint(in)
	return
}

func widthEncoding(width int) (w byte, err error) {
	if 2 <= width && width <= 21 {
		w = byte(width - 1)
		if (3 == width) || (5 <= width && width <= 7) || (9 <= width && width <= 15) || (17 <= width && width <= 21) {
			log.Warnf("encoding: width %d is deprecated", width)
		}
		return
	}
	if 26 == width {
		log.Warnf("encoding: width %d is deprecated", width)
		return 24, nil
	}
	if 28 == width {
		log.Warnf("encoding: width %d is deprecated", width)
		return 25, nil
	}
	if 30 == width {
		log.Warnf("encoding: width %d is deprecated", width)
		return 26, nil
	}

	switch width {
	case 0:
		w = 0
	case 1:
		w = 0
	case 2:
		w = 1
	case 4:
		w = 3
	case 8:
		w = 7
	case 16:
		w = 15
	case 24:
		w = 23
	case 32:
		w = 27
	case 40:
		w = 28
	case 48:
		w = 29
	case 56:
		w = 30
	case 64:
		w = 31
	default:
		// fixme: return 0
		return 0, errors.Errorf("width %d error", width)
	}
	return
}

func widthDecoding(w byte, delta bool) (width byte, err error) {
	if 2 <= w && w <= 23 {
		if 2 == w || (4 <= w && w <= 6) || (8 <= w && w <= 14) || (16 <= w && w <= 22) {
			log.Warnf("decoding: width encoded %d is deprecated", w)
		}
		width = w + 1
		return
	}
	if 24 == w {
		log.Warnf("decoding: width encoded  %d is deprecated", w)
		return 26, nil
	}
	if 25 == w {
		log.Warnf("decoding: width encoded  %d is deprecated", w)
		return 28, nil
	}
	if 26 == w {
		log.Warnf("decoding: width encoded  %d is deprecated", w)
		return 30, nil
	}

	switch w {
	case 0:
		if delta {
			width = 0
		} else {
			width = 1
		}
	case 1:
		width = 2
	case 27:
		width = 32
	case 28:
		width = 40
	case 29:
		width = 48
	case 30:
		width = 56
	case 31:
		width = 64
	default:
		// should not reach
		return 0, errors.Errorf("run length integer v2 width(W) %d error", w)
	}
	return
}

func UnZigzag(x uint64) int64 {
	return int64(x>>1) ^ -int64(x&1)
}

func Zigzag(x int64) uint64 {
	return uint64(x<<1) ^ uint64(x>>63)
}

// base 128 varuint
func ReadVUint(in io.ByteReader) (r uint64, err error) {
	var b byte
	var shift uint
	for {
		b, err = in.ReadByte()
		if err != nil {
			errors.WithStack(err)
		}
		r |= uint64(0x7f&b) << shift
		shift += 7
		if b < 0x80 {
			break
		}
	}
	return
}

func Convert(u uint64) int64 {
	x := int64(u >> 1)
	if u&1 != 0 {
		x = ^x
	}
	return x
}

func EncodingNano(nanos uint64) (encoded uint64) {
	if nanos == 0 {
		return 0
	} else if nanos%100 != 0 {
		return uint64(nanos) << 3 // no encoding if less 2 zeros
	} else {
		nanos /= 100
		trailingZeros := 1
		for nanos%10 == 0 && trailingZeros < 7 { // 3 bits
			nanos /= 10
			trailingZeros++
		}
		return nanos<<3 | uint64(trailingZeros)
	}
}

func DecodingNano(encoded uint64) (nano uint) {
	zeros := 0x07 & encoded
	nano = uint(encoded >> 3)
	if zeros != 0 {
		for i := 0; i <= int(zeros); i++ {
			nano *= 10
		}
	}
	return
}
