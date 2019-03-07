package orc

/*const (
	NONE CompressionKind = iota
	ZLIB
	SNAPPY
	LZO
	LZ4
)

type CompressionKind int

func (k CompressionKind) String() string {
	switch k {
	case NONE:
		return "NONE"
	case ZLIB:
		return "ZLIB"
	case SNAPPY:
		return "SNAPPY"
	case LZO:
		return "LZO"
	case LZ4:
		return "LZ4"
	default:
		return "UKNOWN"
	}
}
type CompressionCodec interface {
}

func NewCodec(kind CompressionKind) (cc CompressionCodec, err error) {
	switch kind {
	case NONE:
	case ZLIB:
	// todo:
	case SNAPPY:
	// todo:
	case LZO:
	// todo:
	case LZ4:
		// todo:
	default:
		return nil, errors.Errorf("unknown compression codec ")
	}
	return cc, err
}
*/