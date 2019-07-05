package orc

import (
	"github.com/PatrickHuang888/goorc/pb/pb"
	"time"
)

const (
	DEFAULT_ROW_SIZE = 1024
)

type ColumnVector interface {
	T() pb.Type_Kind
	ColumnId() uint32
	Rows() int
	reset()
}

type column struct {
	id uint32
}

func (c column) ColumnId() uint32 {
	return c.id
}

type BoolColumn struct {
	column
	vector []bool
}

func (*BoolColumn) T() pb.Type_Kind {
	return pb.Type_BOOLEAN
}
func (bc *BoolColumn) reset() {
	bc.vector = bc.vector[:0]
}
func (bc *BoolColumn) SetVector(vector []bool) {
	bc.vector = vector
}
func (bc *BoolColumn) GetVector() []bool {
	return bc.vector
}

type TinyIntColumn struct {
	column
	vector []byte
}

func (*TinyIntColumn) T() pb.Type_Kind {
	return pb.Type_BYTE
}
func (tic *TinyIntColumn) reset() {
	tic.vector = tic.vector[:0]
}
func (tic *TinyIntColumn) Rows() int {
	return len(tic.vector)
}
func (tic *TinyIntColumn) SetVector(vector []byte) {
	tic.vector = vector
}
func (tic *TinyIntColumn) GetVector() []byte {
	return tic.vector
}

type SmallIntColumn struct {
	column
	vector []int16
}

func (*SmallIntColumn) T() pb.Type_Kind {
	return pb.Type_SHORT
}
func (sic *SmallIntColumn) reset() {
	sic.vector = sic.vector[:0]
}
func (sic *SmallIntColumn) Rows() int {
	return len(sic.vector)
}
func (sic *SmallIntColumn) SetVector(vector []int16) {
	sic.vector = vector
}
func (sic *SmallIntColumn) GetVector() []int16 {
	return sic.vector
}

type IntColumn struct {
	column
	vector []int32
}

func (*IntColumn) T() pb.Type_Kind {
	return pb.Type_INT
}
func (ic *IntColumn) reset() {
	ic.vector = ic.vector[:0]
}
func (ic *IntColumn) Rows() int {
	return len(ic.vector)
}
func (ic *IntColumn) SetVector(vector []int32) {
	ic.vector = vector
}
func (ic *IntColumn) GetVector() []int32 {
	return ic.vector
}

// nullable int column vector for all integer types
type BigIntColumn struct {
	column
	vector []int64
}

func (*BigIntColumn) T() pb.Type_Kind {
	return pb.Type_LONG
}

func (bic *BigIntColumn) GetVector() []int64 {
	return bic.vector
}

func (bic *BigIntColumn) SetVector(vector []int64) {
	bic.vector = vector
}

func (bic *BigIntColumn) Rows() int {
	return len(bic.vector)
}

func (bic *BigIntColumn) reset() {
	bic.vector = bic.vector[:0]
}

type BinaryColumn struct {
	column
	vector [][]byte
}

func (*BinaryColumn) T() pb.Type_Kind {
	return pb.Type_BINARY
}
func (bc *BinaryColumn) reset() {
	bc.vector = bc.vector[:0]
}
func (bc *BinaryColumn) Rows() int {
	return len(bc.vector)
}
func (bc *BinaryColumn) SetVector(vector [][]byte) {
	bc.vector = vector
}
func (bc *BinaryColumn) GetVector() [][]byte {
	return bc.vector
}

type DecimalColumn struct {
	column
	vector [][16]byte  // 38 digits, 128 bits
}

type Date uint64
type DateColumn struct {
	column
	vector []Date
}

type Timestamp struct {
	Date Date
	Nanos time.Duration
}
type TimestampColumn struct {
	column
	vector []Timestamp
}

func (*TimestampColumn) T() pb.Type_Kind {
	return pb.Type_TIMESTAMP
}

type FloatColumn struct {
	column
	vector []float32
}

func (*FloatColumn) T() pb.Type_Kind {
	return pb.Type_FLOAT
}
func (fc *FloatColumn) reset() {
	fc.vector = fc.vector[:0]
}
func (fc *FloatColumn) Rows() int {
	return len(fc.vector)
}
func (fc *FloatColumn) SetVector(vector []float32) {
	fc.vector = vector
}
func (fc *FloatColumn) GetVector() []float32 {
	return fc.vector
}

type DoubleColumn struct {
	column
	vector []float64
}

func (*DoubleColumn) T() pb.Type_Kind {
	return pb.Type_DOUBLE
}
func (dc *DoubleColumn) reset() {
	dc.vector = dc.vector[:0]
}
func (dc *DoubleColumn) Rows() int {
	return len(dc.vector)
}
func (dc *DoubleColumn) SetVector(vector []float64) {
	dc.vector = vector
}
func (dc *DoubleColumn) GetVector() []float64 {
	return dc.vector
}

type StringColumn struct {
	column
	vector []string
}

func (sc *StringColumn) GetVector() []string {
	return sc.vector
}

func (sc *StringColumn) SetVector(v []string) {
	sc.vector = v
}

func (sc *StringColumn) Rows() int {
	return len(sc.vector)
}

func (*StringColumn) T() pb.Type_Kind {
	return pb.Type_STRING
}

func (sc *StringColumn) reset() {
	sc.vector = sc.vector[:0]
}

type StructColumn struct {
	column
	fields []ColumnVector
}

func (sc *StructColumn) AddFields(subColumn ColumnVector)  {
	sc.fields= append(sc.fields, subColumn)
}

func (sc *StructColumn) GetFields() []ColumnVector {
	return sc.fields
}

func (*StructColumn) T() pb.Type_Kind {
	return pb.Type_STRUCT
}

func (sc *StructColumn) Rows() int {
	// toReAssure: impl with arbitrary column
	return sc.fields[0].Rows()
}

func (sc *StructColumn) reset() {
	for _, c := range sc.fields {
		c.reset()
	}
}

type ListColumn struct {
	column
	vector ColumnVector
}

func (*ListColumn) T()pb.Type_Kind  {
	return pb.Type_LIST
}

type MapColumn struct {
	column
	vector map[ColumnVector]ColumnVector
}

func (*MapColumn)T()pb.Type_Kind  {
	return pb.Type_MAP
}

type UnionColumn struct {
	column
	tags []int
	fields []ColumnVector
}

func (*UnionColumn) T()pb.Type_Kind  {
	return pb.Type_UNION
}