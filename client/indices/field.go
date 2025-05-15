package indices

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"time"
	"unsafe"

	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
)

// NewFieldIndex defines new field index.
func NewFieldIndex(name string, ePtr, fieldPtr any) *FieldIndex {
	ePtrType := reflect.TypeOf(ePtr)
	if ePtrType.Kind() != reflect.Ptr {
		panic(errors.New("ePtr is not a pointer"))
	}
	if ePtrType.Elem().Kind() != reflect.Struct {
		panic(errors.New("*ePtr is not a struct"))
	}

	fieldPtrType := reflect.TypeOf(fieldPtr)
	if fieldPtrType.Kind() != reflect.Ptr {
		panic(errors.New("fieldPtr is not a pointer"))
	}
	if fieldPtrType.Elem().Kind() == reflect.Ptr {
		panic(errors.New("field is a pointer"))
	}

	eStart := reflect.ValueOf(ePtr).Pointer()
	eSize := ePtrType.Elem().Size()
	fieldStart := reflect.ValueOf(fieldPtr).Pointer()
	if fieldStart < eStart || fieldStart >= eStart+eSize {
		panic(errors.Errorf("field does not belong to entity"))
	}

	offset := fieldStart - eStart
	fieldType := findField(ePtrType.Elem(), offset)
	indexer, err := indexerForType(fieldType, offset)
	if err != nil {
		panic(err)
	}
	if fieldType != fieldPtrType.Elem() {
		panic(errors.Errorf("unexpected field type %s, expected %s", fieldType, fieldPtrType.Elem()))
	}

	return &FieldIndex{
		name:       name,
		entityType: ePtrType.Elem(),
		indexer:    indexer,
	}
}

// FieldIndex defines index indexing entities by struct field.
type FieldIndex struct {
	name       string
	entityType reflect.Type
	indexer    memdb.Indexer
}

// Name returns name of the index.
func (i *FieldIndex) Name() string {
	return i.name
}

// Type returns type of entity index is defined for.
func (i *FieldIndex) Type() reflect.Type {
	return i.entityType
}

// NumOfArgs returns number of arguments taken by the index.
func (i *FieldIndex) NumOfArgs() uint64 {
	return 1
}

// Schema returns memdb index schema.
func (i *FieldIndex) Schema() *memdb.IndexSchema {
	return &memdb.IndexSchema{
		Name:    i.name,
		Indexer: i.indexer,
	}
}

func findField(t reflect.Type, offset uintptr) reflect.Type {
	var field reflect.StructField
	for {
		for i := range t.NumField() {
			f := t.Field(i)
			if f.Offset > offset {
				break
			}
			field = f
		}

		if field.Type.Kind() != reflect.Struct || field.Type.ConvertibleTo(timeType) {
			return field.Type
		}
		offset -= field.Offset
		t = field.Type
	}
}

func valueByOffset[T any](v reflect.Value, offset uintptr) T {
	return *(*T)(unsafe.Pointer(uintptr(v.UnsafePointer()) + offset))
}

func boolToBytes(v bool, b []byte) {
	if v {
		b[0] = 0x01
	}
}

var _ indexer = boolIndexer{}

type boolIndexer struct {
	offset uintptr
}

func (i boolIndexer) FromArgs(args ...any) ([]byte, error) {
	var b [1]byte
	boolToBytes(reflect.ValueOf(args[0]).Bool(), b[:])
	return b[:], nil
}

func (i boolIndexer) FromObject(o any) (bool, []byte, error) {
	var b [1]byte
	i.PutValue(o, b[:])
	return true, b[:], nil
}

func (i boolIndexer) IsSizeConstant() bool {
	return true
}

func (i boolIndexer) Size(o any) uint64 {
	return 1
}

func (i boolIndexer) PutValue(o any, b []byte) uint64 {
	boolToBytes(valueByOffset[bool](o.(reflect.Value), i.offset), b)
	return 1
}

func stringToBytes(s string, b []byte) uint64 {
	return uint64(copy(b, s)) + 1
}

var _ indexer = stringIndexer{}

type stringIndexer struct {
	offset uintptr
}

func (i stringIndexer) FromArgs(args ...any) ([]byte, error) {
	s := reflect.ValueOf(args[0]).String()
	b := make([]byte, len(s)+1)
	stringToBytes(s, b)
	return b, nil
}

func (i stringIndexer) FromObject(o any) (bool, []byte, error) {
	s := valueByOffset[string](o.(reflect.Value), i.offset)
	b := make([]byte, len(s)+1)
	stringToBytes(s, b)
	return true, b, nil
}

func (i stringIndexer) IsSizeConstant() bool {
	return false
}

func (i stringIndexer) Size(o any) uint64 {
	return uint64(len(valueByOffset[string](o.(reflect.Value), i.offset))) + 1
}

func (i stringIndexer) PutValue(o any, b []byte) uint64 {
	return stringToBytes(valueByOffset[string](o.(reflect.Value), i.offset), b)
}

var (
	secondsOffset = time.Time{}.Unix()
	timeType      = reflect.TypeOf(time.Time{})
)

func timeToBytes(t time.Time, b []byte) {
	binary.BigEndian.PutUint64(b, uint64(t.Unix()-secondsOffset)^0x8000000000000000)
	binary.BigEndian.PutUint32(b[8:], uint32(t.Nanosecond()))
}

var _ indexer = timeIndexer{}

type timeIndexer struct {
	offset uintptr
}

func (i timeIndexer) FromArgs(args ...any) ([]byte, error) {
	var b [12]byte
	timeToBytes(reflect.ValueOf(args[0]).Convert(timeType).Interface().(time.Time), b[:])
	return b[:], nil
}

func (i timeIndexer) FromObject(o any) (bool, []byte, error) {
	var b [12]byte
	i.PutValue(o, b[:])
	return true, b[:], nil
}

func (i timeIndexer) IsSizeConstant() bool {
	return true
}

func (i timeIndexer) Size(o any) uint64 {
	return 12
}

func (i timeIndexer) PutValue(o any, b []byte) uint64 {
	timeToBytes(valueByOffset[time.Time](o.(reflect.Value), i.offset), b)
	return 12
}

func int8ToBytes(i int8, b []byte) {
	b[0] = uint8(i) ^ 0x80
}

var _ indexer = int8Indexer{}

type int8Indexer struct {
	offset uintptr
}

func (i int8Indexer) FromArgs(args ...any) ([]byte, error) {
	var b [1]byte
	int8ToBytes(int8(reflect.ValueOf(args[0]).Int()), b[:])
	return b[:], nil
}

func (i int8Indexer) FromObject(o any) (bool, []byte, error) {
	var b [1]byte
	i.PutValue(o, b[:])
	return true, b[:], nil
}

func (i int8Indexer) IsSizeConstant() bool {
	return true
}

func (i int8Indexer) Size(o any) uint64 {
	return 1
}

func (i int8Indexer) PutValue(o any, b []byte) uint64 {
	int8ToBytes(valueByOffset[int8](o.(reflect.Value), i.offset), b)
	return 1
}

func int16ToBytes(i int16, b []byte) {
	binary.BigEndian.PutUint16(b, uint16(i)^0x8000)
}

var _ indexer = int16Indexer{}

type int16Indexer struct {
	offset uintptr
}

func (i int16Indexer) FromArgs(args ...any) ([]byte, error) {
	var b [2]byte
	int16ToBytes(int16(reflect.ValueOf(args[0]).Int()), b[:])
	return b[:], nil
}

func (i int16Indexer) FromObject(o any) (bool, []byte, error) {
	var b [2]byte
	i.PutValue(o, b[:])
	return true, b[:], nil
}

func (i int16Indexer) IsSizeConstant() bool {
	return true
}

func (i int16Indexer) Size(o any) uint64 {
	return 2
}

func (i int16Indexer) PutValue(o any, b []byte) uint64 {
	int16ToBytes(valueByOffset[int16](o.(reflect.Value), i.offset), b)
	return 2
}

var _ indexer = int32Indexer{}

func int32ToBytes(i int32, b []byte) {
	binary.BigEndian.PutUint32(b, uint32(i)^0x80000000)
}

type int32Indexer struct {
	offset uintptr
}

func (i int32Indexer) FromArgs(args ...any) ([]byte, error) {
	var b [4]byte
	int32ToBytes(int32(reflect.ValueOf(args[0]).Int()), b[:])
	return b[:], nil
}

func (i int32Indexer) FromObject(o any) (bool, []byte, error) {
	var b [4]byte
	i.PutValue(o, b[:])
	return true, b[:], nil
}

func (i int32Indexer) IsSizeConstant() bool {
	return true
}

func (i int32Indexer) Size(o any) uint64 {
	return 4
}

func (i int32Indexer) PutValue(o any, b []byte) uint64 {
	int32ToBytes(valueByOffset[int32](o.(reflect.Value), i.offset), b)
	return 4
}

var _ indexer = int64Indexer{}

func int64ToBytes(i int64, b []byte) {
	binary.BigEndian.PutUint64(b, uint64(i)^0x8000000000000000)
}

type int64Indexer struct {
	offset uintptr
}

func (i int64Indexer) FromArgs(args ...any) ([]byte, error) {
	var b [8]byte
	int64ToBytes(reflect.ValueOf(args[0]).Int(), b[:])
	return b[:], nil
}

func (i int64Indexer) FromObject(o any) (bool, []byte, error) {
	var b [8]byte
	i.PutValue(o, b[:])
	return true, b[:], nil
}

func (i int64Indexer) IsSizeConstant() bool {
	return true
}

func (i int64Indexer) Size(o any) uint64 {
	return 8
}

func (i int64Indexer) PutValue(o any, b []byte) uint64 {
	int64ToBytes(valueByOffset[int64](o.(reflect.Value), i.offset), b)
	return 8
}

func uint8ToBytes(i uint8, b []byte) {
	b[0] = i
}

var _ indexer = uint8Indexer{}

type uint8Indexer struct {
	offset uintptr
}

func (i uint8Indexer) FromArgs(args ...any) ([]byte, error) {
	var b [1]byte
	uint8ToBytes(uint8(reflect.ValueOf(args[0]).Uint()), b[:])
	return b[:], nil
}

func (i uint8Indexer) FromObject(o any) (bool, []byte, error) {
	var b [1]byte
	i.PutValue(o, b[:])
	return true, b[:], nil
}

func (i uint8Indexer) IsSizeConstant() bool {
	return true
}

func (i uint8Indexer) Size(o any) uint64 {
	return 1
}

func (i uint8Indexer) PutValue(o any, b []byte) uint64 {
	uint8ToBytes(valueByOffset[uint8](o.(reflect.Value), i.offset), b)
	return 1
}

func uint16ToBytes(i uint16, b []byte) {
	binary.BigEndian.PutUint16(b, i)
}

var _ indexer = uint16Indexer{}

type uint16Indexer struct {
	offset uintptr
}

func (i uint16Indexer) FromArgs(args ...any) ([]byte, error) {
	var b [2]byte
	uint16ToBytes(uint16(reflect.ValueOf(args[0]).Uint()), b[:])
	return b[:], nil
}

func (i uint16Indexer) FromObject(o any) (bool, []byte, error) {
	var b [2]byte
	i.PutValue(o, b[:])
	return true, b[:], nil
}

func (i uint16Indexer) IsSizeConstant() bool {
	return true
}

func (i uint16Indexer) Size(o any) uint64 {
	return 2
}

func (i uint16Indexer) PutValue(o any, b []byte) uint64 {
	uint16ToBytes(valueByOffset[uint16](o.(reflect.Value), i.offset), b)
	return 2
}

func uint32ToBytes(i uint32, b []byte) {
	binary.BigEndian.PutUint32(b, i)
}

var _ indexer = uint32Indexer{}

type uint32Indexer struct {
	offset uintptr
}

func (i uint32Indexer) FromArgs(args ...any) ([]byte, error) {
	var b [4]byte
	uint32ToBytes(uint32(reflect.ValueOf(args[0]).Uint()), b[:])
	return b[:], nil
}

func (i uint32Indexer) FromObject(o any) (bool, []byte, error) {
	var b [4]byte
	i.PutValue(o, b[:])
	return true, b[:], nil
}

func (i uint32Indexer) IsSizeConstant() bool {
	return true
}

func (i uint32Indexer) Size(o any) uint64 {
	return 4
}

func (i uint32Indexer) PutValue(o any, b []byte) uint64 {
	uint32ToBytes(valueByOffset[uint32](o.(reflect.Value), i.offset), b)
	return 4
}

func uint64ToBytes(i uint64, b []byte) {
	binary.BigEndian.PutUint64(b, i)
}

var _ indexer = uint64Indexer{}

type uint64Indexer struct {
	offset uintptr
}

func (i uint64Indexer) FromArgs(args ...any) ([]byte, error) {
	var b [8]byte
	uint64ToBytes(reflect.ValueOf(args[0]).Uint(), b[:])
	return b[:], nil
}

func (i uint64Indexer) FromObject(o any) (bool, []byte, error) {
	var b [8]byte
	i.PutValue(o, b[:])
	return true, b[:], nil
}

func (i uint64Indexer) IsSizeConstant() bool {
	return true
}

func (i uint64Indexer) Size(o any) uint64 {
	return 8
}

func (i uint64Indexer) PutValue(o any, b []byte) uint64 {
	uint64ToBytes(valueByOffset[uint64](o.(reflect.Value), i.offset), b)
	return 8
}

func indexerForType(t reflect.Type, offset uintptr) (indexer, error) {
	if t.ConvertibleTo(timeType) {
		return timeIndexer{offset: offset}, nil
	}
	switch t.Kind() {
	case reflect.Bool:
		return boolIndexer{offset: offset}, nil
	case reflect.String:
		return stringIndexer{offset: offset}, nil
	case reflect.Int8:
		return int8Indexer{offset: offset}, nil
	case reflect.Int16:
		return int16Indexer{offset: offset}, nil
	case reflect.Int32:
		return int32Indexer{offset: offset}, nil
	case reflect.Int64:
		return int64Indexer{offset: offset}, nil
	case reflect.Uint8:
		return uint8Indexer{offset: offset}, nil
	case reflect.Uint16:
		return uint16Indexer{offset: offset}, nil
	case reflect.Uint32:
		return uint32Indexer{offset: offset}, nil
	case reflect.Uint64:
		return uint64Indexer{offset: offset}, nil
	default:
		return nil, fmt.Errorf("unsupported type: %s", t)
	}
}
