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

func boolToBytes(v bool) []byte {
	if v {
		return []byte{1}
	}
	return []byte{0}
}

type boolIndexer struct {
	offset uintptr
}

func (i boolIndexer) FromArgs(args ...any) ([]byte, error) {
	return boolToBytes(reflect.ValueOf(args[0]).Bool()), nil
}

func (i boolIndexer) FromObject(o any) (bool, []byte, error) {
	return true, boolToBytes(valueByOffset[bool](o.(reflect.Value), i.offset)), nil
}

func stringToBytes(s string) []byte {
	b := make([]byte, len(s)+1) // +1 for null termination
	copy(b, s)
	return b
}

type stringIndexer struct {
	offset uintptr
}

func (i stringIndexer) FromArgs(args ...any) ([]byte, error) {
	return stringToBytes(reflect.ValueOf(args[0]).String()), nil
}

func (i stringIndexer) FromObject(o any) (bool, []byte, error) {
	return true, stringToBytes(valueByOffset[string](o.(reflect.Value), i.offset)), nil
}

var (
	secondsOffset = time.Time{}.Unix()
	timeType      = reflect.TypeOf(time.Time{})
)

func timeToBytes(t time.Time) []byte {
	var b [12]byte
	binary.BigEndian.PutUint64(b[:], uint64(t.Unix()-secondsOffset))
	binary.BigEndian.PutUint32(b[8:], uint32(t.Nanosecond()))
	b[0] ^= 0x80
	return b[:]
}

type timeIndexer struct {
	offset uintptr
}

func (i timeIndexer) FromArgs(args ...any) ([]byte, error) {
	return timeToBytes(reflect.ValueOf(args[0]).Convert(timeType).Interface().(time.Time)), nil
}

func (i timeIndexer) FromObject(o any) (bool, []byte, error) {
	return true, timeToBytes(valueByOffset[time.Time](o.(reflect.Value), i.offset)), nil
}

func int8ToBytes(i int8) []byte {
	return []byte{uint8(i) ^ 0x80}
}

type int8Indexer struct {
	offset uintptr
}

func (i int8Indexer) FromArgs(args ...any) ([]byte, error) {
	return int8ToBytes(int8(reflect.ValueOf(args[0]).Int())), nil
}

func (i int8Indexer) FromObject(o any) (bool, []byte, error) {
	return true, int8ToBytes(valueByOffset[int8](o.(reflect.Value), i.offset)), nil
}

func int16ToBytes(i int16) []byte {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], uint16(i)^0x8000)
	return b[:]
}

type int16Indexer struct {
	offset uintptr
}

func (i int16Indexer) FromArgs(args ...any) ([]byte, error) {
	return int16ToBytes(int16(reflect.ValueOf(args[0]).Int())), nil
}

func (i int16Indexer) FromObject(o any) (bool, []byte, error) {
	return true, int16ToBytes(valueByOffset[int16](o.(reflect.Value), i.offset)), nil
}

func int32ToBytes(i int32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(i)^0x80000000)
	return b[:]
}

type int32Indexer struct {
	offset uintptr
}

func (i int32Indexer) FromArgs(args ...any) ([]byte, error) {
	return int32ToBytes(int32(reflect.ValueOf(args[0]).Int())), nil
}

func (i int32Indexer) FromObject(o any) (bool, []byte, error) {
	return true, int32ToBytes(valueByOffset[int32](o.(reflect.Value), i.offset)), nil
}

func int64ToBytes(i int64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(i)^0x8000000000000000)
	return b[:]
}

type int64Indexer struct {
	offset uintptr
}

func (i int64Indexer) FromArgs(args ...any) ([]byte, error) {
	return int64ToBytes(reflect.ValueOf(args[0]).Int()), nil
}

func (i int64Indexer) FromObject(o any) (bool, []byte, error) {
	return true, int64ToBytes(valueByOffset[int64](o.(reflect.Value), i.offset)), nil
}

func uint8ToBytes(i uint8) []byte {
	return []byte{i}
}

type uint8Indexer struct {
	offset uintptr
}

func (i uint8Indexer) FromArgs(args ...any) ([]byte, error) {
	return uint8ToBytes(uint8(reflect.ValueOf(args[0]).Uint())), nil
}

func (i uint8Indexer) FromObject(o any) (bool, []byte, error) {
	return true, uint8ToBytes(valueByOffset[uint8](o.(reflect.Value), i.offset)), nil
}

func uint16ToBytes(i uint16) []byte {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], i)
	return b[:]
}

type uint16Indexer struct {
	offset uintptr
}

func (i uint16Indexer) FromArgs(args ...any) ([]byte, error) {
	return uint16ToBytes(uint16(reflect.ValueOf(args[0]).Uint())), nil
}

func (i uint16Indexer) FromObject(o any) (bool, []byte, error) {
	return true, uint16ToBytes(valueByOffset[uint16](o.(reflect.Value), i.offset)), nil
}

func uint32ToBytes(i uint32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], i)
	return b[:]
}

type uint32Indexer struct {
	offset uintptr
}

func (i uint32Indexer) FromArgs(args ...any) ([]byte, error) {
	return uint32ToBytes(uint32(reflect.ValueOf(args[0]).Uint())), nil
}

func (i uint32Indexer) FromObject(o any) (bool, []byte, error) {
	return true, uint32ToBytes(valueByOffset[uint32](o.(reflect.Value), i.offset)), nil
}

func uint64ToBytes(i uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], i)
	return b[:]
}

type uint64Indexer struct {
	offset uintptr
}

func (i uint64Indexer) FromArgs(args ...any) ([]byte, error) {
	return uint64ToBytes(reflect.ValueOf(args[0]).Uint()), nil
}

func (i uint64Indexer) FromObject(o any) (bool, []byte, error) {
	return true, uint64ToBytes(valueByOffset[uint64](o.(reflect.Value), i.offset)), nil
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
