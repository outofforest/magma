package index

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
func NewFieldIndex(name string, ePtr, fieldPtr any) (*FieldIndex, error) {
	ePtrType := reflect.TypeOf(ePtr)
	if ePtrType.Kind() != reflect.Ptr {
		return nil, errors.New("ePtr is not a pointer")
	}
	if ePtrType.Elem().Kind() != reflect.Struct {
		return nil, errors.New("*ePtr is not a struct")
	}

	fieldPtrType := reflect.TypeOf(fieldPtr)
	if fieldPtrType.Kind() != reflect.Ptr {
		return nil, errors.New("fieldPtr is not a pointer")
	}
	if fieldPtrType.Elem().Kind() == reflect.Ptr {
		return nil, errors.New("field is a pointer")
	}

	eStart := reflect.ValueOf(ePtr).Pointer()
	eSize := ePtrType.Elem().Size()
	fieldStart := reflect.ValueOf(fieldPtr).Pointer()
	if fieldStart < eStart || fieldStart >= eStart+eSize {
		return nil, errors.Errorf("field does not belong to entity")
	}

	offset := fieldStart - eStart
	fieldType := findField(ePtrType.Elem(), offset)
	indexer, err := indexerForType(fieldType, offset)
	if err != nil {
		return nil, err
	}
	if fieldType != fieldPtrType.Elem() {
		return nil, errors.Errorf("unexpected field type %s, expected %s", fieldType, fieldPtrType.Elem())
	}

	return &FieldIndex{
		name:    name,
		indexer: indexer,
	}, nil
}

// FieldIndex defines index indexing entities by struct field.
type FieldIndex struct {
	name    string
	indexer memdb.Indexer
}

// Name returns name of the index.
func (i *FieldIndex) Name() string {
	return i.name
}

// Index returns memdb index.
func (i *FieldIndex) Index() *memdb.IndexSchema {
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

func valueByOffset[T any](v any, offset uintptr) T {
	v1 := reflect.ValueOf(v)
	v2 := reflect.New(v1.Type())
	v2.Elem().Set(v1)
	return *(*T)(unsafe.Pointer(uintptr(v2.UnsafePointer()) + offset))
}

func boolToBytes(v any, offset uintptr) []byte {
	if valueByOffset[bool](v, offset) {
		return []byte{1}
	}
	return []byte{0}
}

type boolIndexer struct {
	offset uintptr
}

func (i boolIndexer) FromArgs(args ...any) ([]byte, error) {
	return boolToBytes(args[0], i.offset), nil
}

func (i boolIndexer) FromObject(o any) (bool, []byte, error) {
	return true, boolToBytes(o, i.offset), nil
}

func stringToBytes(s any, offset uintptr) []byte {
	return []byte(valueByOffset[string](s, offset) + "\x00")
}

type stringIndexer struct {
	offset uintptr
}

func (i stringIndexer) FromArgs(args ...any) ([]byte, error) {
	return stringToBytes(args[0], i.offset), nil
}

func (i stringIndexer) FromObject(o any) (bool, []byte, error) {
	return true, stringToBytes(o, i.offset), nil
}

var (
	secondsOffset = time.Time{}.Unix()
	timeType      = reflect.TypeOf(time.Time{})
)

func timeToBytes(t any, offset uintptr) []byte {
	tt := valueByOffset[time.Time](t, offset)

	var b [12]byte
	binary.BigEndian.PutUint64(b[:], uint64(tt.Unix()-secondsOffset))
	binary.BigEndian.PutUint32(b[8:], uint32(tt.Nanosecond()))
	b[0] ^= 0x80
	return b[:]
}

type timeIndexer struct {
	offset uintptr
}

func (i timeIndexer) FromArgs(args ...any) ([]byte, error) {
	return timeToBytes(args[0], i.offset), nil
}

func (i timeIndexer) FromObject(o any) (bool, []byte, error) {
	return true, timeToBytes(o, i.offset), nil
}

func int8ToBytes(i any, offset uintptr) []byte {
	return []byte{uint8(valueByOffset[int8](i, offset)) ^ 0x80}
}

type int8Indexer struct {
	offset uintptr
}

func (i int8Indexer) FromArgs(args ...any) ([]byte, error) {
	return int8ToBytes(args[0], i.offset), nil
}

func (i int8Indexer) FromObject(o any) (bool, []byte, error) {
	return true, int8ToBytes(o, i.offset), nil
}

func int16ToBytes(i any, offset uintptr) []byte {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], uint16(valueByOffset[int16](i, offset)))
	b[0] ^= 0x80
	return b[:]
}

type int16Indexer struct {
	offset uintptr
}

func (i int16Indexer) FromArgs(args ...any) ([]byte, error) {
	return int16ToBytes(args[0], i.offset), nil
}

func (i int16Indexer) FromObject(o any) (bool, []byte, error) {
	return true, int16ToBytes(o, i.offset), nil
}

func int32ToBytes(i any, offset uintptr) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(valueByOffset[int32](i, offset)))
	b[0] ^= 0x80
	return b[:]
}

type int32Indexer struct {
	offset uintptr
}

func (i int32Indexer) FromArgs(args ...any) ([]byte, error) {
	return int32ToBytes(args[0], i.offset), nil
}

func (i int32Indexer) FromObject(o any) (bool, []byte, error) {
	return true, int32ToBytes(o, i.offset), nil
}

func int64ToBytes(i any, offset uintptr) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(valueByOffset[int64](i, offset)))
	b[0] ^= 0x80
	return b[:]
}

type int64Indexer struct {
	offset uintptr
}

func (i int64Indexer) FromArgs(args ...any) ([]byte, error) {
	return int64ToBytes(args[0], i.offset), nil
}

func (i int64Indexer) FromObject(o any) (bool, []byte, error) {
	return true, int64ToBytes(o, i.offset), nil
}

func uint8ToBytes(i any, offset uintptr) []byte {
	return []byte{valueByOffset[uint8](i, offset)}
}

type uint8Indexer struct {
	offset uintptr
}

func (i uint8Indexer) FromArgs(args ...any) ([]byte, error) {
	return uint8ToBytes(args[0], i.offset), nil
}

func (i uint8Indexer) FromObject(o any) (bool, []byte, error) {
	return true, uint8ToBytes(o, i.offset), nil
}

func uint16ToBytes(i any, offset uintptr) []byte {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], valueByOffset[uint16](i, offset))
	return b[:]
}

type uint16Indexer struct {
	offset uintptr
}

func (i uint16Indexer) FromArgs(args ...any) ([]byte, error) {
	return uint16ToBytes(args[0], i.offset), nil
}

func (i uint16Indexer) FromObject(o any) (bool, []byte, error) {
	return true, uint16ToBytes(o, i.offset), nil
}

func uint32ToBytes(i any, offset uintptr) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], valueByOffset[uint32](i, offset))
	return b[:]
}

type uint32Indexer struct {
	offset uintptr
}

func (i uint32Indexer) FromArgs(args ...any) ([]byte, error) {
	return uint32ToBytes(args[0], i.offset), nil
}

func (i uint32Indexer) FromObject(o any) (bool, []byte, error) {
	return true, uint32ToBytes(o, i.offset), nil
}

func uint64ToBytes(i any, offset uintptr) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], valueByOffset[uint64](i, offset))
	return b[:]
}

type uint64Indexer struct {
	offset uintptr
}

func (i uint64Indexer) FromArgs(args ...any) ([]byte, error) {
	return uint64ToBytes(args[0], i.offset), nil
}

func (i uint64Indexer) FromObject(o any) (bool, []byte, error) {
	return true, uint64ToBytes(o, i.offset), nil
}

func indexerForType(t reflect.Type, offset uintptr) (memdb.Indexer, error) {
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
