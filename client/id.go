package client

import (
	"reflect"
	"unsafe"

	"github.com/google/uuid"

	"github.com/outofforest/magma/types"
)

const (
	idLength    = 16
	idIndexName = "id"
)

type idConstraint interface {
	~[16]byte // In go it's not possible to constraint on ID, so this is the best we can do.
}

// NewID generates new ID.
func NewID[T idConstraint]() T {
	return T(uuid.New())
}

type idIndexer struct{}

func (idi idIndexer) FromArgs(args ...any) ([]byte, error) {
	id := reflect.ValueOf(args[0]).Convert(idType).Interface().(types.ID)
	return id[:], nil
}

func (idi idIndexer) FromObject(o any) (bool, []byte, error) {
	b2 := make([]byte, idLength)
	copy(b2, unsafeIDFromEntity(o.(reflect.Value)))
	return true, b2, nil
}

func setIDInEntity(id *types.ID, eValue reflect.Value) {
	copy(unsafeIDFromEntity(eValue), unsafe.Slice((*byte)(unsafe.Pointer(id)), idLength))
}

func unsafeIDFromEntity(eValue reflect.Value) []byte {
	return unsafe.Slice((*byte)(eValue.UnsafePointer()), idLength)
}
