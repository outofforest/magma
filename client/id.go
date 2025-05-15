package client

import (
	"crypto/rand"
	"reflect"
	"unsafe"

	"github.com/samber/lo"

	"github.com/outofforest/magma/client/wire"
	"github.com/outofforest/magma/types"
)

const (
	idLength       = 16
	revisionLength = 8
	idIndexName    = "id"
)

type idConstraint interface {
	~[idLength]byte // In go it's not possible to constraint on ID, so this is the best we can do.
}

// NewID generates new ID.
func NewID[T idConstraint]() T {
	var id [idLength]byte
	lo.Must(rand.Read(id[:]))
	return id
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

func setIDInEntity(eValue reflect.Value, id *types.ID) {
	copy(unsafeIDFromEntity(eValue), unsafe.Slice((*byte)(unsafe.Pointer(id)), idLength))
}

func unsafeIDFromEntity(eValue reflect.Value) []byte {
	return unsafe.Slice((*byte)(eValue.UnsafePointer()), idLength)
}

func setRevisionInEntity(eValue reflect.Value, revision *types.Revision) {
	copy(
		unsafe.Slice((*byte)(unsafe.Pointer(uintptr(eValue.UnsafePointer())+idLength)), revisionLength),
		unsafe.Slice((*byte)(unsafe.Pointer(revision)), revisionLength),
	)
}

func revisionFromEntity(eValue reflect.Value) types.Revision {
	return *(*types.Revision)(unsafe.Pointer(uintptr(eValue.UnsafePointer()) + idLength))
}

func copyMetaFromEntity(meta *wire.EntityMetadata, eValue reflect.Value) {
	copy(
		unsafe.Slice((*byte)(unsafe.Pointer(meta)), idLength+revisionLength),
		unsafe.Slice((*byte)(eValue.UnsafePointer()), idLength+revisionLength),
	)
}

func copyMetaToEntity(eValue reflect.Value, meta *wire.EntityMetadata) {
	copy(
		unsafe.Slice((*byte)(eValue.UnsafePointer()), idLength+revisionLength),
		unsafe.Slice((*byte)(unsafe.Pointer(meta)), idLength+revisionLength),
	)
}
