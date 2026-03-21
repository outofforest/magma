package client

import (
	"unsafe"

	"github.com/outofforest/magma/client/wire"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/memdb"
)

const revisionLength = 8

func setIDInEntity[T any](o *T, id *memdb.ID) {
	copy(unsafeIDFromEntity(o), unsafe.Slice((*byte)(unsafe.Pointer(id)), memdb.IDLength))
}

func unsafeIDFromEntity[T any](o *T) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(o)), memdb.IDLength)
}

func setRevisionInEntity[T any](o *T, revision *types.Revision) {
	copy(
		unsafe.Slice((*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(o))+memdb.IDLength)), revisionLength),
		unsafe.Slice((*byte)(unsafe.Pointer(revision)), revisionLength),
	)
}

func revisionFromEntity[T any](o *T) types.Revision {
	return *(*types.Revision)(unsafe.Pointer(uintptr(unsafe.Pointer(o)) + memdb.IDLength))
}

func copyMetaFromEntity[T any](meta *wire.EntityMetadata, o *T) {
	copy(
		unsafe.Slice((*byte)(unsafe.Pointer(meta)), memdb.IDLength+revisionLength),
		unsafe.Slice((*byte)(unsafe.Pointer(o)), memdb.IDLength+revisionLength),
	)
}

func copyMetaToEntity[T any](o *T, meta *wire.EntityMetadata) {
	copy(
		unsafe.Slice((*byte)(unsafe.Pointer(o)), memdb.IDLength+revisionLength),
		unsafe.Slice((*byte)(unsafe.Pointer(meta)), memdb.IDLength+revisionLength),
	)
}
