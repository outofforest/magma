package db

import (
	"unsafe"

	"github.com/outofforest/magma/client/wire"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/memdb"
)

// SetIDInEntity sets ID in entity.
func SetIDInEntity(eValue unsafe.Pointer, id *memdb.ID) {
	copy(UnsafeIDFromEntity(eValue), unsafe.Slice((*byte)(unsafe.Pointer(id)), memdb.IDLength))
}

// UnsafeIDFromEntity reads ID from the entity.
func UnsafeIDFromEntity(eValue unsafe.Pointer) []byte {
	return unsafe.Slice((*byte)(eValue), memdb.IDLength)
}

// SetRevisionInEntity sets revision in entity.
func SetRevisionInEntity(eValue unsafe.Pointer, revision *types.Revision) {
	copy(
		unsafe.Slice((*byte)(unsafe.Add(eValue, memdb.IDLength)), types.RevisionLength),
		unsafe.Slice((*byte)(unsafe.Pointer(revision)), types.RevisionLength),
	)
}

// RevisionFromEntity reads revision from entity.
func RevisionFromEntity(eValue unsafe.Pointer) types.Revision {
	return *(*types.Revision)(unsafe.Add(eValue, memdb.IDLength))
}

// CopyMetaFromEntity copies metadata from entity.
func CopyMetaFromEntity(meta *wire.TxEntityMetadata, eValue unsafe.Pointer) {
	copy(
		unsafe.Slice((*byte)(unsafe.Pointer(meta)), memdb.IDLength+types.RevisionLength),
		unsafe.Slice((*byte)(eValue), memdb.IDLength+types.RevisionLength),
	)
}

// CopyMetaToEntity copies metadata to entity.
func CopyMetaToEntity(eValue unsafe.Pointer, meta *wire.TxEntityMetadata) {
	copy(
		unsafe.Slice((*byte)(eValue), memdb.IDLength+types.RevisionLength),
		unsafe.Slice((*byte)(unsafe.Pointer(meta)), memdb.IDLength+types.RevisionLength),
	)
}
