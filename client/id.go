package client

import (
	"unsafe"

	"github.com/outofforest/magma/client/wire"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/memdb"
)

const revisionLength = 8

func setIDInEntity(eValue unsafe.Pointer, id *memdb.ID) {
	copy(unsafeIDFromEntity(eValue), unsafe.Slice((*byte)(unsafe.Pointer(id)), memdb.IDLength))
}

func unsafeIDFromEntity(eValue unsafe.Pointer) []byte {
	return unsafe.Slice((*byte)(eValue), memdb.IDLength)
}

func setRevisionInEntity(eValue unsafe.Pointer, revision *types.Revision) {
	copy(
		unsafe.Slice((*byte)(unsafe.Pointer(uintptr(eValue)+memdb.IDLength)), revisionLength),
		unsafe.Slice((*byte)(unsafe.Pointer(revision)), revisionLength),
	)
}

func revisionFromEntity(eValue unsafe.Pointer) types.Revision {
	return *(*types.Revision)(unsafe.Pointer(uintptr(eValue) + memdb.IDLength))
}

func copyMetaFromEntity(meta *wire.EntityMetadata, eValue unsafe.Pointer) {
	copy(
		unsafe.Slice((*byte)(unsafe.Pointer(meta)), memdb.IDLength+revisionLength),
		unsafe.Slice((*byte)(eValue), memdb.IDLength+revisionLength),
	)
}

func copyMetaToEntity(eValue unsafe.Pointer, meta *wire.EntityMetadata) {
	copy(
		unsafe.Slice((*byte)(eValue), memdb.IDLength+revisionLength),
		unsafe.Slice((*byte)(unsafe.Pointer(meta)), memdb.IDLength+revisionLength),
	)
}
