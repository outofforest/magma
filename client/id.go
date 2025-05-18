package client

import (
	"reflect"
	"unsafe"

	"github.com/outofforest/magma/client/wire"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/memdb"
	memdbid "github.com/outofforest/memdb/id"
)

const revisionLength = 8

func setIDInEntity(eValue reflect.Value, id *memdb.ID) {
	copy(unsafeIDFromEntity(eValue), unsafe.Slice((*byte)(unsafe.Pointer(id)), memdbid.Length))
}

func unsafeIDFromEntity(eValue reflect.Value) []byte {
	return unsafe.Slice((*byte)(eValue.UnsafePointer()), memdbid.Length)
}

func setRevisionInEntity(eValue reflect.Value, revision *types.Revision) {
	copy(
		unsafe.Slice((*byte)(unsafe.Pointer(uintptr(eValue.UnsafePointer())+memdbid.Length)), revisionLength),
		unsafe.Slice((*byte)(unsafe.Pointer(revision)), revisionLength),
	)
}

func revisionFromEntity(eValue reflect.Value) types.Revision {
	return *(*types.Revision)(unsafe.Pointer(uintptr(eValue.UnsafePointer()) + memdbid.Length))
}

func copyMetaFromEntity(meta *wire.EntityMetadata, eValue reflect.Value) {
	copy(
		unsafe.Slice((*byte)(unsafe.Pointer(meta)), memdbid.Length+revisionLength),
		unsafe.Slice((*byte)(eValue.UnsafePointer()), memdbid.Length+revisionLength),
	)
}

func copyMetaToEntity(eValue reflect.Value, meta *wire.EntityMetadata) {
	copy(
		unsafe.Slice((*byte)(eValue.UnsafePointer()), memdbid.Length+revisionLength),
		unsafe.Slice((*byte)(unsafe.Pointer(meta)), memdbid.Length+revisionLength),
	)
}
