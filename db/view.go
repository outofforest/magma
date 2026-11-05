package db

import (
	"iter"

	"github.com/outofforest/magma/internal"
	"github.com/outofforest/memdb"
	"github.com/outofforest/memdb/indices"
)

// View represents non-persistent view of the DB.
// It is possible to modify the entities inside view,
// but those changes cannot be committed. They are always loca
// to this view only.
type View = internal.View

// Get returns the object.
func Get[T any](v *View, id any) (T, bool) {
	o, exists := internal.First[T](v, memdb.IDIndexID, id)
	if !exists {
		var o T
		return o, false
	}
	return *o, true
}

// GetPointer returns pointer to the object.
func GetPointer[T any](v *View, id any) (*T, bool) {
	return internal.First[T](v, memdb.IDIndexID, id)
}

// First returns the first object matching indexed values.
func First[T any](v *View, index indices.Index[T], args ...any) (T, bool) {
	o, exists := internal.First[T](v, index.ID(), args...)
	if !exists {
		var o T
		return o, false
	}
	return *o, true
}

// FirstPointer returns pointer to the first object matching indexed values.
func FirstPointer[T any](v *View, index indices.Index[T], args ...any) (*T, bool) {
	return internal.First[T](v, index.ID(), args...)
}

// All iterates over all entities using ID index.
func All[T any](v *View) iter.Seq[T] {
	return internal.Iterate[T](v, memdb.IDIndexID)
}

// AllPointers iterates over pointers to all entities using ID index.
func AllPointers[T any](v *View) iter.Seq[*T] {
	return internal.IteratePointers[T](v, memdb.IDIndexID)
}

// Iterate iterates over entities matching index in forward direction.
func Iterate[T any](v *View, index indices.Index[T], args ...any) iter.Seq[T] {
	return internal.Iterate[T](v, index.ID(), args...)
}

// IteratePointers iterates over pointers to entities matching index in forward direction.
func IteratePointers[T any](v *View, index indices.Index[T], args ...any) iter.Seq[*T] {
	return internal.IteratePointers[T](v, index.ID(), args...)
}

// AllIterator returns iterator iterating over all entities using ID index.
func AllIterator[T any](v *View) func() (T, bool) {
	return internal.Iterator[T](v, memdb.IDIndexID)
}

// AllIteratorPointers returns iterator iterating over pointers to all entities using ID index.
func AllIteratorPointers[T any](v *View) func() (*T, bool) {
	return internal.IteratorPointers[T](v, memdb.IDIndexID)
}

// Iterator returns iterator iterating over entities matching index in forward direction.
func Iterator[T any](v *View, index indices.Index[T], args ...any) func() (T, bool) {
	return internal.Iterator[T](v, index.ID(), args...)
}

// IteratorPointers returns iterator iterating over pointers to entities matching index in forward direction.
func IteratorPointers[T any](v *View, index indices.Index[T], args ...any) func() (*T, bool) {
	return internal.IteratorPointers[T](v, index.ID(), args...)
}
