package db

import (
	"iter"
	"reflect"
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/magma/internal"
	"github.com/outofforest/memdb"
	"github.com/outofforest/memdb/indices"
)

// View represents non-persistent view of the DB.
// It is possible to modify the entities inside view,
// but those changes cannot be committed. They are always loca
// to this view only.
type View struct {
	tx     *memdb.Txn
	byType map[reflect.Type]internal.TypeInfo
}

// NewView creates new viewable representation of DB.
func NewView(tx *memdb.Txn, byType map[reflect.Type]internal.TypeInfo) *View {
	return &View{
		tx:     tx,
		byType: byType,
	}
}

// Set sets the entity inside view.
func (v *View) Set(o any) {
	internal.Insert(v.tx, o, v.byType)
}

// Get returns the object.
func Get[T any](v *View, id any) (T, bool) {
	o, exists := first[T](v, memdb.IDIndexID, id)
	if !exists {
		var o T
		return o, false
	}
	return *o, true
}

// GetPointer returns pointer to the object.
func GetPointer[T any](v *View, id any) (*T, bool) {
	return first[T](v, memdb.IDIndexID, id)
}

// First returns the first object matching indexed values.
func First[T any](v *View, index indices.Index[T], args ...any) (T, bool) {
	o, exists := first[T](v, index.ID(), args...)
	if !exists {
		var o T
		return o, false
	}
	return *o, true
}

// FirstPointer returns pointer to the first object matching indexed values.
func FirstPointer[T any](v *View, index indices.Index[T], args ...any) (*T, bool) {
	return first[T](v, index.ID(), args...)
}

// All iterates over all entities using ID index.
func All[T any](v *View) iter.Seq[T] {
	return iterate[T](v, memdb.IDIndexID)
}

// AllPointers iterates over pointers to all entities using ID index.
func AllPointers[T any](v *View) iter.Seq[*T] {
	return iteratePointers[T](v, memdb.IDIndexID)
}

// Iterate iterates over entities matching index in forward direction.
func Iterate[T any](v *View, index indices.Index[T], args ...any) iter.Seq[T] {
	return iterate[T](v, index.ID(), args...)
}

// IteratePointers iterates over pointers to entities matching index in forward direction.
func IteratePointers[T any](v *View, index indices.Index[T], args ...any) iter.Seq[*T] {
	return iteratePointers[T](v, index.ID(), args...)
}

// AllIterator returns iterator iterating over all entities using ID index.
func AllIterator[T any](v *View) func() (T, bool) {
	return iterator[T](v, memdb.IDIndexID)
}

// AllIteratorPointers returns iterator iterating over pointers to all entities using ID index.
func AllIteratorPointers[T any](v *View) func() (*T, bool) {
	return iteratorPointers[T](v, memdb.IDIndexID)
}

// Iterator returns iterator iterating over entities matching index in forward direction.
func Iterator[T any](v *View, index indices.Index[T], args ...any) func() (T, bool) {
	return iterator[T](v, index.ID(), args...)
}

// IteratorPointers returns iterator iterating over pointers to entities matching index in forward direction.
func IteratorPointers[T any](v *View, index indices.Index[T], args ...any) func() (*T, bool) {
	return iteratorPointers[T](v, index.ID(), args...)
}

var defaultPointer unsafe.Pointer

func first[T any](v *View, index uint64, args ...any) (*T, bool) {
	t := reflect.TypeFor[T]()
	typeDef, exists := v.byType[t]
	if !exists {
		panic(errors.Errorf("type %s not defined", t))
	}

	pointer, err := v.tx.First(typeDef.TableID, index, args...)
	if err != nil {
		panic(errors.WithStack(err))
	}

	if pointer == defaultPointer {
		return nil, false
	}
	return (*T)(pointer), true
}

func iterate[T any](v *View, index uint64, args ...any) iter.Seq[T] {
	it := createIterator[T](v, index, args)

	return func(yield func(e T) bool) {
		for pointer := it.Next(); pointer != defaultPointer; pointer = it.Next() {
			if !yield(*(*T)(pointer)) {
				return
			}
		}
	}
}

func iteratePointers[T any](v *View, index uint64, args ...any) iter.Seq[*T] {
	it := createIterator[T](v, index, args)

	return func(yield func(e *T) bool) {
		for pointer := it.Next(); pointer != defaultPointer; pointer = it.Next() {
			if !yield((*T)(pointer)) {
				return
			}
		}
	}
}

func iterator[T any](v *View, index uint64, args ...any) func() (T, bool) {
	it := createIterator[T](v, index, args)

	return func() (T, bool) {
		pointer := it.Next()
		if pointer == defaultPointer {
			var o T
			return o, false
		}
		return *(*T)(pointer), true
	}
}

func iteratorPointers[T any](v *View, index uint64, args ...any) func() (*T, bool) {
	it := createIterator[T](v, index, args)

	return func() (*T, bool) {
		pointer := it.Next()
		if pointer == defaultPointer {
			return nil, false
		}
		return (*T)(pointer), true
	}
}

func createIterator[T any](v *View, index uint64, args []any) memdb.ResultIterator {
	t := reflect.TypeFor[T]()
	typeDef, exists := v.byType[t]
	if !exists {
		panic(errors.Errorf("type %s not defined", t))
	}

	it, err := v.tx.Iterator(typeDef.TableID, index, args...)
	if err != nil {
		panic(errors.WithStack(err))
	}

	return it
}
