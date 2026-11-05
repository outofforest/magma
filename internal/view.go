package internal

import (
	"iter"
	"reflect"

	"github.com/pkg/errors"

	"github.com/outofforest/memdb"
)

// View represents non-persistent view of the DB.
// It is possible to modify the entities inside view,
// but those changes cannot be committed. They are always loca
// to this view only.
type View struct {
	tx      *memdb.Txn
	applier *Applier
}

// NewView creates new viewable representation of DB.
func NewView(tx *memdb.Txn, applier *Applier) *View {
	return &View{
		tx:      tx,
		applier: applier,
	}
}

// Set sets the entity inside view.
func (v *View) Set(o any) {
	v.applier.Insert(v.tx, o)
}

// First returns first entity from index.
func First[T any](v *View, index uint64, args ...any) (*T, bool) {
	t := reflect.TypeFor[T]()
	typeDef, exists := v.applier.byType[t]
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

// Iterate iterates over entities.
func Iterate[T any](v *View, index uint64, args ...any) iter.Seq[T] {
	it := createIterator[T](v, index, args)

	return func(yield func(e T) bool) {
		for pointer := it.Next(); pointer != defaultPointer; pointer = it.Next() {
			if !yield(*(*T)(pointer)) {
				return
			}
		}
	}
}

// IteratePointers iterates over pointers to entities.
func IteratePointers[T any](v *View, index uint64, args ...any) iter.Seq[*T] {
	it := createIterator[T](v, index, args)

	return func(yield func(e *T) bool) {
		for pointer := it.Next(); pointer != defaultPointer; pointer = it.Next() {
			if !yield((*T)(pointer)) {
				return
			}
		}
	}
}

// Iterator returns entity iterator.
func Iterator[T any](v *View, index uint64, args ...any) func() (T, bool) {
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

// IteratorPointers returns entity pointer iterator.
func IteratorPointers[T any](v *View, index uint64, args ...any) func() (*T, bool) {
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
	typeDef, exists := v.applier.byType[t]
	if !exists {
		panic(errors.Errorf("type %s not defined", t))
	}

	it, err := v.tx.Iterator(typeDef.TableID, index, args...)
	if err != nil {
		panic(errors.WithStack(err))
	}

	return it
}
