package client

import (
	"reflect"

	"github.com/pkg/errors"

	"github.com/outofforest/memdb"
)

var emptyID memdb.ID

// View represents immutable snapshot of the DB.
type View struct {
	tx     *memdb.Txn
	byType map[reflect.Type]typeInfo
}

// Get returns the object.
func Get[T any](v *View, id any) (T, bool) {
	return first[T](v, memdb.IDIndexID, id)
}

// First returns the first object matching indexed values.
func First[T any](v *View, index memdb.Index, args ...any) (T, bool) {
	return first[T](v, index.ID(), args...)
}

// All iterates over all entities using ID index.
func All[T any](v *View) func(func(T) bool) {
	return iterate[T](v, memdb.IDIndexID)
}

// Iterate iterates over entities matching index in forward direction.
func Iterate[T any](v *View, index memdb.Index, args ...any) func(func(T) bool) {
	return iterate[T](v, index.ID(), args...)
}

// AllIterator returns iterator iterating over all entities using ID index.
func AllIterator[T any](v *View) func() (T, bool) {
	return iterator[T](v, memdb.IDIndexID)
}

// Iterator returns iterator iterating over entities matching index in forward direction.
func Iterator[T any](v *View, index memdb.Index, args ...any) func() (T, bool) {
	return iterator[T](v, index.ID(), args...)
}

func first[T any](v *View, index uint64, args ...any) (T, bool) {
	var t T
	tt := reflect.TypeOf(t)
	typeDef, exists := v.byType[tt]
	if !exists {
		panic(errors.Errorf("type %s not defined", tt))
	}

	o, err := v.tx.First(typeDef.TableID, index, args...)
	if err != nil {
		panic(errors.WithStack(err))
	}

	if o == nil {
		return t, false
	}
	return o.Elem().Interface().(T), true
}

func iterate[T any](v *View, index uint64, args ...any) func(func(T) bool) {
	var t T
	tt := reflect.TypeOf(t)
	typeDef, exists := v.byType[tt]
	if !exists {
		panic(errors.Errorf("type %s not defined", tt))
	}

	it, err := v.tx.Iterator(typeDef.TableID, index, args...)
	if err != nil {
		panic(errors.WithStack(err))
	}

	return func(yield func(e T) bool) {
		for e := it.Next(); e != nil; e = it.Next() {
			if !yield(e.Elem().Interface().(T)) {
				return
			}
		}
	}
}

func iterator[T any](v *View, index uint64, args ...any) func() (T, bool) {
	var t T
	tt := reflect.TypeOf(t)
	typeDef, exists := v.byType[tt]
	if !exists {
		panic(errors.Errorf("type %s not defined", tt))
	}

	it, err := v.tx.Iterator(typeDef.TableID, index, args...)
	if err != nil {
		panic(errors.WithStack(err))
	}

	return func() (T, bool) {
		e := it.Next()
		if e == nil {
			return t, false
		}
		return e.Elem().Interface().(T), true
	}
}

type change struct {
	Old *reflect.Value
	New *reflect.Value
}

// Tx represents transaction.
type Tx struct {
	*View

	changes map[memdb.ID]change
}

// Set sets object in transaction.
func (tx *Tx) Set(o any) {
	id, oldValue, newValue := insert(tx.tx, tx.byType, o)

	if ch, exists := tx.changes[id]; exists {
		ch.New = newValue
		tx.changes[id] = ch
		return
	}

	tx.changes[id] = change{
		Old: oldValue,
		New: newValue,
	}
}

func insert(tx *memdb.Txn, byType map[reflect.Type]typeInfo, o any) (memdb.ID, *reflect.Value, *reflect.Value) {
	oValue := reflect.ValueOf(o)
	if oValue.Kind() == reflect.Ptr {
		panic(errors.New("object must not be a pointer"))
	}

	oType := oValue.Type()
	typeDef, exists := byType[oType]
	if !exists {
		panic(errors.Errorf("unknown type %s", oType))
	}

	oPtrValue := reflect.New(oType)
	oPtrValue.Elem().Set(oValue)
	id := memdb.ID(unsafeIDFromEntity(oPtrValue))
	if id == emptyID {
		panic(errors.Errorf("id is empty"))
	}
	old, err := tx.Insert(typeDef.TableID, &oPtrValue)
	if err != nil {
		panic(errors.WithStack(err))
	}

	return id, old, &oPtrValue
}
