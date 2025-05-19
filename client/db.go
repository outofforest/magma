package client

import (
	"reflect"

	"github.com/pkg/errors"

	"github.com/outofforest/memdb"
	memdbid "github.com/outofforest/memdb/id"
)

var emptyID memdb.ID

// View represents immutable snapshot of the DB.
type View struct {
	tx     *memdb.Txn
	byType map[reflect.Type]typeInfo
}

// Get returns the object.
func Get[T any](v *View, id any) (T, bool) {
	return first[T](v, memdbid.IndexID, id)
}

// First returns the first object matching indexed values.
func First[T any](v *View, index memdb.Index, args ...any) (T, bool) {
	if uint64(len(args)) > index.NumOfArgs() {
		panic(errors.New("too many arguments"))
	}
	return first[T](v, index.ID(), args...)
}

// Last returns the first object matching indexed values.
func Last[T any](v *View, index memdb.Index, args ...any) (T, bool) {
	if uint64(len(args)) > index.NumOfArgs() {
		panic(errors.New("too many arguments"))
	}
	return last[T](v, index.ID(), args...)
}

// All iterates over all entities using ID index.
func All[T any](v *View) func(func(T) bool) {
	return iterateForward[T](v, memdbid.IndexID)
}

// IterateForward iterates over entities in forward direction using provided index.
func IterateForward[T any](v *View, index memdb.Index, args ...any) func(func(T) bool) {
	if uint64(len(args)) > index.NumOfArgs() {
		panic(errors.New("too many arguments"))
	}
	return iterateForward[T](v, index.ID(), args...)
}

// IterateBackward iterates over entities in backward direction using provided index.
func IterateBackward[T any](v *View, index memdb.Index, args ...any) func(func(T) bool) {
	if uint64(len(args)) > index.NumOfArgs() {
		panic(errors.New("too many arguments"))
	}
	return iterateBackward[T](v, index.ID(), args...)
}

// AllIterator returns iterator iterating over all entities using ID index.
func AllIterator[T any](v *View) func() (T, bool) {
	return forwardIterator[T](v, memdbid.IndexID)
}

// ForwardIterator returns iterator iterating over all entities in forward direction using provided index.
func ForwardIterator[T any](v *View, index memdb.Index, args ...any) func() (T, bool) {
	if uint64(len(args)) > index.NumOfArgs() {
		panic(errors.New("too many arguments"))
	}
	return forwardIterator[T](v, index.ID(), args...)
}

// BackwardIterator returns iterator iterating over all entities in backward direction using provided index.
func BackwardIterator[T any](v *View, index memdb.Index, args ...any) func() (T, bool) {
	if uint64(len(args)) > index.NumOfArgs() {
		panic(errors.New("too many arguments"))
	}
	return backwardIterator[T](v, index.ID(), args...)
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

func last[T any](v *View, index uint64, args ...any) (T, bool) {
	var t T
	tt := reflect.TypeOf(t)
	typeDef, exists := v.byType[tt]
	if !exists {
		panic(errors.Errorf("type %s not defined", tt))
	}

	o, err := v.tx.Last(typeDef.TableID, index, args...)
	if err != nil {
		panic(errors.WithStack(err))
	}

	if o == nil {
		return t, false
	}
	return o.Elem().Interface().(T), true
}

func iterateForward[T any](v *View, index uint64, args ...any) func(func(T) bool) {
	var t T
	tt := reflect.TypeOf(t)
	typeDef, exists := v.byType[tt]
	if !exists {
		panic(errors.Errorf("type %s not defined", tt))
	}

	it, err := v.tx.Get(typeDef.TableID, index, args...)
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

func iterateBackward[T any](v *View, index uint64, args ...any) func(func(T) bool) {
	var t T
	tt := reflect.TypeOf(t)
	typeDef, exists := v.byType[tt]
	if !exists {
		panic(errors.Errorf("type %s not defined", tt))
	}

	it, err := v.tx.GetReverse(typeDef.TableID, index, args...)
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

func forwardIterator[T any](v *View, index uint64, args ...any) func() (T, bool) {
	var t T
	tt := reflect.TypeOf(t)
	typeDef, exists := v.byType[tt]
	if !exists {
		panic(errors.Errorf("type %s not defined", tt))
	}

	it, err := v.tx.Get(typeDef.TableID, index, args...)
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

func backwardIterator[T any](v *View, index uint64, args ...any) func() (T, bool) {
	var t T
	tt := reflect.TypeOf(t)
	typeDef, exists := v.byType[tt]
	if !exists {
		panic(errors.Errorf("type %s not defined", tt))
	}

	it, err := v.tx.GetReverse(typeDef.TableID, index, args...)
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
	oValue := reflect.ValueOf(o)
	if oValue.Kind() == reflect.Ptr {
		panic(errors.New("object must not be a pointer"))
	}

	oType := oValue.Type()
	typeDef, exists := tx.byType[oType]
	if !exists {
		panic(errors.Errorf("unknown type %s", oType))
	}

	oPtrValue := reflect.New(oType)
	oPtrValue.Elem().Set(oValue)
	id := memdb.ID(unsafeIDFromEntity(oPtrValue))
	if id == emptyID {
		panic(errors.Errorf("id is empty"))
	}
	old, err := tx.tx.Insert(typeDef.TableID, &oPtrValue)
	if err != nil {
		panic(errors.WithStack(err))
	}

	if ch, exists := tx.changes[id]; exists {
		ch.New = &oPtrValue
		tx.changes[id] = ch
		return
	}

	tx.changes[id] = change{
		Old: old,
		New: &oPtrValue,
	}
}
