package client

import (
	"reflect"

	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"

	"github.com/outofforest/magma/client/indices"
	"github.com/outofforest/magma/types"
)

var emptyID types.ID

// View represents immutable snapshot of the DB.
type View struct {
	tx     *memdb.Txn
	byType map[reflect.Type]typeInfo
}

// Get returns the object.
func Get[T any](v *View, id any) (T, bool) {
	return find[T](v, idIndexName, id)
}

// Find returns the first object matching indexed values.
func Find[T any](v *View, index indices.Index, args ...any) (T, bool) {
	if uint64(len(args)) > index.NumOfArgs() {
		panic(errors.New("too many arguments"))
	}
	return find[T](v, index.Name(), args...)
}

// All iterates over all entities using ID index.
func All[T any](v *View) func(func(T) bool) {
	return iterate[T](v, idIndexName)
}

// Iterate iterates over entities using provided index.
func Iterate[T any](v *View, index indices.Index, args ...any) func(func(T) bool) {
	if uint64(len(args)) > index.NumOfArgs() {
		panic(errors.New("too many arguments"))
	}
	return iterate[T](v, index.Name(), args...)
}

// AllIterator returns iterator iterating over all entities using ID index.
func AllIterator[T any](v *View) func() (T, bool) {
	return iterator[T](v, idIndexName)
}

// Iterator returns iterator iterating over all entities using provided index.
func Iterator[T any](v *View, index indices.Index, args ...any) func() (T, bool) {
	if uint64(len(args)) > index.NumOfArgs() {
		panic(errors.New("too many arguments"))
	}
	return iterator[T](v, index.Name(), args...)
}

func find[T any](v *View, index string, args ...any) (T, bool) {
	var t T
	tt := reflect.TypeOf(t)
	typeDef, exists := v.byType[tt]
	if !exists {
		panic(errors.Errorf("type %s not defined", tt))
	}

	o, err := v.tx.First(typeDef.Table, index, args...)
	if err != nil {
		panic(errors.WithStack(err))
	}

	if o == nil {
		return t, false
	}
	return o.(reflect.Value).Elem().Interface().(T), true
}

func iterate[T any](v *View, index string, args ...any) func(func(T) bool) {
	var t T
	tt := reflect.TypeOf(t)
	typeDef, exists := v.byType[tt]
	if !exists {
		panic(errors.Errorf("type %s not defined", tt))
	}

	it, err := v.tx.Get(typeDef.Table, index, args...)
	if err != nil {
		panic(errors.WithStack(err))
	}

	return func(yield func(e T) bool) {
		for e := it.Next(); e != nil; e = it.Next() {
			if !yield(e.(reflect.Value).Elem().Interface().(T)) {
				return
			}
		}
	}
}

func iterator[T any](v *View, index string, args ...any) func() (T, bool) {
	var t T
	tt := reflect.TypeOf(t)
	typeDef, exists := v.byType[tt]
	if !exists {
		panic(errors.Errorf("type %s not defined", tt))
	}

	it, err := v.tx.Get(typeDef.Table, index, args...)
	if err != nil {
		panic(errors.WithStack(err))
	}

	return func() (T, bool) {
		e := it.Next()
		if e == nil {
			return t, false
		}
		return e.(reflect.Value).Elem().Interface().(T), true
	}
}

// Tx represents transaction.
type Tx struct {
	*View

	changes map[types.ID]reflect.Value
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
	id := types.ID(unsafeIDFromEntity(oPtrValue))
	if id == emptyID {
		panic(errors.Errorf("id is empty"))
	}
	if err := tx.tx.Insert(typeDef.Table, oPtrValue); err != nil {
		panic(errors.WithStack(err))
	}

	tx.changes[id] = oPtrValue
}
