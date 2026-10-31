package db

import (
	"iter"
	"reflect"
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/magma/types"
	"github.com/outofforest/memdb"
	"github.com/outofforest/memdb/indices"
	"github.com/outofforest/proton"
)

var defaultPointer unsafe.Pointer

var emptyID memdb.ID

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

func first[T any](v *View, index uint64, args ...any) (*T, bool) {
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

// NewDBConfig creates db config.
func NewDBConfig(m proton.Marshaller, indices []memdb.Index) (memdb.Config, error) {
	entities, err := typesFromMarshaller(m)
	if err != nil {
		return memdb.Config{}, err
	}

	return memdb.Config{
		Entities: entities,
		Indices:  indices,
	}, nil
}

func typesFromMarshaller(m proton.Marshaller) ([]reflect.Type, error) {
	entities := m.Messages()
	if len(entities) == 0 {
		return nil, errors.New("no object types provided")
	}

	types := make([]reflect.Type, 0, len(entities))
	for _, e := range entities {
		types = append(types, reflect.TypeOf(e))
	}

	return types, nil
}

var revisionType = reflect.TypeFor[types.Revision]()
var idType = reflect.TypeFor[memdb.ID]()

func validateType(t reflect.Type) (reflect.Type, error) {
	idF, exists := t.FieldByName("ID")
	if !exists {
		return nil, errors.Errorf("object %s has no ID field", t)
	}
	if !idF.Type.ConvertibleTo(idType) {
		return nil, errors.Errorf("object's %s ID field must be of type %s", t, idType)
	}
	if idF.Index[0] != 0 || idF.Offset != 0 {
		return nil, errors.Errorf("id must be the first field in type %s", t)
	}

	revisionF, exists := t.FieldByName("Revision")
	if !exists {
		return nil, errors.Errorf("object %s has no Revision field", t)
	}
	if revisionF.Type != revisionType {
		return nil, errors.Errorf("object's %s Revision field must be of type %s", t, revisionType)
	}
	if revisionF.Index[0] != 1 || revisionF.Offset != memdb.IDLength {
		return nil, errors.Errorf("revision must be the second field in type %d", t)
	}

	return idF.Type, nil
}
