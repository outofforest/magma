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
func (v *View) Get(ePtr any, id any) (bool, error) {
	ePtrV := reflect.ValueOf(ePtr)
	if ePtrV.Kind() != reflect.Ptr {
		return false, errors.Errorf("pointer expected, got %s", ePtrV.Type())
	}
	eV := ePtrV.Elem()
	eT := eV.Type()
	typeDef, exists := v.byType[eT]
	if !exists {
		return false, errors.Errorf("type %s not defined", eT)
	}

	receivedIDType := reflect.TypeOf(id)
	if receivedIDType != typeDef.IDType {
		return false, errors.Errorf("expected id type %s, got %s", typeDef.IDType, receivedIDType)
	}

	o, err := v.tx.First(typeDef.Table, idIndex, id)
	if err != nil {
		return false, errors.WithStack(err)
	}

	if o == nil {
		return false, nil
	}

	eV.Set(reflect.ValueOf(o))
	return true, nil
}

// Find returns the first object matching indexed values.
func (v *View) Find(ePtr any, index indices.Index, args ...any) (bool, error) {
	ePtrV := reflect.ValueOf(ePtr)
	if ePtrV.Kind() != reflect.Ptr {
		return false, errors.Errorf("pointer expected, got %s", ePtrV.Type())
	}
	eV := ePtrV.Elem()
	eT := eV.Type()
	if eT != index.Type() {
		return false, errors.Errorf("expected index type %s, got %s", eT, index.Type())
	}

	typeDef, exists := v.byType[eT]
	if !exists {
		return false, errors.Errorf("type %s not defined", eT)
	}

	o, err := v.tx.First(typeDef.Table, index.Name(), args...)
	if err != nil {
		return false, errors.WithStack(err)
	}

	if o == nil {
		return false, nil
	}

	eV.Set(reflect.ValueOf(o))
	return true, nil
}

// Tx represents transaction.
type Tx struct {
	*View

	changes map[types.ID]reflect.Value
}

// Get returns the object.
func (tx *Tx) Get(ePtr any, id any) bool {
	exists, err := tx.View.Get(ePtr, id)
	if err != nil {
		panic(err)
	}
	return exists
}

// Find returns the first object matching indexed values.
func (tx *Tx) Find(ePtr any, index indices.Index, args ...any) bool {
	exists, err := tx.View.Find(ePtr, index, args...)
	if err != nil {
		panic(err)
	}
	return exists
}

// Set sets object in transaction.
func (tx *Tx) Set(o any) {
	oValue := reflect.ValueOf(o)
	oType := oValue.Type()
	if oType.Kind() == reflect.Ptr {
		panic(errors.New("object must not be a pointer"))
	}

	typeDef, exists := tx.byType[oType]
	if !exists {
		panic(errors.Errorf("unknown type %s", oType))
	}
	id := oValue.Field(typeDef.IDIndex).Convert(idType).Interface().(types.ID)
	if id == emptyID {
		panic(errors.Errorf("id is empty"))
	}

	if err := tx.tx.Insert(typeDef.Table, o); err != nil {
		panic(errors.WithStack(err))
	}

	tx.changes[id] = oValue
}
