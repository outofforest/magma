package client

import (
	"reflect"

	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"

	"github.com/outofforest/magma/types"
)

// View represents immutable snapshot of the DB.
type View struct {
	tx     *memdb.Txn
	byType map[reflect.Type]typeInfo
}

// Tx represents transaction.
type Tx struct {
	*View

	changes map[types.ID]reflect.Value
}

var emptyID types.ID

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

// Get gets object from view.
func Get[ET any, IDT idConstraint](v *View, id IDT) (ET, bool) {
	var e ET
	et := reflect.TypeOf(e)

	typeDef, exists := v.byType[et]
	if !exists {
		panic(errors.Errorf("unknown type %s", et))
	}

	expectedIDType := et.Field(typeDef.IDIndex).Type
	receivedIDType := reflect.TypeOf(id)
	if receivedIDType != expectedIDType {
		panic(errors.Errorf("expected id type %s, got %s", expectedIDType, receivedIDType))
	}

	o, err := v.tx.First(typeDef.Table, idIndex, id)
	if err != nil {
		panic(errors.WithStack(err))
	}

	if o == nil {
		return e, false
	}

	return o.(ET), true
}
