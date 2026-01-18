package client

import (
	"reflect"
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/magma/client/wire"
	"github.com/outofforest/memdb"
	"github.com/outofforest/proton"
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
	OldValue   *reflect.Value
	StartIndex uint64
	EndIndex   uint64
	Added      bool
}

// Tx represents transaction.
type Tx struct {
	client       *Client
	db           *memdb.MemDB
	buf          []byte
	size         uint64
	numOfObjects uint64
	changes      map[memdb.ID]change
}

// View returns read-only view of the DB.
func (tx *Tx) View() *View {
	return &View{
		tx:     tx.db.Txn(false),
		byType: tx.client.byType,
	}
}

// Set sets object in transaction. This function includes the object in tx even if patch is empty.
// This is done to detect possible conflicts with other transactions. Use this function if atomicity
// is required (most of the cases). Compare to SoftSet below.
func (tx *Tx) Set(o any) error {
	return tx.set(o, false)
}

// SoftSet sets object in transaction. It does it only if the object patch is not empty.
// This function is good to use when you don't care about atomicity in the tx.
// It might happen that SoftSet doesn't include the object in tx due to empty patch, but somewhere
// else conflicting transaction is created. This conflict is not detected because we haven't included object
// with incremented revision.
// Good scenario to use SoftSet is loading batches of unrelated object where conflicts don't matter.
func (tx *Tx) SoftSet(o any) error {
	return tx.set(o, true)
}

func (tx *Tx) set(o any, isSoftSet bool) error {
	dbTx := tx.db.Txn(true)
	defer dbTx.Abort()

	id, oldValue, newValue := insert(dbTx, tx.client.byType, o)

	chg, chgExists := tx.changes[id]

	//nolint:nestif
	if chgExists {
		if chg.EndIndex > 0 {
			if chg.EndIndex == tx.size {
				tx.size = chg.StartIndex
			} else {
				copy(tx.buf[chg.StartIndex:], tx.buf[chg.EndIndex:tx.size])
				sizeUpdate := chg.EndIndex - chg.StartIndex
				for i, ch := range tx.changes {
					if ch.StartIndex > chg.StartIndex {
						ch.StartIndex -= sizeUpdate
						ch.EndIndex -= sizeUpdate
					}
					tx.changes[i] = ch
				}
				tx.size -= sizeUpdate
			}

			if chg.Added {
				tx.numOfObjects--
			}

			// This is done to know later that there is nothing to move in the tx if set fails.
			chg.StartIndex = 0
			chg.EndIndex = 0
			chg.Added = false
			tx.changes[id] = chg
		}
	} else {
		chg = change{
			OldValue: oldValue,
		}
	}

	vv := newValue.Elem()
	typeDef, exists := tx.client.byType[vv.Type()]
	if !exists {
		return errors.Errorf("unknown type %s", vv.Type())
	}

	unsafeID := unsafeIDFromEntity(*newValue)
	var oldV reflect.Value
	var isNeeded bool
	if chg.OldValue == nil {
		oldV = reflect.New(typeDef.Type)
		setIDInEntity(oldV, (*memdb.ID)(unsafe.Pointer(&unsafeID[0])))
		isNeeded = true
	} else {
		oldV = *chg.OldValue

		var err error
		isNeeded, err = tx.client.config.Marshaller.IsPatchNeeded(newValue.Interface(), oldV.Interface())
		if err != nil {
			return err
		}
		if isSoftSet && !isNeeded {
			return nil
		}
	}

	entityMeta := &wire.EntityMetadata{
		MessageID: typeDef.MsgID,
	}
	copyMetaFromEntity(entityMeta, oldV)
	entityMeta.Revision++

	chg.StartIndex = tx.size
	chg.EndIndex = chg.StartIndex
	_, entitySize, err := tx.client.metaM.Marshal(entityMeta, tx.buf[chg.EndIndex:])

	switch {
	case err == nil:
	case errors.Is(err, proton.ErrBufferFailure):
		return ErrTxTooBig
	default:
		return err
	}

	chg.EndIndex += entitySize

	_, msgSize, err := tx.client.config.Marshaller.MakePatch(newValue.Interface(), oldV.Interface(), tx.buf[chg.EndIndex:])

	switch {
	case err == nil:
	case errors.Is(err, proton.ErrBufferFailure):
		return ErrTxTooBig
	default:
		return err
	}

	if isNeeded {
		chg.Added = true
		tx.numOfObjects++
	}

	chg.EndIndex += msgSize
	tx.size = chg.EndIndex
	tx.changes[id] = chg

	dbTx.Commit()

	return nil
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
