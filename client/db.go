package client

import (
	"reflect"
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/magma/client/wire"
	"github.com/outofforest/memdb"
	"github.com/outofforest/proton"
)

var _ Tx = &tx{}

var emptyID memdb.ID

// View represents non-persistent view of the DB.
// It is possible to modify the entities inside view,
// but those changes cannot be committed. They are always loca
// to this view only.
type View struct {
	tx     *memdb.Txn
	byType map[reflect.Type]typeInfo
}

// Set sets the entity inside view.
func (v *View) Set(o any) {
	insert(v.tx, v.byType, o)
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
	t := reflect.TypeFor[T]()
	typeDef, exists := v.byType[t]
	if !exists {
		panic(errors.Errorf("type %s not defined", t))
	}

	o, err := memdb.First[T](v.tx, typeDef.TableID, index, args...)
	if err != nil {
		panic(errors.WithStack(err))
	}

	if o == nil {
		var o T
		return o, false
	}
	return *o, true
}

func iterate[T any](v *View, index uint64, args ...any) func(func(T) bool) {
	t := reflect.TypeFor[T]()
	typeDef, exists := v.byType[t]
	if !exists {
		panic(errors.Errorf("type %s not defined", t))
	}

	it, err := memdb.Iterator[T](v.tx, typeDef.TableID, index, args...)
	if err != nil {
		panic(errors.WithStack(err))
	}

	return func(yield func(e T) bool) {
		for e := it.Next(); e != nil; e = it.Next() {
			if !yield(*e) {
				return
			}
		}
	}
}

func iterator[T any](v *View, index uint64, args ...any) func() (T, bool) {
	t := reflect.TypeFor[T]()
	typeDef, exists := v.byType[t]
	if !exists {
		panic(errors.Errorf("type %s not defined", t))
	}

	it, err := memdb.Iterator[T](v.tx, typeDef.TableID, index, args...)
	if err != nil {
		panic(errors.WithStack(err))
	}

	return func() (T, bool) {
		e := it.Next()
		if e == nil {
			var o T
			return o, false
		}
		return *e, true
	}
}

type change struct {
	OldValue   any
	StartIndex uint64
	EndIndex   uint64
	Added      bool
}

type changeID struct {
	ID    memdb.ID
	MsgID uint64
}

type tx struct {
	client       *Client
	db           *memdb.MemDB
	buf          []byte
	size         uint64
	numOfObjects uint64
	changes      map[changeID]change
}

// View returns non-persistent view of the DB.
func (tx *tx) View() *View {
	return &View{
		tx:     tx.db.Txn(true),
		byType: tx.client.byType,
	}
}

// Set sets object in transaction. This function includes the object in tx even if patch is empty.
// This is done to detect possible conflicts with other transactions. Use this function if atomicity
// is required (most of the cases). Compare to SoftSet below.
func Set[T any](tx *tx, o T) error {
	return set(tx, o, false)
}

// SoftSet sets object in transaction. It does it only if the object patch is not empty.
// This function is good to use when you don't care about atomicity in the tx.
// It might happen that SoftSet doesn't include the object in tx due to empty patch, but somewhere
// else conflicting transaction is created. This conflict is not detected because we haven't included object
// with incremented revision.
// Good scenario to use SoftSet is loading batches of unrelated object where conflicts don't matter.
func SoftSet[T any](tx *tx, o T) error {
	return set(tx, o, true)
}

func set[T any](tx *tx, o T, isSoftSet bool) error {
	dbTx := tx.db.Txn(true)

	id, typeDef, oldValue, newValue := insert(dbTx, tx.client.byType, o)

	chID := changeID{
		ID:    id,
		MsgID: typeDef.MsgID,
	}
	chg, chgExists := tx.changes[chID]

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
			tx.changes[chID] = chg
		}
	} else {
		chg = change{
			OldValue: oldValue,
		}
	}

	unsafeID := unsafeIDFromEntity(newValue)
	var oldV *T
	var isNeeded bool
	if chg.OldValue == nil {
		oldV = new(T)
		// FIXME (wojciech): Is it needed?
		setIDInEntity(oldV, (*memdb.ID)(unsafe.Pointer(&unsafeID[0])))
		isNeeded = true
	} else {
		oldV = chg.OldValue.(*T)

		var err error
		isNeeded, err = tx.client.config.Marshaller.IsPatchNeeded(newValue, oldV)
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

	_, msgSize, err := tx.client.config.Marshaller.MakePatch(newValue, oldV, tx.buf[chg.EndIndex:])

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
	tx.changes[chID] = chg

	dbTx.Commit()

	return nil
}

func insert[T any](
	tx *memdb.Txn,
	byType map[reflect.Type]typeInfo,
	o T,
) (memdb.ID, typeInfo, *T, *T) {
	oType := reflect.TypeFor[T]()
	typeDef, exists := byType[oType]
	if !exists {
		panic(errors.Errorf("unknown type %s", oType))
	}

	id := memdb.ID(unsafeIDFromEntity(&o))
	if id == emptyID {
		panic(errors.Errorf("id is empty"))
	}
	old, err := memdb.Insert(tx, typeDef.TableID, &o)
	if err != nil {
		panic(errors.WithStack(err))
	}

	return id, typeDef, old, &o
}
