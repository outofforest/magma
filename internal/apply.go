package internal

import (
	"encoding/binary"
	"io"
	"reflect"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/zeebo/xxh3"

	"github.com/outofforest/magma/client/wire"
	"github.com/outofforest/magma/state/repository/format"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/memdb"
	"github.com/outofforest/proton"
	"github.com/outofforest/varuint64"
)

var (
	emptyID        memdb.ID
	defaultPointer unsafe.Pointer
)

// Applier stores transaction in database.
type Applier struct {
	metaM  proton.Marshaller
	m      proton.Marshaller
	byID   map[uint64]TypeInfo
	byType map[reflect.Type]TypeInfo
}

// NewApplier creates applier.
func NewApplier(metaM, m proton.Marshaller) (*Applier, error) {
	byID, byType, err := NewTypeCache(m)
	if err != nil {
		return nil, err
	}

	return &Applier{
		metaM:  metaM,
		m:      m,
		byID:   byID,
		byType: byType,
	}, nil
}

// ApplyTx stores transaction in database.
func (a *Applier) ApplyTx(
	previousChecksum uint64,
	tx *memdb.Txn,
	txRaw []byte,
	updatedTypes map[reflect.Type]struct{},
) (memdb.ID, uint64, error) {
	txLen, n := varuint64.Parse(txRaw)

	if txLen < format.ChecksumSize {
		return memdb.ID{}, 0, errors.New("unexpected tx size")
	}

	i := len(txRaw) - format.ChecksumSize
	checksum := xxh3.HashSeed(txRaw[:i], previousChecksum)
	if binary.LittleEndian.Uint64(txRaw[i:]) != checksum {
		return memdb.ID{}, 0, errors.New("tx checksum mismatch")
	}

	txLen -= format.ChecksumSize
	txRaw = txRaw[n : n+txLen]

	if _, n2 := varuint64.Parse(txRaw); n2 == txLen {
		// This is a term mark. Ignore.
		return memdb.ID{}, checksum, nil
	}

	metaID, n := varuint64.Parse(txRaw)
	metaAny, metaSize, err := a.metaM.Unmarshal(metaID, txRaw[n:])
	if err != nil {
		return memdb.ID{}, 0, err
	}
	meta := metaAny.(*wire.TxMetadata)

	return meta.ID, checksum, a.storeTx(meta.EntityMetadataID, tx, txRaw[n+metaSize:], updatedTypes)
}

// InitReader reads log from stream.
type InitReader interface {
	ReceiveRawBytes() ([]byte, uint64, error)
}

// ApplyInit stores objects from init in database.
func (a *Applier) ApplyInit(
	r InitReader,
	initSize uint64,
	tx *memdb.Txn,
	updatedTypes map[reflect.Type]struct{},
) (uint64, error) {
	initMetaMsgID, err := a.metaM.ID(&wire.InitMetadata{})
	if err != nil {
		return 0, err
	}

	headerRaw, _, err := r.ReceiveRawBytes()

	if err != nil {
		return 0, err
	}
	_, n := varuint64.Parse(headerRaw)
	size := uint64(len(headerRaw))
	if size > initSize {
		return 0, errors.New("invalid size")
	}

	initMetaMsg, _, err := a.metaM.Unmarshal(initMetaMsgID, headerRaw[n:])
	if err != nil {
		return 0, err
	}

	initMeta := initMetaMsg.(*wire.InitMetadata)

	if size == initSize {
		return initMeta.PreviousChecksum, nil
	}

	for {
		objRaw, _, err := r.ReceiveRawBytes()
		switch {
		case err == nil:
		case errors.Is(err, io.EOF):
			return initMeta.PreviousChecksum, nil
		default:
			return 0, err
		}

		size += uint64(len(objRaw))
		if size > initSize {
			return 0, errors.New("invalid size")
		}

		if err := a.storeObject(initMeta.EntityMetadataID, tx, objRaw, updatedTypes); err != nil {
			return 0, err
		}

		if size == initSize {
			return initMeta.PreviousChecksum, nil
		}
	}
}

// View creates view based on tx.
func (a *Applier) View(tx *memdb.Txn) *View {
	return NewView(tx, a)
}

// Insert inserts object to DB.
func (a *Applier) Insert(tx *memdb.Txn, o any) (memdb.ID, TypeInfo, unsafe.Pointer, unsafe.Pointer) {
	oValue := reflect.ValueOf(o)
	if oValue.Kind() == reflect.Pointer {
		panic(errors.New("object must not be a pointer"))
	}

	oType := oValue.Type()
	typeDef, exists := a.byType[oType]
	if !exists {
		panic(errors.Errorf("unknown type %s", oType))
	}

	oPtrValue := reflect.New(oType)
	oPtrValue.Elem().Set(oValue)
	id := memdb.ID(UnsafeIDFromEntity(oPtrValue.UnsafePointer()))
	if id == emptyID {
		panic(errors.Errorf("id is empty"))
	}
	old, err := tx.Insert(typeDef.TableID, oPtrValue.UnsafePointer())
	if err != nil {
		panic(errors.WithStack(err))
	}

	return id, typeDef, old, oPtrValue.UnsafePointer()
}

func (a *Applier) storeTx(
	entityMetaID uint64,
	tx *memdb.Txn,
	txRaw []byte,
	updatedTypes map[reflect.Type]struct{},
) error {
	dbTx := tx.Txn(true)
	for len(txRaw) > 0 {
		entityMetaRaw, entityMetaSize, err := a.metaM.Unmarshal(entityMetaID, txRaw)
		if err != nil {
			return err
		}

		entityMeta := entityMetaRaw.(*wire.TxEntityMetadata)

		typeDef, exists := a.byID[entityMeta.MessageID]
		if !exists {
			return errors.Errorf("unknown type %s", typeDef.Type)
		}

		pointer, err := dbTx.First(typeDef.TableID, memdb.IDIndexID, entityMeta.ID)
		if err != nil {
			return errors.WithStack(err)
		}

		oV := reflect.New(typeDef.Type)
		if pointer == defaultPointer {
			CopyMetaToEntity(oV.UnsafePointer(), entityMeta)
		} else {
			if entityMeta.Revision <= RevisionFromEntity(pointer) {
				return ErrTxOutdatedTx
			}
			oV.Elem().Set(reflect.NewAt(typeDef.Type, pointer).Elem())
			SetRevisionInEntity(oV.UnsafePointer(), &entityMeta.Revision)
		}

		msgSize, err := a.m.ApplyPatch(oV.Interface(), txRaw[entityMetaSize:])
		if err != nil {
			return err
		}

		if _, err := dbTx.Insert(typeDef.TableID, oV.UnsafePointer()); err != nil {
			return errors.WithStack(err)
		}

		txRaw = txRaw[entityMetaSize+msgSize:]
		updatedTypes[typeDef.Type] = struct{}{}
	}

	dbTx.Commit()

	return nil
}

func (a *Applier) storeObject(
	entityMetaID uint64,
	tx *memdb.Txn,
	objRaw []byte,
	updatedTypes map[reflect.Type]struct{},
) error {
	_, n := varuint64.Parse(objRaw)

	objRaw = objRaw[n:]

	entityMetaRaw, entityMetaSize, err := a.metaM.Unmarshal(entityMetaID, objRaw)
	if err != nil {
		return err
	}

	entityMeta := entityMetaRaw.(*wire.InitEntityMetadata)

	typeDef, exists := a.byID[entityMeta.MessageID]
	if !exists {
		return errors.Errorf("unknown type %s", typeDef.Type)
	}

	msg, _, err := a.m.Unmarshal(entityMeta.MessageID, objRaw[entityMetaSize:])
	if err != nil {
		return err
	}

	oPointer := reflect.ValueOf(msg).UnsafePointer()
	SetIDInEntity(oPointer, &entityMeta.ID)

	if _, err := tx.Insert(typeDef.TableID, oPointer); err != nil {
		return errors.WithStack(err)
	}

	updatedTypes[typeDef.Type] = struct{}{}

	return nil
}

// TypeInfo caches information about entity type.
type TypeInfo struct {
	Type    reflect.Type
	IDType  reflect.Type
	MsgID   uint64
	TableID uint64
}

// NewTypeCache creates new type cache from marshaller.
func NewTypeCache(m proton.Marshaller) (map[uint64]TypeInfo, map[reflect.Type]TypeInfo, error) {
	entities, err := typesFromMarshaller(m)
	if err != nil {
		return nil, nil, err
	}

	byID := map[uint64]TypeInfo{}
	byType := map[reflect.Type]TypeInfo{}
	for tableID, t := range entities {
		idFType, err := validateType(t)
		if err != nil {
			return nil, nil, err
		}

		if _, exists := byType[t]; exists {
			return nil, nil, errors.Errorf("double registration of object %s", t)
		}

		msgID, err := m.ID(reflect.New(t).Interface())
		if err != nil {
			return nil, nil, err
		}

		info := TypeInfo{
			Type:    t,
			IDType:  idFType,
			MsgID:   msgID,
			TableID: uint64(tableID),
		}
		byID[msgID] = info
		byType[t] = info
	}

	return byID, byType, nil
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
