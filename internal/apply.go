package internal

import (
	"encoding/binary"
	"reflect"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/zeebo/xxh3"

	"github.com/outofforest/magma/client/wire"
	"github.com/outofforest/magma/state/repository/format"
	"github.com/outofforest/memdb"
	"github.com/outofforest/proton"
	"github.com/outofforest/varuint64"
)

var (
	emptyID        memdb.ID
	defaultPointer unsafe.Pointer
)

// ApplyTx stores transaction in database.
func ApplyTx(
	m proton.Marshaller,
	metaM proton.Marshaller,
	byID map[uint64]TypeInfo,
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
	metaAny, metaSize, err := metaM.Unmarshal(metaID, txRaw[n:])
	if err != nil {
		return memdb.ID{}, 0, err
	}
	meta := metaAny.(*wire.TxMetadata)

	return meta.ID, checksum, storeTx(m, metaM, meta.EntityMetadataID, byID, tx, txRaw[n+metaSize:], updatedTypes)
}

// InitReader reads log from stream.
type InitReader interface {
	ReceiveRawBytes() ([]byte, uint64, error)
}

// ApplyInit stores objects from init in database.
func ApplyInit(
	r InitReader,
	m proton.Marshaller,
	metaM proton.Marshaller,
	byID map[uint64]TypeInfo,
	initSize uint64,
	tx *memdb.Txn,
	updatedTypes map[reflect.Type]struct{},
) (uint64, error) {
	// +1 here is for size of the checksum.
	checksumSize := uint64(format.ChecksumSize + 1)
	if initSize <= checksumSize {
		return 0, errors.New("invalid init size")
	}

	initSize -= checksumSize

	initMetaMsgID, err := metaM.ID(&wire.InitMetadata{})
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

	initMetaMsg, _, err := metaM.Unmarshal(initMetaMsgID, headerRaw[n:])
	if err != nil {
		return 0, err
	}

	initMeta := initMetaMsg.(*wire.InitMetadata)

	hasher := xxh3.NewSeed(initMeta.PreviousChecksum)
	if _, err := hasher.Write(headerRaw); err != nil {
		return 0, errors.WithStack(err)
	}

	for size < initSize {
		objRaw, _, err := r.ReceiveRawBytes()
		if err != nil {
			return 0, err
		}

		size += uint64(len(objRaw))
		if size > initSize {
			return 0, errors.New("invalid size")
		}

		if _, err := hasher.Write(objRaw); err != nil {
			return 0, errors.WithStack(err)
		}

		if err := storeObject(m, metaM, initMeta.EntityMetadataID, byID, tx, objRaw, updatedTypes); err != nil {
			return 0, err
		}
	}

	checksumRaw, _, err := r.ReceiveRawBytes()
	if err != nil {
		return 0, err
	}
	if uint64(len(checksumRaw)) != checksumSize {
		return 0, errors.New("invalid size")
	}

	checksum := hasher.Sum64()
	expectedChecksum := binary.LittleEndian.Uint64(checksumRaw[1:])

	if checksum != expectedChecksum {
		return 0, errors.Errorf("invalid init checksum, expected: %d, got: %d", expectedChecksum, checksum)
	}

	return initMeta.PreviousChecksum, nil
}

// Insert inserts object to DB.
func Insert(
	tx *memdb.Txn,
	o any,
	byType map[reflect.Type]TypeInfo,
) (
	memdb.ID,
	TypeInfo,
	unsafe.Pointer,
	unsafe.Pointer,
) {
	oValue := reflect.ValueOf(o)
	if oValue.Kind() == reflect.Pointer {
		panic(errors.New("object must not be a pointer"))
	}

	oType := oValue.Type()
	typeDef, exists := byType[oType]
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

func storeTx(
	m proton.Marshaller,
	metaM proton.Marshaller,
	entityMetaID uint64,
	byID map[uint64]TypeInfo,
	tx *memdb.Txn,
	txRaw []byte,
	updatedTypes map[reflect.Type]struct{},
) error {
	dbTx := tx.Txn(true)
	for len(txRaw) > 0 {
		entityMetaRaw, entityMetaSize, err := metaM.Unmarshal(entityMetaID, txRaw)
		if err != nil {
			return err
		}

		entityMeta := entityMetaRaw.(*wire.TxEntityMetadata)

		typeDef, exists := byID[entityMeta.MessageID]
		if !exists {
			return errors.Errorf("unknown type for message ID %d", entityMeta.MessageID)
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

		msgSize, err := m.ApplyPatch(oV.Interface(), txRaw[entityMetaSize:])
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

func storeObject(
	m proton.Marshaller,
	metaM proton.Marshaller,
	entityMetaID uint64,
	byID map[uint64]TypeInfo,
	tx *memdb.Txn,
	objRaw []byte,
	updatedTypes map[reflect.Type]struct{},
) error {
	_, n := varuint64.Parse(objRaw)

	objRaw = objRaw[n:]

	entityMetaRaw, entityMetaSize, err := metaM.Unmarshal(entityMetaID, objRaw)
	if err != nil {
		return err
	}

	entityMeta := entityMetaRaw.(*wire.InitEntityMetadata)

	typeDef, exists := byID[entityMeta.MessageID]
	if !exists {
		return errors.Errorf("unknown type %s", typeDef.Type)
	}

	msg, _, err := m.Unmarshal(entityMeta.MessageID, objRaw[entityMetaSize:])
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
