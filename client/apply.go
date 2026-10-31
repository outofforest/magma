package client

import (
	"encoding/binary"
	"reflect"

	"github.com/pkg/errors"
	"github.com/zeebo/xxh3"

	"github.com/outofforest/magma/client/wire"
	"github.com/outofforest/magma/state/repository/format"
	"github.com/outofforest/memdb"
	"github.com/outofforest/proton"
	"github.com/outofforest/varuint64"
)

// Applier stores transaction in database.
type Applier struct {
	metaM  proton.Marshaller
	m      proton.Marshaller
	byID   map[uint64]typeInfo
	byType map[reflect.Type]typeInfo
}

// NewApplier creates applier.
func NewApplier(m proton.Marshaller) (*Applier, error) {
	entities, err := typesFromMarshaller(m)
	if err != nil {
		return nil, err
	}

	byID := map[uint64]typeInfo{}
	byType := map[reflect.Type]typeInfo{}
	for tableID, t := range entities {
		idFType, err := validateType(t, true)
		if err != nil {
			return nil, err
		}

		if _, exists := byType[t]; exists {
			return nil, errors.Errorf("double registration of object %s", t)
		}

		msgID, err := m.ID(reflect.New(t).Interface())
		if err != nil {
			return nil, err
		}

		info := typeInfo{
			Type:    t,
			IDType:  idFType,
			MsgID:   msgID,
			TableID: uint64(tableID),
		}
		byID[msgID] = info
		byType[t] = info
	}

	return &Applier{
		metaM:  wire.NewMarshaller(),
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

		entityMeta := entityMetaRaw.(*wire.EntityMetadata)

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
			copyMetaToEntity(oV.UnsafePointer(), entityMeta)
		} else {
			if entityMeta.Revision <= revisionFromEntity(pointer) {
				return ErrTxOutdatedTx
			}
			oV.Elem().Set(reflect.NewAt(typeDef.Type, pointer).Elem())
			setRevisionInEntity(oV.UnsafePointer(), &entityMeta.Revision)
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

type typeInfo struct {
	Type    reflect.Type
	IDType  reflect.Type
	MsgID   uint64
	TableID uint64
}
