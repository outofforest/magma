package internal

import (
	"reflect"

	"github.com/pkg/errors"

	"github.com/outofforest/magma/types"
	"github.com/outofforest/memdb"
	"github.com/outofforest/proton"
)

// NewDBConfig creates db config.
func NewDBConfig(entities []reflect.Type, indices []memdb.Index) (memdb.Config, error) {
	return memdb.Config{
		Entities: entities,
		Indices:  indices,
	}, nil
}

// EntitiesFromMarshaller gets entity types from marshaller.
func EntitiesFromMarshaller(m proton.Marshaller) ([]reflect.Type, error) {
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

// TypeInfo caches information about entity type.
type TypeInfo struct {
	Type    reflect.Type
	IDType  reflect.Type
	MsgID   uint64
	TableID uint64
}

// NewByTypeCache creates new type cache indexed by entity types.
func NewByTypeCache(entities []reflect.Type) (map[reflect.Type]TypeInfo, error) {
	byType := map[reflect.Type]TypeInfo{}
	for tableID, t := range entities {
		idFType, err := validateEntityType(t, false)
		if err != nil {
			return nil, err
		}

		if _, exists := byType[t]; exists {
			return nil, errors.Errorf("double registration of object %s", t)
		}

		byType[t] = TypeInfo{
			Type:    t,
			IDType:  idFType,
			TableID: uint64(tableID),
		}
	}

	return byType, nil
}

// NewCache creates new type caches from marshaller.
func NewCache(m proton.Marshaller) (map[uint64]TypeInfo, map[reflect.Type]TypeInfo, error) {
	entities, err := EntitiesFromMarshaller(m)
	if err != nil {
		return nil, nil, err
	}

	byID := map[uint64]TypeInfo{}
	byType := map[reflect.Type]TypeInfo{}
	for tableID, t := range entities {
		idFType, err := validateEntityType(t, true)
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

func validateEntityType(t reflect.Type, isRevisionRequired bool) (reflect.Type, error) {
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

	if isRevisionRequired {
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
	}

	return idF.Type, nil
}
