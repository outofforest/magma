package internal

import (
	"reflect"

	"github.com/pkg/errors"

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
