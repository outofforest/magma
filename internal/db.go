package internal

import (
	"reflect"

	"github.com/pkg/errors"

	"github.com/outofforest/memdb"
	"github.com/outofforest/proton"
)

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
