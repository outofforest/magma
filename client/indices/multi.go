package indices

import (
	"reflect"

	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
)

// NewMultiIndex creates new multiindex.
func NewMultiIndex(subIndices ...Index) *MultiIndex {
	if len(subIndices) == 0 {
		panic(errors.Errorf("no subindices has been provided"))
	}

	t := subIndices[0].Type()

	var numOfArgs uint64
	var name string
	var allowMissingValues bool
	subIndexers := make([]indexer, 0, len(subIndices))
	for _, si := range subIndices {
		if si.Type() != t {
			panic(errors.Errorf("wrong type, expected: %s, got: %s", t, si.Type()))
		}
		numOfArgs += si.NumOfArgs()

		if name != "" {
			name += ","
		}
		name += si.Name()
		schema := si.Schema()
		allowMissingValues = allowMissingValues || schema.AllowMissing
		subIndexers = append(subIndexers, schema.Indexer.(indexer))
	}

	return &MultiIndex{
		name:               name,
		numOfArgs:          numOfArgs,
		entityType:         t,
		allowMissingValues: allowMissingValues,
		indexer: &multiIndexer{
			subIndices:  subIndices,
			subIndexers: subIndexers,
		},
	}
}

// MultiIndex compiles many indices into a single one.
type MultiIndex struct {
	name               string
	numOfArgs          uint64
	entityType         reflect.Type
	indexer            memdb.Indexer
	allowMissingValues bool
}

// Name returns name of the index.
func (i *MultiIndex) Name() string {
	return i.name
}

// Type returns type of entity index is defined for.
func (i *MultiIndex) Type() reflect.Type {
	return i.entityType
}

// NumOfArgs returns number of arguments taken by the index.
func (i *MultiIndex) NumOfArgs() uint64 {
	return i.numOfArgs
}

// Schema returns memdb index schema.
func (i *MultiIndex) Schema() *memdb.IndexSchema {
	return &memdb.IndexSchema{
		Name:         i.name,
		Indexer:      i.indexer,
		AllowMissing: i.allowMissingValues,
	}
}

type multiIndexer struct {
	subIndices  []Index
	subIndexers []indexer
}

func (mi *multiIndexer) FromArgs(args ...any) ([]byte, error) {
	var startArg uint64
	var size int
	subValues := make([][]byte, 0, len(mi.subIndices))
	for i, index := range mi.subIndices {
		if startArg >= uint64(len(args)) {
			break
		}
		numOfArgs := index.NumOfArgs()
		var subValue []byte
		var err error
		if startArg+numOfArgs > uint64(len(args)) {
			subValue, err = mi.subIndexers[i].FromArgs(args[startArg:]...)
		} else {
			subValue, err = mi.subIndexers[i].FromArgs(args[startArg : startArg+numOfArgs]...)
		}
		if err != nil {
			return nil, err
		}
		startArg += numOfArgs
		size += len(subValue)
		subValues = append(subValues, subValue)
	}
	value := make([]byte, 0, size)
	for _, subValue := range subValues {
		value = append(value, subValue...)
	}
	return value, nil
}

func (mi *multiIndexer) FromObject(o any) (bool, []byte, error) {
	var size int
	subValues := make([][]byte, 0, len(mi.subIndexers))
	for _, si := range mi.subIndexers {
		ok, b, err := si.FromObject(o)
		if err != nil || !ok {
			return ok, nil, err
		}
		size += len(b)
		subValues = append(subValues, b)
	}
	value := make([]byte, 0, size)
	for _, subValue := range subValues {
		value = append(value, subValue...)
	}
	return true, value, nil
}
