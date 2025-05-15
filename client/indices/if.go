package indices

import (
	"reflect"

	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
)

// NewIfIndex creates new conditional index.
func NewIfIndex[T any](name string, subIndex Index, f func(o *T) bool) *IfIndex[T] {
	var v T
	if t := reflect.TypeOf(v); t != subIndex.Type() {
		panic(errors.Errorf("subindex type mismatch, expected: %s, got: %s", t, subIndex.Type()))
	}

	return &IfIndex[T]{
		name:     subIndex.Name() + "," + name,
		subIndex: subIndex,
		indexer:  newSubIndexer(subIndex, f),
	}
}

func newSubIndexer[T any](subIndex Index, f func(o *T) bool) memdb.Indexer {
	subIndexer := subIndex.Schema().Indexer
	return ifIndexer[T]{
		subIndexer:       subIndexer,
		singleSubIndexer: subIndexer.(memdb.SingleIndexer),
		f:                f,
	}
}

// IfIndex indexes those elements from another index for which f returns true.
type IfIndex[T any] struct {
	name     string
	subIndex Index
	indexer  memdb.Indexer
}

// Name returns name of the index.
func (i *IfIndex[T]) Name() string {
	return i.name
}

// Type returns type of entity index is defined for.
func (i *IfIndex[T]) Type() reflect.Type {
	return i.subIndex.Type()
}

// NumOfArgs returns number of arguments taken by the index.
func (i *IfIndex[T]) NumOfArgs() uint64 {
	return i.subIndex.NumOfArgs()
}

// Schema returns memdb index schema.
func (i *IfIndex[T]) Schema() *memdb.IndexSchema {
	return &memdb.IndexSchema{
		Name:         i.name,
		AllowMissing: true,
		Indexer:      i.indexer,
	}
}

type ifIndexer[T any] struct {
	subIndexer       memdb.Indexer
	singleSubIndexer memdb.SingleIndexer
	f                func(o *T) bool
}

func (ii ifIndexer[T]) FromArgs(args ...any) ([]byte, error) {
	return ii.subIndexer.FromArgs(args...)
}

func (ii ifIndexer[T]) FromObject(o any) (bool, []byte, error) {
	if !ii.f(o.(reflect.Value).Interface().(*T)) {
		return false, nil, nil
	}
	return ii.singleSubIndexer.FromObject(o)
}
