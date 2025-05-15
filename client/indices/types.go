package indices

import (
	"reflect"

	"github.com/hashicorp/go-memdb"
)

// Index defines the interface of index.
type Index interface {
	Name() string
	Type() reflect.Type
	NumOfArgs() uint64
	Schema() *memdb.IndexSchema
}

type indexer interface {
	memdb.Indexer
	memdb.SingleIndexer

	IsSizeConstant() bool
	Size(o any) uint64
	PutValue(o any, b []byte) uint64
}
