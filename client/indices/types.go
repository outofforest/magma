package indices

import (
	"reflect"

	"github.com/hashicorp/go-memdb"
)

// Index defines the interface of index.
type Index interface {
	Name() string
	Type() reflect.Type
	Schema() *memdb.IndexSchema
}
