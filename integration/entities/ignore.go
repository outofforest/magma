package entities

import (
	"github.com/outofforest/magma/types"
	"github.com/outofforest/memdb"
)

// Ignore is used to send transactions with ignored field.
type Ignore struct {
	ID       memdb.ID
	Revision types.Revision

	Ignored int64
	Value   int64
}
