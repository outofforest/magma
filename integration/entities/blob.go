package entities

import (
	"github.com/outofforest/magma/types"
	"github.com/outofforest/memdb"
)

// Blob is used to send big transactions.
type Blob struct {
	ID       memdb.ID
	Revision types.Revision
	Data     []byte
}
