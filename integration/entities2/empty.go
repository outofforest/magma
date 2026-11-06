package entities2

import (
	"github.com/outofforest/magma/types"
	"github.com/outofforest/memdb"
)

// Empty entity contains no fields despite ID and Revision.
type Empty struct {
	ID       memdb.ID
	Revision types.Revision
}
