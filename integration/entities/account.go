package entities

import (
	"github.com/outofforest/magma/types"
	"github.com/outofforest/memdb"
)

// AccountID represents account id.
type AccountID memdb.ID

// Account represents user account in the system.
type Account struct {
	ID        AccountID
	Revision  types.Revision
	FirstName string
	LastName  string
}
