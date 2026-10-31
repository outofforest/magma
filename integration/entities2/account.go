package entities2

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
	Name      string
	FirstName string
	LastName  string
}
