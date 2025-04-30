package entities

import "github.com/outofforest/magma/types"

// AccountID represents account id.
type AccountID types.ID

// Account represents user account in the system.
type Account struct {
	ID        AccountID
	Revision  types.Revision
	FirstName string
	LastName  string
}
