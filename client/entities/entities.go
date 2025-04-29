package entities

import "github.com/outofforest/magma/types"

// Account represents user account in the system.
type Account struct {
	ID        types.ID
	Revision  types.Revision
	FirstName string
	LastName  string
}
