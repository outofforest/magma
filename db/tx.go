package db

import (
	"context"
)

// Transactor builds and broadcasts transactions.
type Transactor interface {
	Tx(ctx context.Context, txF func(tx Tx) error) error
}

// Tx prepares transaction.
type Tx interface {
	View() *View
	Set(o any) error
	SoftSet(o any) error
}
