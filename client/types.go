package client

import (
	"context"

	db2 "github.com/outofforest/magma/db"
)

// ReadClient defines read-only client.
type ReadClient interface {
	View() *db2.View
	WarmUp(ctx context.Context) error
}

// WriteClient defines read & write client.
type WriteClient interface {
	ReadClient
	NewTransactor() db2.Transactor
}
