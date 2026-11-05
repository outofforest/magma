package client

import (
	"context"

	"github.com/outofforest/magma/client/db"
)

// ReadClient defines read-only client.
type ReadClient interface {
	View() *db.View
	WarmUp(ctx context.Context) error
}

// WriteClient defines read & write client.
type WriteClient interface {
	ReadClient
	NewTransactor() db.Transactor
}
