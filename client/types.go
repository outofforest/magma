package client

import "context"

// ReadClient defines read-only client.
type ReadClient interface {
	View() *View
	WarmUp(ctx context.Context) error
}

// WriteClient defines read & write client.
type WriteClient interface {
	ReadClient
	NewTransactor() Transactor
}
