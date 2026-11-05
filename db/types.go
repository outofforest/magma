package db

import (
	"context"

	"github.com/outofforest/magma/internal"
	"github.com/outofforest/proton"
)

var (
	// ErrTxTooBig means that adding new entity to transaction caused it to exceed the max size limit.
	ErrTxTooBig = internal.ErrTxTooBig

	// ErrTxFailure is a general error meaning that transaction failed.
	ErrTxFailure = internal.ErrTxFailure

	// ErrTxTimeout means timeout has occurred when sending transaction.
	ErrTxTimeout = internal.ErrTxTimeout

	// ErrTxBroadcastTimeout means that client was not able to broadcast the transaction before timeout.
	ErrTxBroadcastTimeout = internal.ErrTxBroadcastTimeout

	// ErrTxAwaitTimeout means that client hasn't received broadcasted transaction before timeout.
	ErrTxAwaitTimeout = internal.ErrTxAwaitTimeout

	// ErrTxOutdatedTx means that awaited transaction is outdated and hasn't been applied.
	ErrTxOutdatedTx = internal.ErrTxOutdatedTx
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

// UpgradeFunc defines the function for db conversion.
type UpgradeFunc func(view *View, ch chan<- any) error

// Upgrade defines db upgrade.
type Upgrade struct {
	Marshaller  proton.Marshaller
	UpgradeFunc UpgradeFunc
}
