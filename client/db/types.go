package db

import (
	"context"

	"github.com/pkg/errors"

	"github.com/outofforest/proton"
)

var (
	// ErrTxTooBig means that adding new entity to transaction caused it to exceed the max size limit.
	ErrTxTooBig = errors.New("transaction exceeds the max size limit")

	// ErrTxFailure is a general error meaning that transaction failed.
	ErrTxFailure = errors.New("transaction failed")

	// ErrTxTimeout means timeout has occurred when sending transaction.
	ErrTxTimeout = errors.Wrap(ErrTxFailure, "transaction timeout")

	// ErrTxBroadcastTimeout means that client was not able to broadcast the transaction before timeout.
	ErrTxBroadcastTimeout = errors.Wrap(ErrTxTimeout, "broadcast timeout")

	// ErrTxAwaitTimeout means that client hasn't received broadcasted transaction before timeout.
	ErrTxAwaitTimeout = errors.Wrap(ErrTxTimeout, "await timeout")

	// ErrTxOutdatedTx means that awaited transaction is outdated and hasn't been applied.
	ErrTxOutdatedTx = errors.Wrap(ErrTxFailure, "outdated transaction")
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
