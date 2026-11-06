package db

import "github.com/outofforest/magma/internal"

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
