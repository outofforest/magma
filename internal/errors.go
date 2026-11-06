package internal

import "github.com/pkg/errors"

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

// RecoveryHandler is used to recover from panics.
func RecoveryHandler(err *error) {
	if r := recover(); r != nil {
		if e, ok := r.(error); ok {
			*err = e
			return
		}
		*err = errors.Errorf("function panicked: %s", r)
	}
}
