package client

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"

	"github.com/outofforest/magma/state/repository/format"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/memdb"
	"github.com/outofforest/proton"
	"github.com/outofforest/varuint64"
)

const maxMsgSize = 4 * 1024

// NewTestConfig creates new config for test client.
func NewTestConfig(marshaller proton.Marshaller, triggerFunc TriggerFunc, indices ...memdb.Index) Config {
	return Config{
		Service:        "test",
		MaxMessageSize: maxMsgSize,
		Marshaller:     marshaller,
		Indices:        indices,
	}
}

// NewTestClient creates new client for tests.
func NewTestClient(t *testing.T, config Config) TestClient {
	client, err := New(config)
	require.NoError(t, err)

	return TestClient{
		client:      client,
		triggerFunc: config.TriggerFunc,
	}
}

// TestClient is the client wrapper used in unit tests.
type TestClient struct {
	client      *Client
	triggerFunc TriggerFunc
}

// WarmUp is a noop in test client.
func (tc TestClient) WarmUp(ctx context.Context) error {
	return errors.WithStack(ctx.Err())
}

// View returns current view.
func (tc TestClient) View() *View {
	return tc.client.View()
}

// NewTransactor returns new transactor.
func (tc TestClient) NewTransactor() Transactor {
	return testTransactor{
		tc: tc,
	}
}

// Trigger triggers trigger function.
func (tc TestClient) Trigger(ctx context.Context) error {
	return tc.triggerFunc(ctx, tc.View())
}

// Checksum returns current checksum of received transaction log.
func (tc TestClient) Checksum() uint64 {
	return tc.client.previousChecksum
}

type testTransactor struct {
	tc TestClient
}

func (t testTransactor) Tx(ctx context.Context, txF func(tx *Tx) error) error {
	tx, _, err := t.tc.client.NewTransactor().(*transactor).prepareTx(txF)
	if err != nil {
		return err
	}

	if tx.Tx == nil {
		return nil
	}

	_, n := varuint64.Parse(tx.Tx)
	txRaw := tx.Tx[n:]
	size := uint64(len(txRaw)) + format.ChecksumSize
	buf := make([]byte, varuint64.MaxSize+size)
	n = varuint64.Put(buf, size)
	copy(buf[n:], txRaw)

	size = n + uint64(len(txRaw))
	binary.LittleEndian.PutUint64(buf[size:], xxh3.HashSeed(buf[:size], t.tc.client.previousChecksum))

	txn := t.tc.client.db.Txn(true)
	previousChecksum, err := t.tc.client.applyTx(nil, t.tc.client.previousChecksum, txn, buf[:size+format.ChecksumSize])
	if err != nil {
		txn.Abort()
		return err
	}
	txn.Commit()
	t.tc.client.previousChecksum = previousChecksum
	t.tc.client.nextIndex += types.Index(len(tx.Tx))
	return nil
}
