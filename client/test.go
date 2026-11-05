package client

import (
	"context"
	"encoding/binary"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"

	"github.com/outofforest/magma/config"
	db2 "github.com/outofforest/magma/db"
	"github.com/outofforest/magma/state/repository/format"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/memdb"
	"github.com/outofforest/proton"
	"github.com/outofforest/varuint64"
)

var (
	_ ReadClient     = &TestClient{}
	_ WriteClient    = &TestClient{}
	_ db2.Transactor = &testTransactor{}
)

const maxMsgSize = 4 * 1024

// NewTestConfig creates new config for test client.
func NewTestConfig(marshaller proton.Marshaller, indices ...memdb.Index) Config {
	return Config{
		Service:        "test",
		MaxMessageSize: maxMsgSize,
		Partition: config.PartitionConfig{
			InitialMarshaller: marshaller,
		},
		Indices:       indices,
		PeerAddresses: []string{""},
	}
}

// TestClient is the client wrapper used in unit tests.
type TestClient struct {
	client       *Client
	updatedTypes map[reflect.Type]struct{}
	w            io.Writer
}

// NewTestClient creates new client for tests.
func NewTestClient(t *testing.T, config Config, previousChecksum uint64, w io.Writer) *TestClient {
	client, err := New(config)
	require.NoError(t, err)

	client.previousChecksum = previousChecksum

	return &TestClient{
		client:       client,
		updatedTypes: map[reflect.Type]struct{}{},
		w:            w,
	}
}

// WarmUp is a noop in test client.
func (tc *TestClient) WarmUp(ctx context.Context) error {
	return errors.WithStack(ctx.Err())
}

// View returns current view.
func (tc *TestClient) View() *db2.View {
	return tc.client.View()
}

// NewTransactor returns new transactor.
func (tc *TestClient) NewTransactor() db2.Transactor {
	return &testTransactor{
		tc:           tc,
		updatedTypes: tc.updatedTypes,
	}
}

// Checksum returns current checksum of received transaction log.
func (tc *TestClient) Checksum() uint64 {
	return tc.client.previousChecksum
}

type testTransactor struct {
	tc           *TestClient
	updatedTypes map[reflect.Type]struct{}
}

func (t *testTransactor) Tx(ctx context.Context, txF func(tx db2.Tx) error) error {
	tx, _, err := t.tc.client.NewTransactor().(*transactor).prepareTx(memdb.ID{}, time.Time{}, txF)
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
	previousChecksum, err := t.tc.client.applyTx(nil, t.tc.client.previousChecksum, txn,
		buf[:size+format.ChecksumSize], t.updatedTypes)
	if err != nil {
		return err
	}

	if t.tc.w != nil {
		_, err := t.tc.w.Write(buf[:size+format.ChecksumSize])
		if err != nil {
			return errors.WithStack(err)
		}
	}

	txn.Commit()
	t.tc.client.previousChecksum = previousChecksum
	t.tc.client.nextIndex += types.Index(len(tx.Tx))

	return nil
}
