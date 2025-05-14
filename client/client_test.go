package client

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"

	"github.com/outofforest/magma/client/indices"
	"github.com/outofforest/magma/integration/entities"
	"github.com/outofforest/magma/state/repository/format"
	"github.com/outofforest/varuint64"
)

func TestNewEntities(t *testing.T) {
	requireT := require.New(t)
	c := newTestClient(t)

	acc1 := entities.Account{
		ID:        NewID[entities.AccountID](),
		FirstName: "First1",
		LastName:  "Last1",
	}
	acc2 := entities.Account{
		ID:        NewID[entities.AccountID](),
		FirstName: "First2",
		LastName:  "Last2",
	}

	c.Tx(func(tx *Tx) error {
		_, exists := Get[entities.Account](tx.View, acc1.ID)
		requireT.False(exists)

		tx.Set(acc1)

		acc, exists := Get[entities.Account](tx.View, acc1.ID)
		requireT.True(exists)
		requireT.Equal(acc1, acc)

		return nil
	})

	acc1.Revision = 1

	c.Tx(func(tx *Tx) error {
		acc, exists := Get[entities.Account](tx.View, acc1.ID)
		requireT.True(exists)
		requireT.Equal(acc1, acc)

		tx.Set(acc2)

		acc, exists = Get[entities.Account](tx.View, acc2.ID)
		requireT.True(exists)
		requireT.Equal(acc2, acc)

		return nil
	})

	acc2.Revision = 1
	v := c.View()

	acc, exists := Get[entities.Account](v, acc1.ID)
	requireT.True(exists)
	requireT.Equal(acc, acc1)

	acc, exists = Get[entities.Account](v, acc2.ID)
	requireT.True(exists)
	requireT.Equal(acc, acc2)
}

const maxMsgSize = 4 * 1024

func newTestClient(t *testing.T, indices ...indices.Index) testClient {
	requireT := require.New(t)
	client, err := New(Config{
		Service:        "test",
		MaxMessageSize: maxMsgSize,
		Marshaller:     entities.NewMarshaller(),
		Indices:        indices,
	})
	requireT.NoError(err)

	return testClient{
		requireT: requireT,
		client:   client,
	}
}

type testClient struct {
	requireT *require.Assertions
	client   *Client
}

func (tc testClient) View() *View {
	return tc.client.View()
}

func (tc testClient) Tx(txF func(tx *Tx) error) {
	tx, _, err := tc.client.NewTransactor().prepareTx(txF)
	tc.requireT.NoError(err)

	if tx.Tx == nil {
		return
	}

	_, n := varuint64.Parse(tx.Tx)
	txRaw := tx.Tx[n:]
	size := uint64(len(txRaw)) + format.ChecksumSize
	buf := make([]byte, varuint64.MaxSize+size)
	n = varuint64.Put(buf, size)
	copy(buf[n:], txRaw)

	size = n + uint64(len(txRaw))
	binary.LittleEndian.PutUint64(buf[size:], xxh3.HashSeed(buf[:size], tc.client.previousChecksum))
	tc.requireT.NoError(tc.client.applyTx(buf[:size+format.ChecksumSize]))
}
