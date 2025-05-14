package client

import (
	"encoding/binary"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"

	"github.com/outofforest/magma/client/indices"
	"github.com/outofforest/magma/integration/entities"
	"github.com/outofforest/magma/state/repository/format"
	"github.com/outofforest/varuint64"
)

func TestCreateEntities(t *testing.T) {
	t.Parallel()

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

func TestEmptyTransaction(t *testing.T) {
	t.Parallel()

	newTestClient(t).Tx(func(tx *Tx) error {
		return nil
	})
}

func TestFieldIndexString(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	var acc entities.Account
	indexLastName := indices.NewFieldIndex("lastName", &acc, &acc.LastName)

	c := newTestClient(t, indexLastName)

	accs := []entities.Account{
		{
			ID:        NewID[entities.AccountID](),
			FirstName: "First2",
			LastName:  "Last2",
		},
		{
			ID:        NewID[entities.AccountID](),
			FirstName: "First1",
			LastName:  "Last1",
		},
	}

	c.Tx(func(tx *Tx) error {
		for _, acc := range accs {
			tx.Set(acc)
		}
		return nil
	})

	for i := range accs {
		accs[i].Revision = 1
	}

	v := c.View()
	acc, exists := Find[entities.Account](v, indexLastName)
	requireT.True(exists)
	requireT.Equal(accs[1], acc)

	acc, exists = Find[entities.Account](v, indexLastName, "Last2")
	requireT.True(exists)
	requireT.Equal(accs[0], acc)

	_, exists = Find[entities.Account](v, indexLastName, "La")
	requireT.False(exists)

	i := 0
	for acc := range Iterate[entities.Account](v, indexLastName) {
		switch i {
		case 0:
			requireT.Equal(accs[1], acc)
		case 1:
			requireT.Equal(accs[0], acc)
		default:
			requireT.Fail("wrong index")
		}
		i++
	}

	i = 0
	for acc := range Iterate[entities.Account](v, indexLastName, "Last1") {
		switch i {
		case 0:
			requireT.Equal(accs[1], acc)
		default:
			requireT.Fail("wrong index")
		}
		i++
	}

	for range Iterate[entities.Account](v, indexLastName, "Missing") {
		requireT.Fail("nothing should be returned")
	}

	it := Iterator[entities.Account](v, indexLastName)
	acc, ok := it()
	requireT.True(ok)
	requireT.Equal(accs[1], acc)
	acc, ok = it()
	requireT.True(ok)
	requireT.Equal(accs[0], acc)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Account](v, indexLastName, "Last2")
	acc, ok = it()
	requireT.True(ok)
	requireT.Equal(accs[0], acc)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Account](v, indexLastName, "Missing")
	_, ok = it()
	requireT.False(ok)
}

func TestIfIndex(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	var acc entities.Account
	indexLastName := indices.NewIfIndex[entities.Account](
		"if", indices.NewFieldIndex("lastName", &acc, &acc.LastName),
		func(acc *entities.Account) bool {
			return acc.FirstName == "First1"
		},
	)

	c := newTestClient(t, indexLastName)

	accs := []entities.Account{
		{
			ID:        NewID[entities.AccountID](),
			FirstName: "First1",
			LastName:  "Last3",
		},
		{
			ID:        NewID[entities.AccountID](),
			FirstName: "First2",
			LastName:  "Last2",
		},
		{
			ID:        NewID[entities.AccountID](),
			FirstName: "First1",
			LastName:  "Last2",
		},
	}

	c.Tx(func(tx *Tx) error {
		for _, acc := range accs {
			tx.Set(acc)
		}
		return nil
	})

	for i := range accs {
		accs[i].Revision = 1
	}

	v := c.View()
	acc, exists := Find[entities.Account](v, indexLastName)
	requireT.True(exists)
	requireT.Equal(accs[2], acc)

	acc, exists = Find[entities.Account](v, indexLastName, "Last2")
	requireT.True(exists)
	requireT.Equal(accs[2], acc)

	_, exists = Find[entities.Account](v, indexLastName, "Missing")
	requireT.False(exists)

	i := 0
	for acc := range Iterate[entities.Account](v, indexLastName) {
		switch i {
		case 0:
			requireT.Equal(accs[2], acc)
		case 1:
			requireT.Equal(accs[0], acc)
		default:
			requireT.Fail("wrong index")
		}
		i++
	}

	i = 0
	for acc := range Iterate[entities.Account](v, indexLastName, "Last3") {
		switch i {
		case 0:
			requireT.Equal(accs[0], acc)
		default:
			requireT.Fail("wrong index")
		}
		i++
	}

	for range Iterate[entities.Account](v, indexLastName, "Missing") {
		requireT.Fail("nothing should be returned")
	}

	it := Iterator[entities.Account](v, indexLastName)
	acc, ok := it()
	requireT.True(ok)
	requireT.Equal(accs[2], acc)
	acc, ok = it()
	requireT.True(ok)
	requireT.Equal(accs[0], acc)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Account](v, indexLastName, "Last2")
	acc, ok = it()
	requireT.True(ok)
	requireT.Equal(accs[2], acc)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Account](v, indexLastName, "Missing")
	_, ok = it()
	requireT.False(ok)
}

func TestMultiIndex(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	var acc entities.Account
	indexName := indices.NewMultiIndex(
		indices.NewFieldIndex("lastName", &acc, &acc.LastName),
		indices.NewFieldIndex("firstName", &acc, &acc.FirstName),
	)

	c := newTestClient(t, indexName)

	accs := []entities.Account{
		{
			ID:        NewID[entities.AccountID](),
			FirstName: "First3",
			LastName:  "Last2",
		},
		{
			ID:        NewID[entities.AccountID](),
			FirstName: "First2",
			LastName:  "Last2",
		},
		{
			ID:        NewID[entities.AccountID](),
			FirstName: "First1",
			LastName:  "Last1",
		},
		{
			ID:        NewID[entities.AccountID](),
			FirstName: "First",
			LastName:  "Last",
		},
		{
			ID:        NewID[entities.AccountID](),
			FirstName: "st",
			LastName:  "La",
		},
	}

	c.Tx(func(tx *Tx) error {
		for _, acc := range accs {
			tx.Set(acc)
		}
		return nil
	})

	for i := range accs {
		accs[i].Revision = 1
	}

	v := c.View()
	acc, exists := Find[entities.Account](v, indexName)
	requireT.True(exists)
	requireT.Equal(accs[4], acc)

	acc, exists = Find[entities.Account](v, indexName, "Last")
	requireT.True(exists)
	requireT.Equal(accs[3], acc)

	acc, exists = Find[entities.Account](v, indexName, "La")
	requireT.True(exists)
	requireT.Equal(accs[4], acc)

	_, exists = Find[entities.Account](v, indexName, "Las")
	requireT.False(exists)

	acc, exists = Find[entities.Account](v, indexName, "Last2", "First3")
	requireT.True(exists)
	requireT.Equal(accs[0], acc)

	_, exists = Find[entities.Account](v, indexName, "Last2", "Fir")
	requireT.False(exists)

	i := 0
	for acc := range Iterate[entities.Account](v, indexName) {
		switch i {
		case 0:
			requireT.Equal(accs[4], acc)
		case 1:
			requireT.Equal(accs[3], acc)
		case 2:
			requireT.Equal(accs[2], acc)
		case 3:
			requireT.Equal(accs[1], acc)
		case 4:
			requireT.Equal(accs[0], acc)
		default:
			requireT.Fail("wrong index")
		}
		i++
	}

	i = 0
	for acc := range Iterate[entities.Account](v, indexName, "Last2") {
		switch i {
		case 0:
			requireT.Equal(accs[1], acc)
		case 1:
			requireT.Equal(accs[0], acc)
		default:
			requireT.Fail("wrong index")
		}
		i++
	}

	i = 0
	for acc := range Iterate[entities.Account](v, indexName, "Last2", "First2") {
		switch i {
		case 0:
			requireT.Equal(accs[1], acc)
		default:
			requireT.Fail("wrong index")
		}
		i++
	}

	for range Iterate[entities.Account](v, indexName, "Las") {
		requireT.Fail("nothing should be returned")
	}

	for range Iterate[entities.Account](v, indexName, "Last2", "Fir") {
		requireT.Fail("nothing should be returned")
	}

	it := Iterator[entities.Account](v, indexName)
	acc, ok := it()
	requireT.True(ok)
	requireT.Equal(accs[4], acc)
	acc, ok = it()
	requireT.True(ok)
	requireT.Equal(accs[3], acc)
	acc, ok = it()
	requireT.True(ok)
	requireT.Equal(accs[2], acc)
	acc, ok = it()
	requireT.True(ok)
	requireT.Equal(accs[1], acc)
	acc, ok = it()
	requireT.True(ok)
	requireT.Equal(accs[0], acc)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Account](v, indexName, "Last2")
	acc, ok = it()
	requireT.True(ok)
	requireT.Equal(accs[1], acc)
	acc, ok = it()
	requireT.True(ok)
	requireT.Equal(accs[0], acc)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Account](v, indexName, "Last2", "First3")
	acc, ok = it()
	requireT.True(ok)
	requireT.Equal(accs[0], acc)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Account](v, indexName, "Las")
	_, ok = it()
	requireT.False(ok)
}

func TestMultiIfIndex(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	var acc entities.Account
	indexName := indices.NewMultiIndex(
		indices.NewIfIndex[entities.Account]("ifLast",
			indices.NewFieldIndex("lastName", &acc, &acc.LastName), func(e *entities.Account) bool {
				return strings.HasPrefix(e.LastName, "A")
			}),
		indices.NewIfIndex[entities.Account]("ifFirst",
			indices.NewFieldIndex("firstName", &acc, &acc.FirstName), func(e *entities.Account) bool {
				return strings.HasPrefix(e.FirstName, "B")
			}),
	)

	c := newTestClient(t, indexName)

	accs := []entities.Account{
		{
			ID:        NewID[entities.AccountID](),
			FirstName: "C1",
			LastName:  "A1",
		},
		{
			ID:        NewID[entities.AccountID](),
			FirstName: "B2",
			LastName:  "C2",
		},
		{
			ID:        NewID[entities.AccountID](),
			FirstName: "B1",
			LastName:  "A1",
		},
		{
			ID:        NewID[entities.AccountID](),
			FirstName: "B2",
			LastName:  "A2",
		},
	}

	c.Tx(func(tx *Tx) error {
		for _, acc := range accs {
			tx.Set(acc)
		}
		return nil
	})

	for i := range accs {
		accs[i].Revision = 1
	}

	v := c.View()

	it := Iterator[entities.Account](v, indexName)
	acc, ok := it()
	requireT.True(ok)
	requireT.Equal(accs[2], acc)
	acc, ok = it()
	requireT.True(ok)
	requireT.Equal(accs[3], acc)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Account](v, indexName, "A1", "B1")
	acc, ok = it()
	requireT.True(ok)
	requireT.Equal(accs[2], acc)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Account](v, indexName, "A2")
	acc, ok = it()
	requireT.True(ok)
	requireT.Equal(accs[3], acc)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Account](v, indexName, "B1", "A1")
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Account](v, indexName, "B1")
	_, ok = it()
	requireT.False(ok)
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
