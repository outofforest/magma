package client

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/integration/entities"
)

func TestIDIndex(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	c := newTestClient(t)

	id0 := entities.AccountID{0x02}
	id1 := entities.AccountID{0x01}

	accs := []entities.Account{
		{ID: id0},
		{ID: id1},
	}

	requireT.NoError(c.Tx(func(tx *Tx) error {
		for _, acc := range accs {
			tx.Set(acc)
		}
		return nil
	}))

	for i := range accs {
		accs[i].Revision = 1
	}

	v := c.View()
	acc, exists := Get[entities.Account](v, id0)
	requireT.True(exists)
	requireT.Equal(accs[0], acc)

	acc, exists = Get[entities.Account](v, id1)
	requireT.True(exists)
	requireT.Equal(accs[1], acc)

	_, exists = Get[entities.Account](v, entities.AccountID{0x09})
	requireT.False(exists)

	i := 0
	for acc := range All[entities.Account](v) {
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

	it := AllIterator[entities.Account](v)
	acc, ok := it()
	requireT.True(ok)
	requireT.Equal(accs[1], acc)
	acc, ok = it()
	requireT.True(ok)
	requireT.Equal(accs[0], acc)
	_, ok = it()
	requireT.False(ok)
}
