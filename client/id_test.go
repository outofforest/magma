package client

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/client/wire"
	db2 "github.com/outofforest/magma/db"
	"github.com/outofforest/magma/integration/entities"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/memdb"
	"github.com/outofforest/qa"
)

func TestMetaLayout(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	metaT := reflect.TypeFor[wire.TxEntityMetadata]()

	idF, exists := metaT.FieldByName("ID")
	requireT.True(exists)
	requireT.Equal([]int{0}, idF.Index)
	requireT.EqualValues(0, idF.Offset)
	requireT.EqualValues(memdb.IDLength, idF.Type.Size())

	revisionF, exists := metaT.FieldByName("Revision")
	requireT.True(exists)
	requireT.Equal([]int{1}, revisionF.Index)
	requireT.EqualValues(memdb.IDLength, revisionF.Offset)
	requireT.EqualValues(types.RevisionLength, revisionF.Type.Size())
}

func TestIDIndex(t *testing.T) {
	t.Parallel()

	ctx := qa.NewContext(t)

	requireT := require.New(t)

	c := NewTestClient(t, cfg, 0, nil)

	id0 := entities.AccountID{0x02}
	id1 := entities.AccountID{0x01}

	accs := []entities.Account{
		{ID: id0},
		{ID: id1},
	}

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db2.Tx) error {
		for _, acc := range accs {
			requireT.NoError(tx.Set(acc))
		}
		return nil
	}))

	for i := range accs {
		accs[i].Revision = 1
	}

	v := c.View()
	acc, exists := db2.Get[entities.Account](v, id0)
	requireT.True(exists)
	requireT.Equal(accs[0], acc)

	accP, exists := db2.GetPointer[entities.Account](v, id0)
	requireT.True(exists)
	requireT.Equal(accs[0], *accP)

	acc, exists = db2.Get[entities.Account](v, id1)
	requireT.True(exists)
	requireT.Equal(accs[1], acc)

	accP, exists = db2.GetPointer[entities.Account](v, id1)
	requireT.True(exists)
	requireT.Equal(accs[1], *accP)

	_, exists = db2.Get[entities.Account](v, entities.AccountID{0x09})
	requireT.False(exists)

	_, exists = db2.GetPointer[entities.Account](v, entities.AccountID{0x09})
	requireT.False(exists)

	i := 0
	for acc := range db2.All[entities.Account](v) {
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
	for accP := range db2.AllPointers[entities.Account](v) {
		switch i {
		case 0:
			requireT.Equal(accs[1], *accP)
		case 1:
			requireT.Equal(accs[0], *accP)
		default:
			requireT.Fail("wrong index")
		}
		i++
	}

	it := db2.AllIterator[entities.Account](v)
	acc, ok := it()
	requireT.True(ok)
	requireT.Equal(accs[1], acc)
	acc, ok = it()
	requireT.True(ok)
	requireT.Equal(accs[0], acc)
	_, ok = it()
	requireT.False(ok)

	itP := db2.AllIteratorPointers[entities.Account](v)
	accP, ok = itP()
	requireT.True(ok)
	requireT.Equal(accs[1], *accP)
	accP, ok = itP()
	requireT.True(ok)
	requireT.Equal(accs[0], *accP)
	_, ok = itP()
	requireT.False(ok)
}
