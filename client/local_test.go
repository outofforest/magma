package client

import (
	"context"
	"reflect"
	"testing"

	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	magmatypes "github.com/outofforest/magma/types"
	"github.com/outofforest/memdb"
	"github.com/outofforest/memdb/indices"
	"github.com/outofforest/qa"
)

type (
	staticID1 memdb.ID
	staticID2 memdb.ID

	localEntity1 struct {
		ID       staticID1
		Revision magmatypes.Revision
		Value    string
	}

	localEntity2 struct {
		ID       staticID2
		Revision magmatypes.Revision
		Value    uint64
	}

	localInvalid struct {
		ID    staticID1
		Value uint64
	}
)

var (
	e1 = localEntity1{
		ID:    memdb.NewID[staticID1](),
		Value: "str1",
	}
	e2 = localEntity1{
		ID:    memdb.NewID[staticID1](),
		Value: "str2",
	}
	e3 = localEntity2{
		ID:    memdb.NewID[staticID2](),
		Value: 3,
	}
	e4 = localEntity2{
		ID:    memdb.NewID[staticID2](),
		Value: 4,
	}
)

func TestLocalClient(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	ctx := qa.NewContext(t)

	c, err := NewLocalClient(LocalConfig{
		Types: []reflect.Type{
			reflect.TypeFor[localEntity1](),
			reflect.TypeFor[localEntity2](),
		},
	})
	requireT.NoError(err)
	requireT.NoError(c.WarmUp(ctx))
	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx Tx) error {
		for _, e := range []any{e1, e2, e3, e4} {
			if err := tx.Set(e); err != nil {
				return err
			}
		}
		return nil
	}))

	v := c.View()
	eDB1, exists := Get[localEntity1](v, e1.ID)
	requireT.True(exists)
	requireT.Equal(e1, eDB1)

	eDB1, exists = Get[localEntity1](v, e2.ID)
	requireT.True(exists)
	requireT.Equal(e2, eDB1)

	eDB2, exists := Get[localEntity2](v, e3.ID)
	requireT.True(exists)
	requireT.Equal(e3, eDB2)

	eDB2, exists = Get[localEntity2](v, e4.ID)
	requireT.True(exists)
	requireT.Equal(e4, eDB2)
}

func TestLocalClientTrigger(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	ctx := qa.NewContext(t)

	var triggered bool
	c, err := NewLocalClient(LocalConfig{
		Types: []reflect.Type{
			reflect.TypeFor[localEntity1](),
			reflect.TypeFor[localEntity2](),
		},
		TriggerFunc: func(ctx context.Context, v *View, ids map[any]struct{}) error {
			triggered = true
			return nil
		},
	})
	requireT.NoError(err)
	requireT.NoError(c.WarmUp(ctx))

	requireT.False(triggered)

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx Tx) error {
		for _, e := range []any{e1, e2, e3, e4} {
			if err := tx.Set(e); err != nil {
				return err
			}
		}
		return nil
	}))

	requireT.True(triggered)
}

func TestLocalClientTriggerReceivesIDs(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	ctx := qa.NewContext(t)

	var receivedIDs map[any]struct{}
	c, err := NewLocalClient(LocalConfig{
		Types: []reflect.Type{
			reflect.TypeFor[localEntity1](),
			reflect.TypeFor[localEntity2](),
		},
		TriggerFunc: func(ctx context.Context, v *View, ids map[any]struct{}) error {
			receivedIDs = ids
			return nil
		},
	})
	requireT.NoError(err)
	requireT.NoError(c.WarmUp(ctx))

	requireT.Nil(receivedIDs)

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx Tx) error {
		for _, e := range []any{e1, e2, e3, e4} {
			if err := tx.Set(e); err != nil {
				return err
			}
		}
		return nil
	}))

	requireT.ElementsMatch([]any{
		e1.ID,
		e2.ID,
		e3.ID,
		e4.ID,
	}, lo.Keys(receivedIDs))
}

func TestLocalClientTriggerErr(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	ctx := qa.NewContext(t)

	c, err := NewLocalClient(LocalConfig{
		Types: []reflect.Type{
			reflect.TypeFor[localEntity1](),
			reflect.TypeFor[localEntity2](),
		},
		TriggerFunc: func(ctx context.Context, v *View, ids map[any]struct{}) error {
			return errors.New("test")
		},
	})
	requireT.NoError(err)
	requireT.NoError(c.WarmUp(ctx))
	requireT.Error(c.NewTransactor().Tx(ctx, func(tx Tx) error {
		for _, e := range []any{e1, e2, e3, e4} {
			if err := tx.Set(e); err != nil {
				return err
			}
		}
		return nil
	}))
}

func TestLocalClientIndexes(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	ctx := qa.NewContext(t)

	var ie1 localEntity1
	index1 := indices.NewFieldIndex(&ie1, &ie1.Value)
	var ie2 localEntity2
	index2 := indices.NewFieldIndex(&ie2, &ie2.Value)

	c, err := NewLocalClient(LocalConfig{
		Types: []reflect.Type{
			reflect.TypeFor[localEntity1](),
			reflect.TypeFor[localEntity2](),
		},
		Indices: []memdb.Index{index1, index2},
	})
	requireT.NoError(err)
	requireT.NoError(c.WarmUp(ctx))
	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx Tx) error {
		for _, e := range []any{e1, e2, e3, e4} {
			if err := tx.Set(e); err != nil {
				return err
			}
		}
		return nil
	}))

	v := c.View()
	e1s := []localEntity1{}
	for e := range Iterate[localEntity1](v, index1) {
		e1s = append(e1s, e)
	}
	requireT.Equal([]localEntity1{e1, e2}, e1s)

	e2s := []localEntity2{}
	for e := range Iterate[localEntity2](v, index2) {
		e2s = append(e2s, e)
	}
	requireT.Equal([]localEntity2{e3, e4}, e2s)
}

func TestLocalClientInvalidEntityErr(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	ctx := qa.NewContext(t)

	c, err := NewLocalClient(LocalConfig{
		Types: []reflect.Type{
			reflect.TypeFor[localEntity1](),
			reflect.TypeFor[localEntity2](),
		},
	})
	requireT.NoError(err)
	requireT.NoError(c.WarmUp(ctx))
	requireT.Panics(func() {
		_ = c.NewTransactor().Tx(ctx, func(tx Tx) error {
			return tx.Set(localInvalid{ID: memdb.NewID[staticID1]()})
		})
	})
}

func TestLocalClientNoIDErr(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	ctx := qa.NewContext(t)

	e := e1
	e.ID = staticID1{}

	c, err := NewLocalClient(LocalConfig{
		Types: []reflect.Type{
			reflect.TypeFor[localEntity1](),
			reflect.TypeFor[localEntity2](),
		},
	})
	requireT.NoError(err)
	requireT.NoError(c.WarmUp(ctx))
	requireT.Panics(func() {
		_ = c.NewTransactor().Tx(ctx, func(tx Tx) error {
			return tx.Set(e)
		})
	})
}

func TestLocalClientTwoTransactions(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	ctx := qa.NewContext(t)

	e12 := e1
	e12.Value = "str12"

	c, err := NewLocalClient(LocalConfig{
		Types: []reflect.Type{
			reflect.TypeFor[localEntity1](),
			reflect.TypeFor[localEntity2](),
		},
	})
	requireT.NoError(err)
	requireT.NoError(c.WarmUp(ctx))
	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx Tx) error {
		for _, e := range []any{e1, e2} {
			if err := tx.Set(e); err != nil {
				return err
			}
		}
		return nil
	}))

	v := c.View()
	eDB1, exists := Get[localEntity1](v, e1.ID)
	requireT.True(exists)
	requireT.Equal(e1, eDB1)

	eDB1, exists = Get[localEntity1](v, e2.ID)
	requireT.True(exists)
	requireT.Equal(e2, eDB1)

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx Tx) error {
		return tx.Set(e12)
	}))

	v = c.View()
	eDB1, exists = Get[localEntity1](v, e1.ID)
	requireT.True(exists)
	requireT.Equal(e12, eDB1)

	eDB1, exists = Get[localEntity1](v, e2.ID)
	requireT.True(exists)
	requireT.Equal(e2, eDB1)
}

func TestLocalClientTransactionAbort(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	ctx := qa.NewContext(t)

	e12 := e1
	e12.Value = "str12"

	c, err := NewLocalClient(LocalConfig{
		Types: []reflect.Type{
			reflect.TypeFor[localEntity1](),
			reflect.TypeFor[localEntity2](),
		},
	})
	requireT.NoError(err)
	requireT.NoError(c.WarmUp(ctx))
	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx Tx) error {
		for _, e := range []any{e1, e2} {
			if err := tx.Set(e); err != nil {
				return err
			}
		}
		return nil
	}))

	v := c.View()
	eDB1, exists := Get[localEntity1](v, e1.ID)
	requireT.True(exists)
	requireT.Equal(e1, eDB1)

	eDB1, exists = Get[localEntity1](v, e2.ID)
	requireT.True(exists)
	requireT.Equal(e2, eDB1)

	requireT.Error(c.NewTransactor().Tx(ctx, func(tx Tx) error {
		requireT.NoError(tx.Set(e12))
		return errors.New("test")
	}))

	v = c.View()
	eDB1, exists = Get[localEntity1](v, e1.ID)
	requireT.True(exists)
	requireT.Equal(e1, eDB1)

	eDB1, exists = Get[localEntity1](v, e2.ID)
	requireT.True(exists)
	requireT.Equal(e2, eDB1)
}
