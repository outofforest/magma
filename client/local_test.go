package client

import (
	"context"
	"reflect"
	"testing"

	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/client/db"
	"github.com/outofforest/magma/client/localentities"
	"github.com/outofforest/memdb"
	"github.com/outofforest/memdb/indices"
	"github.com/outofforest/qa"
)

var (
	e1 = localentities.Entity1{
		ID:    memdb.NewID[localentities.ID1](),
		Value: "str1",
	}
	e2 = localentities.Entity1{
		ID:    memdb.NewID[localentities.ID1](),
		Value: "str2",
	}
	e3 = localentities.Entity2{
		ID:    memdb.NewID[localentities.ID2](),
		Value: 3,
	}
	e4 = localentities.Entity2{
		ID:    memdb.NewID[localentities.ID2](),
		Value: 4,
	}
)

func TestLocalClient(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	ctx := qa.NewContext(t)

	c, err := NewLocalClient(LocalConfig{
		Marshaller: localentities.NewMarshaller(),
	})
	requireT.NoError(err)
	requireT.NoError(c.WarmUp(ctx))
	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		for _, e := range []any{e1, e2, e3, e4} {
			if err := tx.Set(e); err != nil {
				return err
			}
		}
		return nil
	}))

	v := c.View()
	eDB1, exists := db.Get[localentities.Entity1](v, e1.ID)
	requireT.True(exists)
	requireT.Equal(e1, eDB1)

	eDB1, exists = db.Get[localentities.Entity1](v, e2.ID)
	requireT.True(exists)
	requireT.Equal(e2, eDB1)

	eDB2, exists := db.Get[localentities.Entity2](v, e3.ID)
	requireT.True(exists)
	requireT.Equal(e3, eDB2)

	eDB2, exists = db.Get[localentities.Entity2](v, e4.ID)
	requireT.True(exists)
	requireT.Equal(e4, eDB2)
}

func TestLocalClientTrigger(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	ctx := qa.NewContext(t)

	var triggered bool
	c, err := NewLocalClient(LocalConfig{
		Marshaller: localentities.NewMarshaller(),
		TriggerFunc: func(ctx context.Context, v *db.View, types map[reflect.Type]struct{}) error {
			triggered = true
			return nil
		},
	})
	requireT.NoError(err)
	requireT.NoError(c.WarmUp(ctx))

	requireT.False(triggered)

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
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

	var receivedTypes map[reflect.Type]struct{}
	c, err := NewLocalClient(LocalConfig{
		Marshaller: localentities.NewMarshaller(),
		TriggerFunc: func(ctx context.Context, v *db.View, types map[reflect.Type]struct{}) error {
			receivedTypes = types
			return nil
		},
	})
	requireT.NoError(err)
	requireT.NoError(c.WarmUp(ctx))

	requireT.Nil(receivedTypes)

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		for _, e := range []any{e1, e2, e3, e4} {
			if err := tx.Set(e); err != nil {
				return err
			}
		}
		return nil
	}))

	requireT.ElementsMatch([]reflect.Type{
		reflect.TypeFor[localentities.Entity1](),
		reflect.TypeFor[localentities.Entity2](),
	}, lo.Keys(receivedTypes))
}

func TestLocalClientTriggerViewUpdateIsLocal(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	ctx := qa.NewContext(t)

	c, err := NewLocalClient(LocalConfig{
		Marshaller: localentities.NewMarshaller(),
		TriggerFunc: func(ctx context.Context, v *db.View, types map[reflect.Type]struct{}) error {
			e := e1
			e.Value = "str2"
			v.Set(e)

			e2, exists := db.Get[localentities.Entity1](v, e.ID)
			requireT.True(exists)
			requireT.Equal(e, e2)

			return nil
		},
	})
	requireT.NoError(err)
	requireT.NoError(c.WarmUp(ctx))
	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		return tx.Set(e1)
	}))

	e2, exists := db.Get[localentities.Entity1](c.View(), e1.ID)
	requireT.True(exists)
	requireT.Equal(e1, e2)
}

func TestLocalClientTriggerErr(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	ctx := qa.NewContext(t)

	c, err := NewLocalClient(LocalConfig{
		Marshaller: localentities.NewMarshaller(),
		TriggerFunc: func(ctx context.Context, v *db.View, types map[reflect.Type]struct{}) error {
			return errors.New("test")
		},
	})
	requireT.NoError(err)
	requireT.NoError(c.WarmUp(ctx))
	requireT.Error(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
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

	var ie1 localentities.Entity1
	index1 := indices.NewFieldIndex(&ie1, &ie1.Value)
	var ie2 localentities.Entity2
	index2 := indices.NewFieldIndex(&ie2, &ie2.Value)

	c, err := NewLocalClient(LocalConfig{
		Marshaller: localentities.NewMarshaller(),
		Indices:    []memdb.Index{index1, index2},
	})
	requireT.NoError(err)
	requireT.NoError(c.WarmUp(ctx))
	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		for _, e := range []any{e1, e2, e3, e4} {
			if err := tx.Set(e); err != nil {
				return err
			}
		}
		return nil
	}))

	v := c.View()
	e1s := []localentities.Entity1{}
	for e := range db.Iterate[localentities.Entity1](v, index1) {
		e1s = append(e1s, e)
	}
	requireT.Equal([]localentities.Entity1{e1, e2}, e1s)

	e2s := []localentities.Entity2{}
	for e := range db.Iterate[localentities.Entity2](v, index2) {
		e2s = append(e2s, e)
	}
	requireT.Equal([]localentities.Entity2{e3, e4}, e2s)
}

func TestLocalClientInvalidEntityErr(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	ctx := qa.NewContext(t)

	c, err := NewLocalClient(LocalConfig{
		Marshaller: localentities.NewMarshaller(),
	})
	requireT.NoError(err)
	requireT.NoError(c.WarmUp(ctx))
	requireT.Panics(func() {
		_ = c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
			return tx.Set(localentities.Invalid{})
		})
	})
}

func TestLocalClientNoIDErr(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	ctx := qa.NewContext(t)

	e := e1
	e.ID = localentities.ID1{}

	c, err := NewLocalClient(LocalConfig{
		Marshaller: localentities.NewMarshaller(),
	})
	requireT.NoError(err)
	requireT.NoError(c.WarmUp(ctx))
	requireT.Panics(func() {
		_ = c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
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
		Marshaller: localentities.NewMarshaller(),
	})
	requireT.NoError(err)
	requireT.NoError(c.WarmUp(ctx))
	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		for _, e := range []any{e1, e2} {
			if err := tx.Set(e); err != nil {
				return err
			}
		}
		return nil
	}))

	v := c.View()
	eDB1, exists := db.Get[localentities.Entity1](v, e1.ID)
	requireT.True(exists)
	requireT.Equal(e1, eDB1)

	eDB1, exists = db.Get[localentities.Entity1](v, e2.ID)
	requireT.True(exists)
	requireT.Equal(e2, eDB1)

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		return tx.Set(e12)
	}))

	v = c.View()
	eDB1, exists = db.Get[localentities.Entity1](v, e1.ID)
	requireT.True(exists)
	requireT.Equal(e12, eDB1)

	eDB1, exists = db.Get[localentities.Entity1](v, e2.ID)
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
		Marshaller: localentities.NewMarshaller(),
	})
	requireT.NoError(err)
	requireT.NoError(c.WarmUp(ctx))
	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		for _, e := range []any{e1, e2} {
			if err := tx.Set(e); err != nil {
				return err
			}
		}
		return nil
	}))

	v := c.View()
	eDB1, exists := db.Get[localentities.Entity1](v, e1.ID)
	requireT.True(exists)
	requireT.Equal(e1, eDB1)

	eDB1, exists = db.Get[localentities.Entity1](v, e2.ID)
	requireT.True(exists)
	requireT.Equal(e2, eDB1)

	requireT.Error(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		requireT.NoError(tx.Set(e12))
		return errors.New("test")
	}))

	v = c.View()
	eDB1, exists = db.Get[localentities.Entity1](v, e1.ID)
	requireT.True(exists)
	requireT.Equal(e1, eDB1)

	eDB1, exists = db.Get[localentities.Entity1](v, e2.ID)
	requireT.True(exists)
	requireT.Equal(e2, eDB1)
}
