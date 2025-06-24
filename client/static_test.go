package client

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	magmatypes "github.com/outofforest/magma/types"
	"github.com/outofforest/memdb"
	"github.com/outofforest/memdb/indices"
	"github.com/outofforest/qa"
)

type (
	staticID1 memdb.ID
	staticID2 memdb.ID

	staticEntity1 struct {
		ID       staticID1
		Revision magmatypes.Revision
		Value    string
	}

	staticEntity2 struct {
		ID       staticID2
		Revision magmatypes.Revision
		Value    uint64
	}

	staticInvalid struct {
		ID    staticID1
		Value uint64
	}
)

var (
	e1 = staticEntity1{
		ID:    memdb.NewID[staticID1](),
		Value: "str1",
	}
	e2 = staticEntity1{
		ID:    memdb.NewID[staticID1](),
		Value: "str2",
	}
	e3 = staticEntity2{
		ID:    memdb.NewID[staticID2](),
		Value: 3,
	}
	e4 = staticEntity2{
		ID:    memdb.NewID[staticID2](),
		Value: 4,
	}
)

func TestStaticClient(t *testing.T) {
	requireT := require.New(t)
	ctx := qa.NewContext(t)

	c, err := NewStaticClient(StaticConfig{}, []any{e1, e2, e3, e4})
	requireT.NoError(err)
	requireT.NoError(c.WarmUp(ctx))

	v := c.View()
	eDB1, exists := Get[staticEntity1](v, e1.ID)
	requireT.True(exists)
	requireT.Equal(e1, eDB1)

	eDB1, exists = Get[staticEntity1](v, e2.ID)
	requireT.True(exists)
	requireT.Equal(e2, eDB1)

	eDB2, exists := Get[staticEntity2](v, e3.ID)
	requireT.True(exists)
	requireT.Equal(e3, eDB2)

	eDB2, exists = Get[staticEntity2](v, e4.ID)
	requireT.True(exists)
	requireT.Equal(e4, eDB2)
}

func TestStaticClientBeforeWarmUp(t *testing.T) {
	requireT := require.New(t)

	c, err := NewStaticClient(StaticConfig{}, []any{e1, e2, e3, e4})
	requireT.NoError(err)

	v := c.View()
	_, exists := Get[staticEntity1](v, e1.ID)
	requireT.False(exists)

	_, exists = Get[staticEntity1](v, e2.ID)
	requireT.False(exists)

	_, exists = Get[staticEntity2](v, e3.ID)
	requireT.False(exists)

	_, exists = Get[staticEntity2](v, e4.ID)
	requireT.False(exists)
}

func TestStaticClientTrigger(t *testing.T) {
	requireT := require.New(t)
	ctx := qa.NewContext(t)

	var triggered bool
	c, err := NewStaticClient(StaticConfig{
		TriggerFunc: func(ctx context.Context, v *View) error {
			triggered = true
			return nil
		},
	}, []any{e1, e2, e3, e4})
	requireT.NoError(err)

	requireT.False(triggered)

	requireT.NoError(c.WarmUp(ctx))

	requireT.True(triggered)
}

func TestStaticClientWarmUpTwice(t *testing.T) {
	requireT := require.New(t)
	ctx := qa.NewContext(t)

	var triggered bool
	c, err := NewStaticClient(StaticConfig{
		TriggerFunc: func(ctx context.Context, v *View) error {
			triggered = true
			return nil
		},
	}, []any{e1, e2, e3, e4})
	requireT.NoError(err)

	requireT.False(triggered)

	requireT.NoError(c.WarmUp(ctx))
	requireT.True(triggered)

	triggered = false

	requireT.NoError(c.WarmUp(ctx))
	requireT.False(triggered)
}

func TestStaticClientTriggerErr(t *testing.T) {
	requireT := require.New(t)
	ctx := qa.NewContext(t)

	c, err := NewStaticClient(StaticConfig{
		TriggerFunc: func(ctx context.Context, v *View) error {
			return errors.New("test")
		},
	}, []any{e1, e2, e3, e4})
	requireT.NoError(err)

	requireT.Error(c.WarmUp(ctx))
}

func TestStaticClientIndexes(t *testing.T) {
	requireT := require.New(t)
	ctx := qa.NewContext(t)

	var ie1 staticEntity1
	index1 := indices.NewFieldIndex(&ie1, &ie1.Value)
	var ie2 staticEntity2
	index2 := indices.NewFieldIndex(&ie2, &ie2.Value)

	c, err := NewStaticClient(StaticConfig{
		Indices: []memdb.Index{index1, index2},
	}, []any{e1, e2, e3, e4})
	requireT.NoError(err)
	requireT.NoError(c.WarmUp(ctx))

	v := c.View()
	e1s := []staticEntity1{}
	for e := range Iterate[staticEntity1](v, index1) {
		e1s = append(e1s, e)
	}
	requireT.Equal([]staticEntity1{e1, e2}, e1s)

	e2s := []staticEntity2{}
	for e := range Iterate[staticEntity2](v, index2) {
		e2s = append(e2s, e)
	}
	requireT.Equal([]staticEntity2{e3, e4}, e2s)
}

func TestStaticClientNoEntitiesErr(t *testing.T) {
	requireT := require.New(t)

	_, err := NewStaticClient(StaticConfig{}, nil)
	requireT.Error(err)
}

func TestStaticClientInvalidEntityErr(t *testing.T) {
	requireT := require.New(t)

	_, err := NewStaticClient(StaticConfig{}, []any{staticInvalid{ID: memdb.NewID[staticID1]()}})
	requireT.Error(err)
}

func TestStaticClientNoIDErr(t *testing.T) {
	requireT := require.New(t)
	ctx := qa.NewContext(t)

	e := e1
	e.ID = staticID1{}
	c, err := NewStaticClient(StaticConfig{}, []any{e})
	requireT.NoError(err)
	requireT.Panics(func() {
		_ = c.WarmUp(ctx)
	})
}
