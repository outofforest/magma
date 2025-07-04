package client

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/logger"
	"github.com/outofforest/magma/integration/entities"
	"github.com/outofforest/memdb"
	"github.com/outofforest/memdb/indices"
)

var config = NewTestConfig(entities.NewMarshaller(), nil)

func withIndices(config Config, indices ...memdb.Index) Config {
	config.Indices = append(append([]memdb.Index{}, config.Indices...), indices...)
	return config
}

func TestEntityCreation(t *testing.T) {
	t.Parallel()

	ctx := newContext(t)

	requireT := require.New(t)
	c := NewTestClient(t, config)

	acc1 := entities.Account{
		ID:        memdb.NewID[entities.AccountID](),
		FirstName: "First1",
		LastName:  "Last1",
	}
	acc2 := entities.Account{
		ID:        memdb.NewID[entities.AccountID](),
		FirstName: "First2",
		LastName:  "Last2",
	}

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx *Tx) error {
		_, exists := Get[entities.Account](tx.View, acc1.ID)
		requireT.False(exists)

		tx.Set(acc1)

		acc, exists := Get[entities.Account](tx.View, acc1.ID)
		requireT.True(exists)
		requireT.Equal(acc1, acc)

		return nil
	}))

	acc1.Revision = 1

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx *Tx) error {
		acc, exists := Get[entities.Account](tx.View, acc1.ID)
		requireT.True(exists)
		requireT.Equal(acc1, acc)

		tx.Set(acc2)

		acc, exists = Get[entities.Account](tx.View, acc2.ID)
		requireT.True(exists)
		requireT.Equal(acc2, acc)

		return nil
	}))

	acc2.Revision = 1
	v := c.View()

	acc, exists := Get[entities.Account](v, acc1.ID)
	requireT.True(exists)
	requireT.Equal(acc, acc1)

	acc, exists = Get[entities.Account](v, acc2.ID)
	requireT.True(exists)
	requireT.Equal(acc, acc2)
}

func TestEntityUpdate(t *testing.T) {
	t.Parallel()

	ctx := newContext(t)

	requireT := require.New(t)
	c := NewTestClient(t, config)

	acc1 := entities.Account{
		ID:        memdb.NewID[entities.AccountID](),
		FirstName: "First1",
		LastName:  "Last1",
	}
	acc2 := entities.Account{
		ID:        memdb.NewID[entities.AccountID](),
		FirstName: "First2",
		LastName:  "Last2",
	}

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx *Tx) error {
		tx.Set(acc1)
		tx.Set(acc2)

		return nil
	}))

	acc1.Revision = 1
	acc2.Revision = 1

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx *Tx) error {
		acc, exists := Get[entities.Account](tx.View, acc1.ID)
		requireT.True(exists)
		requireT.Equal(acc1, acc)

		acc.FirstName = "AAA"
		tx.Set(acc)

		accc, exists := Get[entities.Account](tx.View, acc1.ID)
		requireT.True(exists)
		requireT.Equal(accc, acc)

		acc, exists = Get[entities.Account](tx.View, acc2.ID)
		requireT.True(exists)
		requireT.Equal(acc2, acc)

		acc.FirstName = "BBB"
		tx.Set(acc)

		accc, exists = Get[entities.Account](tx.View, acc2.ID)
		requireT.True(exists)
		requireT.Equal(accc, acc)

		return nil
	}))

	acc1.Revision = 2
	acc1.FirstName = "AAA"
	acc2.Revision = 2
	acc2.FirstName = "BBB"
	v := c.View()

	acc, exists := Get[entities.Account](v, acc1.ID)
	requireT.True(exists)
	requireT.Equal(acc1, acc)

	acc, exists = Get[entities.Account](v, acc2.ID)
	requireT.True(exists)
	requireT.Equal(acc2, acc)
}

func TestFailingTransaction(t *testing.T) {
	t.Parallel()

	ctx := newContext(t)

	requireT := require.New(t)

	err := errors.New("error")
	requireT.ErrorIs(NewTestClient(t, config).NewTransactor().
		Tx(ctx, func(tx *Tx) error {
			return err
		}), err)
}

func TestFieldIndexString(t *testing.T) {
	t.Parallel()

	ctx := newContext(t)

	requireT := require.New(t)

	var acc entities.Account
	indexLastName := indices.NewFieldIndex(&acc, &acc.LastName)

	c := NewTestClient(t, withIndices(config, indexLastName))

	accs := []entities.Account{
		{
			ID:        memdb.NewID[entities.AccountID](),
			FirstName: "First2",
			LastName:  "Last2",
		},
		{
			ID:        memdb.NewID[entities.AccountID](),
			FirstName: "First1",
			LastName:  "Last1",
		},
		{
			ID:        memdb.NewID[entities.AccountID](),
			FirstName: "First3",
			LastName:  "Last21",
		},
	}

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx *Tx) error {
		for _, acc := range accs {
			tx.Set(acc)
		}
		return nil
	}))

	for i := range accs {
		accs[i].Revision = 1
	}

	v := c.View()
	acc, exists := First[entities.Account](v, indexLastName)
	requireT.True(exists)
	requireT.Equal(accs[1], acc)

	acc, exists = First[entities.Account](v, indexLastName, "Last2")
	requireT.True(exists)
	requireT.Equal(accs[0], acc)

	_, exists = First[entities.Account](v, indexLastName, "La")
	requireT.False(exists)

	i := 0
	for acc := range Iterate[entities.Account](v, indexLastName) {
		switch i {
		case 0:
			requireT.Equal(accs[1], acc)
		case 1:
			requireT.Equal(accs[0], acc)
		case 2:
			requireT.Equal(accs[2], acc)
		default:
			requireT.Fail("wrong index")
		}
		i++
	}
	requireT.Equal(3, i)

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
	requireT.Equal(1, i)

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
	acc, ok = it()
	requireT.True(ok)
	requireT.Equal(accs[2], acc)
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

	i = 0
	for acc := range Iterate[entities.Account](v, indexLastName, memdb.From, "Last2") {
		switch i {
		case 0:
			requireT.Equal(accs[0], acc)
		case 1:
			requireT.Equal(accs[2], acc)
		default:
			requireT.Fail("wrong index")
		}
		i++
	}
	requireT.Equal(2, i)

	i = 0
	for acc := range Iterate[entities.Account](v, indexLastName, memdb.From, "Last21", memdb.Back, 1) {
		switch i {
		case 0:
			requireT.Equal(accs[0], acc)
		case 1:
			requireT.Equal(accs[2], acc)
		default:
			requireT.Fail("wrong index")
		}
		i++
	}
	requireT.Equal(2, i)

	for range Iterate[entities.Account](v, indexLastName, memdb.From, "Missing") {
		requireT.Fail("nothing should be returned")
	}

	it = Iterator[entities.Account](v, indexLastName, memdb.From)
	acc, ok = it()
	requireT.True(ok)
	requireT.Equal(accs[1], acc)
	acc, ok = it()
	requireT.True(ok)
	requireT.Equal(accs[0], acc)
	acc, ok = it()
	requireT.True(ok)
	requireT.Equal(accs[2], acc)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Account](v, indexLastName, memdb.From, "Last2")
	acc, ok = it()
	requireT.True(ok)
	requireT.Equal(accs[0], acc)
	acc, ok = it()
	requireT.True(ok)
	requireT.Equal(accs[2], acc)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Account](v, indexLastName, memdb.From, "Missing")
	_, ok = it()
	requireT.False(ok)
}

func TestFieldIndexBool(t *testing.T) {
	t.Parallel()

	ctx := newContext(t)

	requireT := require.New(t)

	var e entities.Fields
	index := indices.NewFieldIndex(&e, &e.Bool)

	c := NewTestClient(t, withIndices(config, index))

	es := []entities.Fields{
		{
			ID:   memdb.NewID[memdb.ID](),
			Bool: true,
		},
		{
			ID:   memdb.NewID[memdb.ID](),
			Bool: false,
		},
	}

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx *Tx) error {
		for _, e := range es {
			tx.Set(e)
		}
		return nil
	}))

	for i := range es {
		es[i].Revision = 1
	}

	v := c.View()
	e, exists := First[entities.Fields](v, index)
	requireT.True(exists)
	requireT.Equal(es[1], e)

	e, exists = First[entities.Fields](v, index, true)
	requireT.True(exists)
	requireT.Equal(es[0], e)

	e, exists = First[entities.Fields](v, index, false)
	requireT.True(exists)
	requireT.Equal(es[1], e)

	it := Iterator[entities.Fields](v, index)
	e, ok := it()
	requireT.True(ok)
	requireT.Equal(es[1], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[0], e)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Fields](v, index, true)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[0], e)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Fields](v, index, memdb.From)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[1], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[0], e)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Fields](v, index, memdb.From, true)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[0], e)
	_, ok = it()
	requireT.False(ok)
}

func TestFieldIndexTime(t *testing.T) {
	t.Parallel()

	ctx := newContext(t)

	requireT := require.New(t)

	var e entities.Fields
	index := indices.NewFieldIndex(&e, &e.Time)

	c := NewTestClient(t, withIndices(config, index))

	time0 := time.Unix(100, 10)
	time1 := time.Unix(10, 20)
	time2 := time.Unix(10, 10)
	time3 := time.Unix(0, 10)
	time4 := time.Unix(-10, 20)
	time5 := time.Unix(-10, 10)
	time6 := time.Unix(-100, 20)

	es := []entities.Fields{
		{
			ID:   memdb.NewID[memdb.ID](),
			Time: time0,
		},
		{
			ID:   memdb.NewID[memdb.ID](),
			Time: time1,
		},
		{
			ID:   memdb.NewID[memdb.ID](),
			Time: time2,
		},
		{
			ID:   memdb.NewID[memdb.ID](),
			Time: time3,
		},
		{
			ID:   memdb.NewID[memdb.ID](),
			Time: time4,
		},
		{
			ID:   memdb.NewID[memdb.ID](),
			Time: time5,
		},
		{
			ID:   memdb.NewID[memdb.ID](),
			Time: time6,
		},
	}

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx *Tx) error {
		for _, e := range es {
			tx.Set(e)
		}
		return nil
	}))

	for i := range es {
		es[i].Revision = 1
	}

	v := c.View()
	e, exists := First[entities.Fields](v, index)
	requireT.True(exists)
	requireT.Equal(es[6], e)

	e, exists = First[entities.Fields](v, index, time4)
	requireT.True(exists)
	requireT.Equal(es[4], e)

	e, exists = First[entities.Fields](v, index, time1)
	requireT.True(exists)
	requireT.Equal(es[1], e)

	_, exists = First[entities.Fields](v, index, time.Time{})
	requireT.False(exists)

	it := Iterator[entities.Fields](v, index)
	e, ok := it()
	requireT.True(ok)
	requireT.Equal(es[6], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[5], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[4], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[3], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[2], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[1], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[0], e)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Fields](v, index, time3)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[3], e)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Fields](v, index, memdb.From)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[6], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[5], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[4], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[3], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[2], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[1], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[0], e)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Fields](v, index, memdb.From, time3)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[3], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[2], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[1], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[0], e)
	_, ok = it()
	requireT.False(ok)
}

func TestFieldIndexInt8(t *testing.T) {
	t.Parallel()

	ctx := newContext(t)

	type intType = int8

	requireT := require.New(t)

	var e entities.Fields
	index := indices.NewFieldIndex(&e, &e.Int8)

	c := NewTestClient(t, withIndices(config, index))

	es := []entities.Fields{
		{
			ID:   memdb.NewID[memdb.ID](),
			Int8: 100,
		},
		{
			ID:   memdb.NewID[memdb.ID](),
			Int8: 10,
		},
		{
			ID:   memdb.NewID[memdb.ID](),
			Int8: 0,
		},
		{
			ID:   memdb.NewID[memdb.ID](),
			Int8: -10,
		},
		{
			ID:   memdb.NewID[memdb.ID](),
			Int8: -100,
		},
	}

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx *Tx) error {
		for _, e := range es {
			tx.Set(e)
		}
		return nil
	}))

	for i := range es {
		es[i].Revision = 1
	}

	v := c.View()
	e, exists := First[entities.Fields](v, index)
	requireT.True(exists)
	requireT.Equal(es[4], e)

	e, exists = First[entities.Fields](v, index, intType(-100))
	requireT.True(exists)
	requireT.Equal(es[4], e)

	e, exists = First[entities.Fields](v, index, intType(100))
	requireT.True(exists)
	requireT.Equal(es[0], e)

	_, exists = First[entities.Fields](v, index, intType(1))
	requireT.False(exists)

	it := Iterator[entities.Fields](v, index)
	e, ok := it()
	requireT.True(ok)
	requireT.Equal(es[4], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[3], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[2], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[1], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[0], e)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Fields](v, index, intType(10))
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[1], e)
	_, ok = it()
	requireT.False(ok)
}

func TestFieldIndexInt16(t *testing.T) {
	t.Parallel()

	ctx := newContext(t)

	type intType = int16

	requireT := require.New(t)

	var e entities.Fields
	index := indices.NewFieldIndex(&e, &e.Int16)

	c := NewTestClient(t, withIndices(config, index))

	es := []entities.Fields{
		{
			ID:    memdb.NewID[memdb.ID](),
			Int16: 100,
		},
		{
			ID:    memdb.NewID[memdb.ID](),
			Int16: 10,
		},
		{
			ID:    memdb.NewID[memdb.ID](),
			Int16: 0,
		},
		{
			ID:    memdb.NewID[memdb.ID](),
			Int16: -10,
		},
		{
			ID:    memdb.NewID[memdb.ID](),
			Int16: -100,
		},
	}

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx *Tx) error {
		for _, e := range es {
			tx.Set(e)
		}
		return nil
	}))

	for i := range es {
		es[i].Revision = 1
	}

	v := c.View()
	e, exists := First[entities.Fields](v, index)
	requireT.True(exists)
	requireT.Equal(es[4], e)

	e, exists = First[entities.Fields](v, index, intType(-100))
	requireT.True(exists)
	requireT.Equal(es[4], e)

	e, exists = First[entities.Fields](v, index, intType(100))
	requireT.True(exists)
	requireT.Equal(es[0], e)

	_, exists = First[entities.Fields](v, index, intType(1))
	requireT.False(exists)

	it := Iterator[entities.Fields](v, index)
	e, ok := it()
	requireT.True(ok)
	requireT.Equal(es[4], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[3], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[2], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[1], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[0], e)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Fields](v, index, intType(10))
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[1], e)
	_, ok = it()
	requireT.False(ok)
}

func TestFieldIndexInt32(t *testing.T) {
	t.Parallel()

	ctx := newContext(t)

	type intType = int32

	requireT := require.New(t)

	var e entities.Fields
	index := indices.NewFieldIndex(&e, &e.Int32)

	c := NewTestClient(t, withIndices(config, index))

	es := []entities.Fields{
		{
			ID:    memdb.NewID[memdb.ID](),
			Int32: 100,
		},
		{
			ID:    memdb.NewID[memdb.ID](),
			Int32: 10,
		},
		{
			ID:    memdb.NewID[memdb.ID](),
			Int32: 0,
		},
		{
			ID:    memdb.NewID[memdb.ID](),
			Int32: -10,
		},
		{
			ID:    memdb.NewID[memdb.ID](),
			Int32: -100,
		},
	}

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx *Tx) error {
		for _, e := range es {
			tx.Set(e)
		}
		return nil
	}))

	for i := range es {
		es[i].Revision = 1
	}

	v := c.View()
	e, exists := First[entities.Fields](v, index)
	requireT.True(exists)
	requireT.Equal(es[4], e)

	e, exists = First[entities.Fields](v, index, intType(-100))
	requireT.True(exists)
	requireT.Equal(es[4], e)

	e, exists = First[entities.Fields](v, index, intType(100))
	requireT.True(exists)
	requireT.Equal(es[0], e)

	_, exists = First[entities.Fields](v, index, intType(1))
	requireT.False(exists)

	it := Iterator[entities.Fields](v, index)
	e, ok := it()
	requireT.True(ok)
	requireT.Equal(es[4], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[3], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[2], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[1], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[0], e)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Fields](v, index, intType(10))
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[1], e)
	_, ok = it()
	requireT.False(ok)
}

func TestFieldIndexInt64(t *testing.T) {
	t.Parallel()

	ctx := newContext(t)

	type intType = int64

	requireT := require.New(t)

	var e entities.Fields
	index := indices.NewFieldIndex(&e, &e.Int64)

	c := NewTestClient(t, withIndices(config, index))

	es := []entities.Fields{
		{
			ID:    memdb.NewID[memdb.ID](),
			Int64: 100,
		},
		{
			ID:    memdb.NewID[memdb.ID](),
			Int64: 10,
		},
		{
			ID:    memdb.NewID[memdb.ID](),
			Int64: 0,
		},
		{
			ID:    memdb.NewID[memdb.ID](),
			Int64: -10,
		},
		{
			ID:    memdb.NewID[memdb.ID](),
			Int64: -100,
		},
	}

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx *Tx) error {
		for _, e := range es {
			tx.Set(e)
		}
		return nil
	}))

	for i := range es {
		es[i].Revision = 1
	}

	v := c.View()
	e, exists := First[entities.Fields](v, index)
	requireT.True(exists)
	requireT.Equal(es[4], e)

	e, exists = First[entities.Fields](v, index, intType(-100))
	requireT.True(exists)
	requireT.Equal(es[4], e)

	e, exists = First[entities.Fields](v, index, intType(100))
	requireT.True(exists)
	requireT.Equal(es[0], e)

	_, exists = First[entities.Fields](v, index, intType(1))
	requireT.False(exists)

	it := Iterator[entities.Fields](v, index)
	e, ok := it()
	requireT.True(ok)
	requireT.Equal(es[4], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[3], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[2], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[1], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[0], e)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Fields](v, index, intType(10))
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[1], e)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Fields](v, index, memdb.From)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[4], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[3], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[2], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[1], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[0], e)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Fields](v, index, memdb.From, intType(10))
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[1], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[0], e)
	_, ok = it()
	requireT.False(ok)
}

func TestFieldIndexUInt8(t *testing.T) {
	t.Parallel()

	ctx := newContext(t)

	type intType = uint8

	requireT := require.New(t)

	var e entities.Fields
	index := indices.NewFieldIndex(&e, &e.Uint8)

	c := NewTestClient(t, withIndices(config, index))

	es := []entities.Fields{
		{
			ID:    memdb.NewID[memdb.ID](),
			Uint8: 100,
		},
		{
			ID:    memdb.NewID[memdb.ID](),
			Uint8: 10,
		},
		{
			ID:    memdb.NewID[memdb.ID](),
			Uint8: 0,
		},
	}

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx *Tx) error {
		for _, e := range es {
			tx.Set(e)
		}
		return nil
	}))

	for i := range es {
		es[i].Revision = 1
	}

	v := c.View()
	e, exists := First[entities.Fields](v, index)
	requireT.True(exists)
	requireT.Equal(es[2], e)

	e, exists = First[entities.Fields](v, index, intType(100))
	requireT.True(exists)
	requireT.Equal(es[0], e)

	_, exists = First[entities.Fields](v, index, intType(1))
	requireT.False(exists)

	it := Iterator[entities.Fields](v, index)
	e, ok := it()
	requireT.True(ok)
	requireT.Equal(es[2], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[1], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[0], e)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Fields](v, index, intType(10))
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[1], e)
	_, ok = it()
	requireT.False(ok)
}

func TestFieldIndexUInt16(t *testing.T) {
	t.Parallel()

	ctx := newContext(t)

	type intType = uint16

	requireT := require.New(t)

	var e entities.Fields
	index := indices.NewFieldIndex(&e, &e.Uint16)

	c := NewTestClient(t, withIndices(config, index))

	es := []entities.Fields{
		{
			ID:     memdb.NewID[memdb.ID](),
			Uint16: 100,
		},
		{
			ID:     memdb.NewID[memdb.ID](),
			Uint16: 10,
		},
		{
			ID:     memdb.NewID[memdb.ID](),
			Uint16: 0,
		},
	}

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx *Tx) error {
		for _, e := range es {
			tx.Set(e)
		}
		return nil
	}))

	for i := range es {
		es[i].Revision = 1
	}

	v := c.View()
	e, exists := First[entities.Fields](v, index)
	requireT.True(exists)
	requireT.Equal(es[2], e)

	e, exists = First[entities.Fields](v, index, intType(100))
	requireT.True(exists)
	requireT.Equal(es[0], e)

	_, exists = First[entities.Fields](v, index, intType(1))
	requireT.False(exists)

	it := Iterator[entities.Fields](v, index)
	e, ok := it()
	requireT.True(ok)
	requireT.Equal(es[2], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[1], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[0], e)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Fields](v, index, intType(10))
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[1], e)
	_, ok = it()
	requireT.False(ok)
}

func TestFieldIndexUInt32(t *testing.T) {
	t.Parallel()

	ctx := newContext(t)

	type intType = uint32

	requireT := require.New(t)

	var e entities.Fields
	index := indices.NewFieldIndex(&e, &e.Uint32)

	c := NewTestClient(t, withIndices(config, index))

	es := []entities.Fields{
		{
			ID:     memdb.NewID[memdb.ID](),
			Uint32: 100,
		},
		{
			ID:     memdb.NewID[memdb.ID](),
			Uint32: 10,
		},
		{
			ID:     memdb.NewID[memdb.ID](),
			Uint32: 0,
		},
	}

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx *Tx) error {
		for _, e := range es {
			tx.Set(e)
		}
		return nil
	}))

	for i := range es {
		es[i].Revision = 1
	}

	v := c.View()
	e, exists := First[entities.Fields](v, index)
	requireT.True(exists)
	requireT.Equal(es[2], e)

	e, exists = First[entities.Fields](v, index, intType(100))
	requireT.True(exists)
	requireT.Equal(es[0], e)

	_, exists = First[entities.Fields](v, index, intType(1))
	requireT.False(exists)

	it := Iterator[entities.Fields](v, index)
	e, ok := it()
	requireT.True(ok)
	requireT.Equal(es[2], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[1], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[0], e)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Fields](v, index, intType(10))
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[1], e)
	_, ok = it()
	requireT.False(ok)
}

func TestFieldIndexUInt64(t *testing.T) {
	t.Parallel()

	ctx := newContext(t)

	type intType = uint64

	requireT := require.New(t)

	var e entities.Fields
	index := indices.NewFieldIndex(&e, &e.Uint64)

	c := NewTestClient(t, withIndices(config, index))

	es := []entities.Fields{
		{
			ID:     memdb.NewID[memdb.ID](),
			Uint64: 100,
		},
		{
			ID:     memdb.NewID[memdb.ID](),
			Uint64: 10,
		},
		{
			ID:     memdb.NewID[memdb.ID](),
			Uint64: 0,
		},
	}

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx *Tx) error {
		for _, e := range es {
			tx.Set(e)
		}
		return nil
	}))

	for i := range es {
		es[i].Revision = 1
	}

	v := c.View()
	e, exists := First[entities.Fields](v, index)
	requireT.True(exists)
	requireT.Equal(es[2], e)

	e, exists = First[entities.Fields](v, index, intType(100))
	requireT.True(exists)
	requireT.Equal(es[0], e)

	_, exists = First[entities.Fields](v, index, intType(1))
	requireT.False(exists)

	it := Iterator[entities.Fields](v, index)
	e, ok := it()
	requireT.True(ok)
	requireT.Equal(es[2], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[1], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[0], e)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Fields](v, index, intType(10))
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[1], e)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Fields](v, index, memdb.From)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[2], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[1], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[0], e)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Fields](v, index, memdb.From, intType(10))
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[1], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[0], e)
	_, ok = it()
	requireT.False(ok)
}

func TestFieldIndexID(t *testing.T) {
	t.Parallel()

	ctx := newContext(t)

	requireT := require.New(t)

	var e entities.Fields
	index := indices.NewFieldIndex(&e, &e.EntityID)

	c := NewTestClient(t, withIndices(config, index))

	es := []entities.Fields{
		{
			ID:       memdb.NewID[memdb.ID](),
			EntityID: entities.AccountID{0x02},
		},
		{
			ID:       memdb.NewID[memdb.ID](),
			EntityID: entities.AccountID{0x00, 0x01},
		},
		{
			ID:       memdb.NewID[memdb.ID](),
			EntityID: entities.AccountID{0x00, 0x00},
		},
	}

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx *Tx) error {
		for _, e := range es {
			tx.Set(e)
		}
		return nil
	}))

	for i := range es {
		es[i].Revision = 1
	}

	v := c.View()
	e, exists := First[entities.Fields](v, index)
	requireT.True(exists)
	requireT.Equal(es[2], e)

	e, exists = First[entities.Fields](v, index, entities.AccountID{0x02})
	requireT.True(exists)
	requireT.Equal(es[0], e)

	_, exists = First[entities.Fields](v, index, entities.AccountID{0x03})
	requireT.False(exists)

	it := Iterator[entities.Fields](v, index)
	e, ok := it()
	requireT.True(ok)
	requireT.Equal(es[2], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[1], e)
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[0], e)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Fields](v, index, entities.AccountID{0x00, 0x01})
	e, ok = it()
	requireT.True(ok)
	requireT.Equal(es[1], e)
	_, ok = it()
	requireT.False(ok)
}

func TestIfIndex(t *testing.T) {
	t.Parallel()

	ctx := newContext(t)

	requireT := require.New(t)

	var acc entities.Account
	indexLastName := indices.NewIfIndex[entities.Account](
		indices.NewFieldIndex(&acc, &acc.LastName),
		func(acc *entities.Account) bool {
			return acc.FirstName == "First1"
		},
	)

	c := NewTestClient(t, withIndices(config, indexLastName))

	accs := []entities.Account{
		{
			ID:        memdb.NewID[entities.AccountID](),
			FirstName: "First1",
			LastName:  "Last3",
		},
		{
			ID:        memdb.NewID[entities.AccountID](),
			FirstName: "First2",
			LastName:  "Last2",
		},
		{
			ID:        memdb.NewID[entities.AccountID](),
			FirstName: "First1",
			LastName:  "Last2",
		},
	}

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx *Tx) error {
		for _, acc := range accs {
			tx.Set(acc)
		}
		return nil
	}))

	for i := range accs {
		accs[i].Revision = 1
	}

	v := c.View()
	acc, exists := First[entities.Account](v, indexLastName)
	requireT.True(exists)
	requireT.Equal(accs[2], acc)

	acc, exists = First[entities.Account](v, indexLastName, "Last2")
	requireT.True(exists)
	requireT.Equal(accs[2], acc)

	_, exists = First[entities.Account](v, indexLastName, "Missing")
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
	requireT.Equal(2, i)

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
	requireT.Equal(1, i)

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

func TestIfIndexWhenEntityIsExcludedAfterUpdate(t *testing.T) {
	t.Parallel()

	ctx := newContext(t)

	requireT := require.New(t)

	acc := entities.Account{
		ID:        memdb.NewID[entities.AccountID](),
		FirstName: "First1",
		LastName:  "Last1",
	}

	indexLastName := indices.NewIfIndex[entities.Account](
		indices.NewFieldIndex(&acc, &acc.LastName),
		func(acc *entities.Account) bool {
			return acc.FirstName == "First1"
		},
	)

	c := NewTestClient(t, withIndices(config, indexLastName))

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx *Tx) error {
		tx.Set(acc)
		return nil
	}))

	v := c.View()
	acc, exists := First[entities.Account](v, indexLastName)
	requireT.True(exists)

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx *Tx) error {
		acc.FirstName = "First2"
		tx.Set(acc)
		return nil
	}))

	v = c.View()
	_, exists = First[entities.Account](v, indexLastName)
	requireT.False(exists)
}

func TestMultiIndex(t *testing.T) {
	t.Parallel()

	ctx := newContext(t)

	requireT := require.New(t)

	var acc entities.Account
	indexName := indices.NewMultiIndex(
		indices.NewFieldIndex(&acc, &acc.LastName),
		indices.NewFieldIndex(&acc, &acc.FirstName),
	)

	c := NewTestClient(t, withIndices(config, indexName))

	accs := []entities.Account{
		{
			ID:        memdb.NewID[entities.AccountID](),
			FirstName: "First3",
			LastName:  "Last2",
		},
		{
			ID:        memdb.NewID[entities.AccountID](),
			FirstName: "First2",
			LastName:  "Last2",
		},
		{
			ID:        memdb.NewID[entities.AccountID](),
			FirstName: "First1",
			LastName:  "Last1",
		},
		{
			ID:        memdb.NewID[entities.AccountID](),
			FirstName: "First",
			LastName:  "Last",
		},
		{
			ID:        memdb.NewID[entities.AccountID](),
			FirstName: "st",
			LastName:  "La",
		},
	}

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx *Tx) error {
		for _, acc := range accs {
			tx.Set(acc)
		}
		return nil
	}))

	for i := range accs {
		accs[i].Revision = 1
	}

	v := c.View()
	acc, exists := First[entities.Account](v, indexName)
	requireT.True(exists)
	requireT.Equal(accs[4], acc)

	acc, exists = First[entities.Account](v, indexName, "Last2")
	requireT.True(exists)
	requireT.Equal(accs[1], acc)

	acc, exists = First[entities.Account](v, indexName, "Last")
	requireT.True(exists)
	requireT.Equal(accs[3], acc)

	acc, exists = First[entities.Account](v, indexName, "La")
	requireT.True(exists)
	requireT.Equal(accs[4], acc)

	_, exists = First[entities.Account](v, indexName, "Las")
	requireT.False(exists)

	acc, exists = First[entities.Account](v, indexName, "Last2", "First3")
	requireT.True(exists)
	requireT.Equal(accs[0], acc)

	_, exists = First[entities.Account](v, indexName, "Last2", "Fir")
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
	requireT.Equal(5, i)

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
	requireT.Equal(2, i)

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
	requireT.Equal(1, i)

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

	ctx := newContext(t)

	requireT := require.New(t)

	var acc entities.Account
	indexName := indices.NewMultiIndex(
		indices.NewIfIndex[entities.Account](
			indices.NewFieldIndex(&acc, &acc.LastName), func(e *entities.Account) bool {
				return strings.HasPrefix(e.LastName, "A")
			}),
		indices.NewIfIndex[entities.Account](
			indices.NewFieldIndex(&acc, &acc.FirstName), func(e *entities.Account) bool {
				return strings.HasPrefix(e.FirstName, "B")
			}),
	)

	c := NewTestClient(t, withIndices(config, indexName))

	accs := []entities.Account{
		{
			ID:        memdb.NewID[entities.AccountID](),
			FirstName: "C1",
			LastName:  "A1",
		},
		{
			ID:        memdb.NewID[entities.AccountID](),
			FirstName: "B2",
			LastName:  "C2",
		},
		{
			ID:        memdb.NewID[entities.AccountID](),
			FirstName: "B1",
			LastName:  "A1",
		},
		{
			ID:        memdb.NewID[entities.AccountID](),
			FirstName: "B2",
			LastName:  "A2",
		},
	}

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx *Tx) error {
		for _, acc := range accs {
			tx.Set(acc)
		}
		return nil
	}))

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

func TestReverseIndex(t *testing.T) {
	t.Parallel()

	ctx := newContext(t)

	requireT := require.New(t)

	var acc entities.Account
	indexLastName := indices.NewReverseIndex(indices.NewFieldIndex(&acc, &acc.LastName))

	c := NewTestClient(t, withIndices(config, indexLastName))

	accs := []entities.Account{
		{
			ID:        memdb.NewID[entities.AccountID](),
			FirstName: "First3",
			LastName:  "Last3",
		},
		{
			ID:        memdb.NewID[entities.AccountID](),
			FirstName: "First2",
			LastName:  "Last2",
		},
		{
			ID:        memdb.NewID[entities.AccountID](),
			FirstName: "First1",
			LastName:  "Last1",
		},
	}

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx *Tx) error {
		for _, acc := range accs {
			tx.Set(acc)
		}
		return nil
	}))

	for i := range accs {
		accs[i].Revision = 1
	}

	v := c.View()
	acc, exists := First[entities.Account](v, indexLastName)
	requireT.True(exists)
	requireT.Equal(accs[0], acc)

	acc, exists = First[entities.Account](v, indexLastName, "Last2")
	requireT.True(exists)
	requireT.Equal(accs[1], acc)

	_, exists = First[entities.Account](v, indexLastName, "Missing")
	requireT.False(exists)

	i := 0
	for acc := range Iterate[entities.Account](v, indexLastName) {
		switch i {
		case 0:
			requireT.Equal(accs[0], acc)
		case 1:
			requireT.Equal(accs[1], acc)
		case 2:
			requireT.Equal(accs[2], acc)
		default:
			requireT.Fail("wrong index")
		}
		i++
	}
	requireT.Equal(3, i)

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
	requireT.Equal(1, i)

	for range Iterate[entities.Account](v, indexLastName, "Missing") {
		requireT.Fail("nothing should be returned")
	}

	it := Iterator[entities.Account](v, indexLastName)
	acc, ok := it()
	requireT.True(ok)
	requireT.Equal(accs[0], acc)
	acc, ok = it()
	requireT.True(ok)
	requireT.Equal(accs[1], acc)
	acc, ok = it()
	requireT.True(ok)
	requireT.Equal(accs[2], acc)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Account](v, indexLastName, "Last2")
	acc, ok = it()
	requireT.True(ok)
	requireT.Equal(accs[1], acc)
	_, ok = it()
	requireT.False(ok)

	it = Iterator[entities.Account](v, indexLastName, "Missing")
	_, ok = it()
	requireT.False(ok)
}

func newContext(t *testing.T) context.Context {
	return logger.WithLogger(t.Context(), logger.New(logger.DefaultConfig))
}
