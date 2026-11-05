package client

import (
	"context"
	"reflect"

	"github.com/pkg/errors"

	"github.com/outofforest/magma/client/wire"
	db2 "github.com/outofforest/magma/db"
	"github.com/outofforest/magma/internal"
	"github.com/outofforest/memdb"
	"github.com/outofforest/proton"
)

var (
	_ ReadClient     = &LocalClient{}
	_ WriteClient    = &LocalClient{}
	_ db2.Transactor = &localTransactor{}
	_ db2.Tx         = &localTx{}
)

// LocalConfig is the config for local client.
type LocalConfig struct {
	Marshaller  proton.Marshaller
	Indices     []memdb.Index
	TriggerFunc TriggerFunc
}

// LocalClient is the client storing entities locally without propagating transactions to the server.
type LocalClient struct {
	config  LocalConfig
	db      *memdb.MemDB
	applier *internal.Applier
}

// NewLocalClient creates static client.
func NewLocalClient(config LocalConfig) (*LocalClient, error) {
	dbConfig, err := internal.NewDBConfig(config.Marshaller, config.Indices)
	if err != nil {
		return nil, err
	}

	applier, err := internal.NewApplier(wire.NewMarshaller(), config.Marshaller)
	if err != nil {
		return nil, err
	}

	db, err := memdb.NewMemDB(dbConfig)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &LocalClient{
		config:  config,
		db:      db,
		applier: applier,
	}, nil
}

// WarmUp waits until hot end is reached for the first time.
func (c *LocalClient) WarmUp(ctx context.Context) error {
	return nil
}

// View returns db view.
func (c *LocalClient) View() *db2.View {
	return internal.NewView(c.db.Txn(true), c.applier)
}

// NewTransactor creates new transactor.
func (c *LocalClient) NewTransactor() db2.Transactor {
	return &localTransactor{
		c: c,
	}
}

type localTransactor struct {
	c *LocalClient
}

func (t *localTransactor) Tx(ctx context.Context, txF func(tx db2.Tx) error) error {
	pendingTx := &localTx{
		client:       t.c,
		txn:          t.c.db.Txn(true),
		updatedTypes: map[reflect.Type]struct{}{},
	}

	if err := txF(pendingTx); err != nil {
		return err
	}

	if len(pendingTx.updatedTypes) == 0 {
		return nil
	}

	pendingTx.txn.Commit()

	if t.c.config.TriggerFunc != nil {
		if err := t.c.config.TriggerFunc(ctx, pendingTx.View(), pendingTx.updatedTypes); err != nil {
			return err
		}
	}

	return nil
}

type localTx struct {
	client       *LocalClient
	txn          *memdb.Txn
	updatedTypes map[reflect.Type]struct{}
}

func (tx *localTx) View() *db2.View {
	return internal.NewView(tx.txn.Txn(true), tx.client.applier)
}

func (tx *localTx) Set(o any) error {
	_, typeDef, _, _ := tx.client.applier.Insert(tx.txn, o)
	tx.updatedTypes[typeDef.Type] = struct{}{}
	return nil
}

func (tx *localTx) SoftSet(o any) error {
	return tx.Set(o)
}
