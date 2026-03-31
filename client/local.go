package client

import (
	"context"
	"reflect"

	"github.com/pkg/errors"

	"github.com/outofforest/memdb"
)

var (
	_ ReadClient  = &LocalClient{}
	_ WriteClient = &LocalClient{}
	_ Transactor  = &localTransactor{}
	_ Tx          = &localTx{}
)

// LocalConfig is the config for local client.
type LocalConfig struct {
	Indices     []memdb.Index
	TriggerFunc TriggerFunc
	Types       []reflect.Type
}

// LocalClient is the client storing entities locally without propagating transactions to the server.
type LocalClient struct {
	config LocalConfig
	db     *memdb.MemDB
	byType map[reflect.Type]typeInfo
}

// NewLocalClient creates static client.
func NewLocalClient(config LocalConfig) (*LocalClient, error) {
	byType := map[reflect.Type]typeInfo{}

	dbConfig := memdb.Config{
		Entities: config.Types,
		Indices:  config.Indices,
	}

	for _, t := range dbConfig.Entities {
		if _, exists := byType[t]; exists {
			continue
		}

		idFType, err := validateType(t, false)
		if err != nil {
			return nil, err
		}

		info := typeInfo{
			Type:    t,
			IDType:  idFType,
			TableID: uint64(len(byType)),
		}
		byType[t] = info
	}

	db, err := memdb.NewMemDB(dbConfig)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &LocalClient{
		config: config,
		db:     db,
		byType: byType,
	}, nil
}

// WarmUp waits until hot end is reached for the first time.
func (c *LocalClient) WarmUp(ctx context.Context) error {
	return nil
}

// View returns db view.
func (c *LocalClient) View() *View {
	return &View{
		tx:     c.db.Txn(true),
		byType: c.byType,
	}
}

// NewTransactor creates new transactor.
func (c *LocalClient) NewTransactor() Transactor {
	return &localTransactor{
		c: c,
	}
}

type localTransactor struct {
	c *LocalClient
}

func (t *localTransactor) Tx(ctx context.Context, txF func(tx Tx) error) error {
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

func (tx *localTx) View() *View {
	return &View{
		tx:     tx.txn.Txn(true),
		byType: tx.client.byType,
	}
}

func (tx *localTx) Set(o any) error {
	_, typeDef, _, _ := insert(tx.txn, tx.client.byType, o)
	tx.updatedTypes[typeDef.Type] = struct{}{}
	return nil
}

func (tx *localTx) SoftSet(o any) error {
	return tx.Set(o)
}
