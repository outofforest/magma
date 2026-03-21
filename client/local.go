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

// NewStaticClient creates static client.
func NewStaticClient(config LocalConfig) (*LocalClient, error) {
	byType := map[reflect.Type]typeInfo{}
	dbIndexes := [][]memdb.Index{}
	for _, t := range config.Types {
		if _, exists := byType[t]; exists {
			continue
		}

		idFType, err := validateType(t)
		if err != nil {
			return nil, err
		}

		info := typeInfo{
			Type:    t,
			IDType:  idFType,
			TableID: uint64(len(byType)),
		}
		byType[t] = info

		dbIndexes = buildDBIndexesForType(dbIndexes, config.Indices, t)
	}

	db, err := memdb.NewMemDB(dbIndexes)
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
		tx:     c.db.Txn(false),
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
	txn := t.c.db.Txn(true)

	pendingTx := &localTx{
		client: t.c,
		txn:    txn,
		ids:    map[any]struct{}{},
	}

	if err := txF(pendingTx); err != nil {
		return err
	}

	txn.Commit()

	if t.c.config.TriggerFunc != nil {
		if err := t.c.config.TriggerFunc(ctx, t.c.View(), pendingTx.ids); err != nil {
			return err
		}
	}

	return nil
}

type localTx struct {
	client *LocalClient
	txn    *memdb.Txn
	ids    map[any]struct{}
}

func (tx *localTx) View() *View {
	return &View{
		tx:     tx.txn,
		byType: tx.client.byType,
	}
}

func (tx *localTx) Set(o any) error {
	id, typeDef, _, _ := insert(tx.txn, tx.client.byType, o)
	tx.ids[reflect.ValueOf(id).Convert(typeDef.IDType).Interface()] = struct{}{}
	return nil
}

func (tx *localTx) SoftSet(o any) error {
	return tx.Set(o)
}
