package client

import (
	"context"
	"reflect"

	"github.com/pkg/errors"

	"github.com/outofforest/memdb"
)

// StaticConfig is the config for static client.
type StaticConfig struct {
	Indices     []memdb.Index
	TriggerFunc TriggerFunc
}

// NewStaticClient creates static client.
func NewStaticClient(config StaticConfig, entities []any) (*StaticClient, error) {
	byType := map[reflect.Type]typeInfo{}
	dbIndexes := [][]memdb.Index{}
	for _, e := range entities {
		t := reflect.TypeOf(e)
		if _, exists := byType[t]; exists {
			continue
		}

		if err := validateType(t); err != nil {
			return nil, err
		}

		info := typeInfo{
			Type:    t,
			TableID: uint64(len(byType)),
		}
		byType[t] = info

		dbIndexes = buildDBIndexesForType(dbIndexes, config.Indices, t)
	}

	db, err := memdb.NewMemDB(dbIndexes)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &StaticClient{
		config:   config,
		db:       db,
		byType:   byType,
		entities: entities,
	}, nil
}

// StaticClient is the read-only client serving preloaded entities.
type StaticClient struct {
	config   StaticConfig
	db       *memdb.MemDB
	byType   map[reflect.Type]typeInfo
	loaded   bool
	entities []any
}

// WarmUp waits until hot end is reached for the first time.
func (c *StaticClient) WarmUp(ctx context.Context) error {
	if c.loaded {
		return nil
	}
	c.loaded = true

	tx := c.db.Txn(true)
	for _, e := range c.entities {
		insert(tx, c.byType, e)
	}
	tx.Commit()
	c.entities = nil

	if c.config.TriggerFunc != nil {
		if err := c.config.TriggerFunc(ctx, c.View()); err != nil {
			return err
		}
	}
	return nil
}

// View returns db view.
func (c *StaticClient) View() *View {
	return &View{
		tx:     c.db.Txn(false),
		byType: c.byType,
	}
}
