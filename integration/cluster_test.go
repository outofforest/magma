package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/outofforest/logger"
	"github.com/outofforest/magma/client"
	"github.com/outofforest/magma/integration/entities"
	"github.com/outofforest/magma/integration/system"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/parallel"
)

func TestClusterSinglePeer(t *testing.T) {
	requireT := require.New(t)
	ctx := system.NewContext(t)

	p := system.NewPeer(t, "P", types.Partitions{"default": true})
	c := system.NewClient(t, p, "client", "default", nil)

	cluster := system.NewCluster(p)
	cluster.StartPeers(ctx, p)
	cluster.StartClients(ctx, c)

	accountID := client.NewID[entities.AccountID]()

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx *client.Tx) error {
		tx.Set(entities.Account{
			ID:        accountID,
			FirstName: "FirstName",
			LastName:  "LastName",
		})
		return nil
	}))

	v := c.View()
	account, exists := client.Get[entities.Account](v, accountID)
	requireT.True(exists)
	requireT.Equal(entities.Account{
		ID:        accountID,
		FirstName: "FirstName",
		LastName:  "LastName",
	}, account)
}

func TestCluster3Peers3Clients(t *testing.T) {
	requireT := require.New(t)
	ctx := system.NewContext(t)

	peers := []*system.Peer{
		system.NewPeer(t, "P1", types.Partitions{"default": true}),
		system.NewPeer(t, "P2", types.Partitions{"default": true}),
		system.NewPeer(t, "P3", types.Partitions{"default": true}),
	}

	ids := make([]entities.AccountID, 0, len(peers))
	results := make(chan map[entities.AccountID]entities.Account, 3)
	triggerFunc := func() func(ctx context.Context, v *client.View) error {
		var found bool
		return func(ctx context.Context, v *client.View) error {
			if found {
				return nil
			}

			accounts := map[entities.AccountID]entities.Account{}
			for _, id := range ids {
				acc, exists := client.Get[entities.Account](v, id)
				if !exists {
					return nil
				}
				accounts[id] = acc
			}

			found = true

			select {
			case <-ctx.Done():
				return errors.WithStack(ctx.Err())
			case results <- accounts:
				return nil
			}
		}
	}

	clients := make([]*system.Client, 0, len(peers))
	idCh := make(chan entities.AccountID, len(peers))
	for i, peer := range peers {
		clients = append(clients, system.NewClient(t, peer, fmt.Sprintf("client-%d", i), "default",
			triggerFunc()))
		id := client.NewID[entities.AccountID]()
		ids = append(ids, id)
		idCh <- id
	}

	cluster := system.NewCluster(peers...)
	cluster.StartPeers(ctx, peers...)
	cluster.StartClients(ctx, clients...)

	clientGroup := parallel.NewGroup(ctx)
	for _, c := range clients {
		clientGroup.Spawn("client", parallel.Continue, func(ctx context.Context) error {
			return c.NewTransactor().Tx(ctx, func(tx *client.Tx) error {
				tx.Set(entities.Account{
					ID:        <-idCh,
					FirstName: "FirstName",
					LastName:  "LastName",
				})
				return nil
			})
		})
	}
	if err := clientGroup.Wait(); err != nil {
		logger.Get(ctx).Error("Error", zap.Error(err))
		return
	}

	for range clients {
		select {
		case <-ctx.Done():
			requireT.NoError(ctx.Err())
		case accounts := <-results:
			for _, id := range ids {
				acc, exists := accounts[id]
				requireT.True(exists)
				requireT.Equal(entities.Account{
					ID:        id,
					FirstName: "FirstName",
					LastName:  "LastName",
				}, acc)
			}
		}
	}
}
