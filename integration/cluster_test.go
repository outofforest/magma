package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/outofforest/logger"
	"github.com/outofforest/magma/client"
	"github.com/outofforest/magma/integration/entities"
	"github.com/outofforest/magma/integration/system"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/parallel"
)

func TestCluster(t *testing.T) {
	requireT := require.New(t)
	ctx := system.NewContext(t)

	peers := []*system.Peer{
		system.NewPeer(t, "P1", types.Partitions{"default": true}),
		system.NewPeer(t, "P2", types.Partitions{"default": true}),
		system.NewPeer(t, "P3", types.Partitions{"default": true}),
	}

	clients := make([]*system.Client, 0, len(peers))
	ids := make([]entities.AccountID, 0, len(peers))
	idCh := make(chan entities.AccountID, len(peers))
	for i, peer := range peers {
		clients = append(clients, system.NewClient(t, peer, fmt.Sprintf("client-%d", i), "default",
			nil))
		id := client.NewID[entities.AccountID]()
		ids = append(ids, id)
		idCh <- id
	}

	cluster := system.NewCluster(peers...)
	cluster.StartPeers(ctx, peers...)
	cluster.StartClients(ctx, clients...)

	clientGroup := parallel.NewGroup(ctx)
	groupClients := parallel.NewSubgroup(clientGroup.Spawn, "clients", parallel.Continue)
	for _, c := range clients {
		groupClients.Spawn("client", parallel.Continue, func(ctx context.Context) error {
			tr := c.NewTransactor()
			err := tr.Tx(ctx, func(tx *client.Tx) error {
				tx.Set(entities.Account{
					ID:        <-idCh,
					FirstName: "FirstName",
					LastName:  "LastName",
				})
				return nil
			})
			if err != nil {
				return err
			}

			return nil
		})
	}
	if err := groupClients.Wait(); err != nil {
		logger.Get(ctx).Error("Error", zap.Error(err))
		return
	}

	for _, c := range clients {
		view := c.View()
		for _, id := range ids {
			acc, exists := client.Get[entities.Account](view, id)
			requireT.True(exists)
			requireT.Equal(entities.Account{
				ID:        id,
				FirstName: "FirstName",
				LastName:  "LastName",
			}, acc)
		}
	}
}
