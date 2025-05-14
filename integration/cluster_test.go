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

const (
	partitionDefault types.PartitionID = "default"
	partition1       types.PartitionID = "partition1"
	partition2       types.PartitionID = "partition2"
	partition3       types.PartitionID = "partition3"
)

func TestSinglePeer(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	ctx := system.NewContext(t)

	p := system.NewPeer(t, "P", types.Partitions{partitionDefault: true})
	c := system.NewClient(t, p, "client", partitionDefault, nil)

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
		Revision:  1,
		FirstName: "FirstName",
		LastName:  "LastName",
	}, account)
}

func Test3Peers3Clients(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	ctx := system.NewContext(t)

	peers := []*system.Peer{
		system.NewPeer(t, "P1", types.Partitions{partitionDefault: true}),
		system.NewPeer(t, "P2", types.Partitions{partitionDefault: true}),
		system.NewPeer(t, "P3", types.Partitions{partitionDefault: true}),
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
		clients = append(clients, system.NewClient(t, peer, fmt.Sprintf("client-%d", i), partitionDefault,
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
					Revision:  1,
					FirstName: "FirstName",
					LastName:  "LastName",
				}, acc)
			}
		}
	}
}

func TestPeerRestart(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	ctx := system.NewContext(t)

	peers := []*system.Peer{
		system.NewPeer(t, "P1", types.Partitions{partitionDefault: true}),
		system.NewPeer(t, "P2", types.Partitions{partitionDefault: true}),
		system.NewPeer(t, "P3", types.Partitions{partitionDefault: true}),
	}

	clients := make([]*system.Client, 0, len(peers))
	for i, peer := range peers {
		clients = append(clients, system.NewClient(t, peer, fmt.Sprintf("client-%d", i), partitionDefault,
			nil))
	}

	cluster := system.NewCluster(peers...)
	cluster.StartPeers(ctx, peers...)
	cluster.StartClients(ctx, clients...)

	for i := range 3 * len(peers) {
		pI := i % len(peers)
		cI := (i + 1) % len(clients)

		cluster.StopPeers(peers[pI])

	loop:
		for j := range 5 {
			err := clients[cI].NewTransactor().Tx(ctx, func(tx *client.Tx) error {
				tx.Set(entities.Account{
					ID:        client.NewID[entities.AccountID](),
					FirstName: "FirstName",
					LastName:  "LastName",
				})
				return nil
			})
			switch {
			case err == nil:
				break loop
			case errors.Is(err, client.ErrBroadcastTimeout):
			case errors.Is(err, client.ErrAwaitTimeout):
			default:
				requireT.NoError(err)
			}

			if j == 4 {
				requireT.Fail("sending transaction failed")
			}
		}

		cluster.StartPeers(ctx, peers[pI])
	}
}

func TestPassivePeer(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	ctx := system.NewContext(t)

	peer1 := system.NewPeer(t, "P1", types.Partitions{partitionDefault: true})
	peer2 := system.NewPeer(t, "P2", types.Partitions{partitionDefault: true})
	peerObserver := system.NewPeer(t, "PO", types.Partitions{partitionDefault: false})

	c := system.NewClient(t, peer1, "client", partitionDefault, nil)

	cluster := system.NewCluster(peer1, peer2, peerObserver)
	cluster.StartPeers(ctx, peer1, peer2)
	cluster.StartClients(ctx, c)

	accs := []entities.Account{
		{
			ID:        client.NewID[entities.AccountID](),
			FirstName: "FirstName1",
			LastName:  "LastName1",
		},
		{
			ID:        client.NewID[entities.AccountID](),
			FirstName: "FirstName2",
			LastName:  "LastName2",
		},
		{
			ID:        client.NewID[entities.AccountID](),
			FirstName: "FirstName3",
			LastName:  "LastName3",
		},
	}

	tr := c.NewTransactor()
	for _, acc := range accs {
		requireT.NoError(tr.Tx(ctx, func(tx *client.Tx) error {
			tx.Set(acc)
			return nil
		}))
	}

	cluster.StopClients(c)

	c = system.NewClient(t, peerObserver, "client", partitionDefault, nil)
	cluster.StartPeers(ctx, peerObserver)
	cluster.StartClients(ctx, c)

	v := c.View()
	for _, acc := range accs {
		acc.Revision = 1
		acc2, exists := client.Get[entities.Account](v, acc.ID)
		requireT.True(exists)
		requireT.Equal(acc, acc2)
	}
}

func TestSync(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	ctx := system.NewContext(t)

	peer1 := system.NewPeer(t, "P1", types.Partitions{partitionDefault: true})
	peer2 := system.NewPeer(t, "P2", types.Partitions{partitionDefault: true})
	peer3 := system.NewPeer(t, "P3", types.Partitions{partitionDefault: true})

	c := system.NewClient(t, peer1, "client", partitionDefault, nil)

	cluster := system.NewCluster(peer1, peer2, peer3)
	cluster.StartPeers(ctx, peer1, peer2)
	cluster.StartClients(ctx, c)

	accID := client.NewID[entities.AccountID]()
	accs := []entities.Account{
		{
			ID:        accID,
			FirstName: "FirstName0",
			LastName:  "LastName0",
		},
		{
			ID:        accID,
			FirstName: "FirstName1",
			LastName:  "LastName1",
		},
		{
			ID:        accID,
			FirstName: "FirstName2",
			LastName:  "LastName2",
		},
		{
			ID:        accID,
			FirstName: "FirstName3",
			LastName:  "LastName3",
		},
		{
			ID:        accID,
			FirstName: "FirstName4",
			LastName:  "LastName4",
		},
		{
			ID:        accID,
			FirstName: "FirstName5",
			LastName:  "LastName5",
		},
	}

	tr := c.NewTransactor()
	for _, acc := range accs[:3] {
		requireT.NoError(tr.Tx(ctx, func(tx *client.Tx) error {
			tx.Set(acc)
			return nil
		}))
	}

	accCh1 := make(chan entities.Account, 1)
	accCh2 := make(chan entities.Account, 1)
	triggerFunc := func(accCh chan<- entities.Account) func(ctx context.Context, v *client.View) error {
		return func(ctx context.Context, v *client.View) error {
			a, exists := client.Get[entities.Account](v, accID)
			if exists {
				accCh <- a
			}
			return nil
		}
	}

	c2 := system.NewClient(t, peer3, "client2", partitionDefault, triggerFunc(accCh1))
	c3 := system.NewClient(t, peer3, "client3", partitionDefault, triggerFunc(accCh2))
	cluster.StartPeers(ctx, peer3)
	cluster.StartClients(ctx, c2, c3)
	cluster.StopClients(c2)
	cluster.StopPeers(peer3)

	requireT.NotEmpty(accCh1)
	acc := <-accCh1
	acc.Revision = 0
	requireT.Equal(accs[2], acc)

	requireT.NotEmpty(accCh2)
	acc = <-accCh2
	acc.Revision = 0
	requireT.Equal(accs[2], acc)

	for _, acc := range accs[3:] {
		requireT.NoError(tr.Tx(ctx, func(tx *client.Tx) error {
			tx.Set(acc)
			return nil
		}))
	}

	c2 = system.NewClient(t, peer3, "client2", partitionDefault, triggerFunc(accCh1))

	cluster.StartPeers(ctx, peer3)
	cluster.StartClients(ctx, c2)
	cluster.StopClients(c2)
	cluster.StopPeers(peer3)

	requireT.NotEmpty(accCh1)
	acc = <-accCh1
	acc.Revision = 0
	requireT.Equal(accs[5], acc)

	select {
	case <-ctx.Done():
		requireT.NoError(ctx.Err())
	case acc := <-accCh2:
		acc.Revision = 0
		requireT.Equal(accs[5], acc)
	}
}

func TestPartitions(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	ctx := system.NewContext(t)

	peer1 := system.NewPeer(t, "P1", types.Partitions{partition1: true, partition2: true})
	peer2 := system.NewPeer(t, "P2", types.Partitions{partition2: true, partition3: true})
	peer3 := system.NewPeer(t, "P3", types.Partitions{partition3: true, partition1: true})

	c1 := system.NewClient(t, peer1, "client1", partition1, nil)
	c2 := system.NewClient(t, peer2, "client2", partition2, nil)
	c3 := system.NewClient(t, peer3, "client3", partition3, nil)

	cluster := system.NewCluster(peer1, peer2, peer3)
	cluster.StartPeers(ctx, peer1, peer2, peer3)
	cluster.StartClients(ctx, c1, c2, c3)

	accs := [][]entities.Account{
		{
			{
				ID:        client.NewID[entities.AccountID](),
				FirstName: "P1FirstName0",
				LastName:  "P1LastName0",
			},
			{
				ID:        client.NewID[entities.AccountID](),
				FirstName: "P1FirstName1",
				LastName:  "P1LastName1",
			},
			{
				ID:        client.NewID[entities.AccountID](),
				FirstName: "P1FirstName2",
				LastName:  "P1LastName2",
			},
		},
		{
			{
				ID:        client.NewID[entities.AccountID](),
				FirstName: "P2FirstName0",
				LastName:  "P2LastName0",
			},
			{
				ID:        client.NewID[entities.AccountID](),
				FirstName: "P2FirstName1",
				LastName:  "P2LastName1",
			},
			{
				ID:        client.NewID[entities.AccountID](),
				FirstName: "P2FirstName2",
				LastName:  "P2LastName2",
			},
		},
		{
			{
				ID:        client.NewID[entities.AccountID](),
				FirstName: "P3FirstName0",
				LastName:  "P3LastName0",
			},
			{
				ID:        client.NewID[entities.AccountID](),
				FirstName: "P3FirstName1",
				LastName:  "P3LastName1",
			},
			{
				ID:        client.NewID[entities.AccountID](),
				FirstName: "P3FirstName2",
				LastName:  "P3LastName2",
			},
		},
	}

	clientGroup := parallel.NewGroup(ctx)
	for i, c := range []*system.Client{c1, c2, c3} {
		clientGroup.Spawn("client", parallel.Continue, func(ctx context.Context) error {
			tr := c.NewTransactor()
			for _, acc := range accs[i] {
				err := tr.Tx(ctx, func(tx *client.Tx) error {
					tx.Set(acc)
					return nil
				})
				if err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err := clientGroup.Wait(); err != nil {
		logger.Get(ctx).Error("Error", zap.Error(err))
		return
	}

	cluster.StopClients(c1, c2, c3)

	c1 = system.NewClient(t, peer3, "client1", partition1, nil)
	c2 = system.NewClient(t, peer1, "client2", partition2, nil)
	c3 = system.NewClient(t, peer2, "client3", partition3, nil)

	cluster.StartClients(ctx, c1, c2, c3)

	for i, c := range []*system.Client{c1, c2, c3} {
		v := c.View()
		for j, accs := range accs {
			for _, acc := range accs {
				acc2, exists := client.Get[entities.Account](v, acc.ID)
				if i == j {
					requireT.True(exists)
					acc2.Revision = 0
					requireT.Equal(acc, acc2)
				} else {
					requireT.False(exists)
				}
			}
		}
	}
}
