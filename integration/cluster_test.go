package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/client"
	"github.com/outofforest/magma/integration/entities"
	"github.com/outofforest/magma/integration/system"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/memdb"
	"github.com/outofforest/memdb/indices"
	"github.com/outofforest/parallel"
)

const (
	partitionDefault types.PartitionID = "default"
	partition1       types.PartitionID = "partition1"
	partition2       types.PartitionID = "partition2"
	partition3       types.PartitionID = "partition3"
)

func TestBenchmark(t *testing.T) {
	t.Parallel()

	const (
		numOfClients          = 20
		transactionsPerClient = 50
	)

	requireT := require.New(t)

	peers := []*system.Peer{
		system.NewPeer(t, "P1", types.Partitions{partitionDefault: true}),
		system.NewPeer(t, "P2", types.Partitions{partitionDefault: true}),
		system.NewPeer(t, "P3", types.Partitions{partitionDefault: true}),
	}

	var e entities.Account
	indexLastName := indices.NewFieldIndex(&e, &e.LastName)
	indexFirstName := indices.NewFieldIndex(&e, &e.FirstName)
	indexName := indices.NewMultiIndex(indexLastName, indexFirstName)

	clients := make([]*system.Client, 0, numOfClients)
	for i := range numOfClients {
		clients = append(clients, system.NewClient(t, peers[i%len(peers)], fmt.Sprintf("client-%d", i),
			partitionDefault, nil, indexLastName, indexFirstName, indexName))
	}

	cluster, ctx := system.NewCluster(t, peers...)
	cluster.StartPeers(peers...)
	cluster.StartClients(clients...)

	clientGroup := parallel.NewGroup(ctx)
	for i, c := range clients {
		clientGroup.Spawn("client", parallel.Continue, func(ctx context.Context) error {
			tr := c.NewTransactor()
			for j := range transactionsPerClient {
				err := tr.Tx(ctx, func(tx *client.Tx) error {
					tx.Set(entities.Account{
						ID:        memdb.NewID[entities.AccountID](),
						FirstName: fmt.Sprintf("FirstName-%d-%d", i, j),
						LastName:  fmt.Sprintf("LastName-%d-%d", i, j),
					})
					return nil
				})
				if err != nil {
					return err
				}
			}
			return nil
		})
	}
	requireT.NoError(clientGroup.Wait())
}

func TestSinglePeer(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	p := system.NewPeer(t, "P", types.Partitions{partitionDefault: true})
	c := system.NewClient(t, p, "client", partitionDefault, nil)

	cluster, ctx := system.NewCluster(t, p)
	cluster.StartPeers(p)
	cluster.StartClients(c)

	accountID := memdb.NewID[entities.AccountID]()

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
		id := memdb.NewID[entities.AccountID]()
		ids = append(ids, id)
		idCh <- id
	}

	cluster, ctx := system.NewCluster(t, peers...)
	cluster.StartPeers(peers...)
	cluster.StartClients(clients...)

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
	requireT.NoError(clientGroup.Wait())

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

	cluster, ctx := system.NewCluster(t, peers...)
	cluster.StartPeers(peers...)
	cluster.StartClients(clients...)

	for i := range 2 * len(peers) {
		pI := i % len(peers)
		cI := (i + 1) % len(clients)

		cluster.StopPeers(peers[pI])

	loop:
		for j := range 5 {
			err := clients[cI].NewTransactor().Tx(ctx, func(tx *client.Tx) error {
				tx.Set(entities.Account{
					ID:        memdb.NewID[entities.AccountID](),
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

		cluster.StartPeers(peers[pI])
	}
}

func TestPassivePeer(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	peer1 := system.NewPeer(t, "P1", types.Partitions{partitionDefault: true})
	peer2 := system.NewPeer(t, "P2", types.Partitions{partitionDefault: true})
	peerObserver := system.NewPeer(t, "PO", types.Partitions{partitionDefault: false})

	c := system.NewClient(t, peer1, "client", partitionDefault, nil)

	cluster, ctx := system.NewCluster(t, peer1, peer2, peerObserver)
	cluster.StartPeers(peer1, peer2)
	cluster.StartClients(c)

	accs := []entities.Account{
		{
			ID:        memdb.NewID[entities.AccountID](),
			FirstName: "FirstName1",
			LastName:  "LastName1",
		},
		{
			ID:        memdb.NewID[entities.AccountID](),
			FirstName: "FirstName2",
			LastName:  "LastName2",
		},
		{
			ID:        memdb.NewID[entities.AccountID](),
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
	cluster.StartPeers(peerObserver)
	cluster.StartClients(c)

	v := c.View()
	for _, acc := range accs {
		acc.Revision = 1
		acc2, exists := client.Get[entities.Account](v, acc.ID)
		requireT.True(exists)
		requireT.Equal(acc, acc2)
	}
}

func TestSyncWhileRunning(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	peer1 := system.NewPeer(t, "P1", types.Partitions{partitionDefault: true})
	peer2 := system.NewPeer(t, "P2", types.Partitions{partitionDefault: true})
	peer3 := system.NewPeer(t, "P3", types.Partitions{partitionDefault: true})

	c := system.NewClient(t, peer1, "client", partitionDefault, nil)

	cluster, ctx := system.NewCluster(t, peer1, peer2, peer3)
	cluster.StartPeers(peer1, peer2)
	cluster.StartClients(c)

	accID := memdb.NewID[entities.AccountID]()
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

	cluster.StartPeers(peer3)
	c2 := system.NewClient(t, peer3, "client2", partitionDefault, nil)
	cluster.StartClients(c2)
	cluster.StopClients(c2)

	cluster.DisableLink(peer3, peer1)
	cluster.DisableLink(peer3, peer2)

	for _, acc := range accs[3:] {
		requireT.NoError(tr.Tx(ctx, func(tx *client.Tx) error {
			tx.Set(acc)
			return nil
		}))
	}

	c2 = system.NewClient(t, peer3, "client2", partitionDefault, nil)
	cluster.StartClients(c2)

	tr2 := c2.NewTransactor()
	requireT.Error(tr2.Tx(ctx, func(tx *client.Tx) error {
		tx.Set(entities.Account{
			ID:        memdb.NewID[entities.AccountID](),
			FirstName: "FirstName100",
			LastName:  "LastName100",
		})
		return nil
	}))

	c3 := system.NewClient(t, peer3, "client3", partitionDefault, nil)
	cluster.StartClients(c3)

	v := c3.View()
	acc, exists := client.Get[entities.Account](v, accID)
	requireT.True(exists)
	acc.Revision = 0
	requireT.Equal(accs[2], acc)

	cluster.EnableLink(peer3, peer1)
	cluster.EnableLink(peer3, peer2)

	var err error
	for range 10 {
		err = tr2.Tx(ctx, func(tx *client.Tx) error {
			tx.Set(entities.Account{
				ID:        memdb.NewID[entities.AccountID](),
				FirstName: "FirstName100",
				LastName:  "LastName100",
			})
			return nil
		})
		if err == nil {
			break
		}
	}
	requireT.NoError(err)

	v = c2.View()
	acc, exists = client.Get[entities.Account](v, accID)
	requireT.True(exists)
	acc.Revision = 0
	requireT.Equal(accs[5], acc)
}

func TestSyncAfterRestart(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	peer1 := system.NewPeer(t, "P1", types.Partitions{partitionDefault: true})
	peer2 := system.NewPeer(t, "P2", types.Partitions{partitionDefault: true})
	peer3 := system.NewPeer(t, "P3", types.Partitions{partitionDefault: true})

	c := system.NewClient(t, peer1, "client", partitionDefault, nil)

	cluster, ctx := system.NewCluster(t, peer1, peer2, peer3)
	cluster.StartPeers(peer1, peer2)
	cluster.StartClients(c)

	accID := memdb.NewID[entities.AccountID]()
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
				select {
				case accCh <- a:
				default:
				}
			}
			return nil
		}
	}

	c2 := system.NewClient(t, peer3, "client2", partitionDefault, triggerFunc(accCh1))
	c3 := system.NewClient(t, peer3, "client3", partitionDefault, triggerFunc(accCh2))
	cluster.StartPeers(peer3)
	cluster.StartClients(c2, c3)

	requireT.NoError(c2.NewTransactor().Tx(ctx, func(tx *client.Tx) error {
		tx.Set(entities.Account{ID: memdb.NewID[entities.AccountID]()})
		return nil
	}))
	requireT.NoError(c3.NewTransactor().Tx(ctx, func(tx *client.Tx) error {
		tx.Set(entities.Account{ID: memdb.NewID[entities.AccountID]()})
		return nil
	}))

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

	cluster.StartPeers(peer3)
	cluster.StartClients(c2)

	requireT.NoError(c2.NewTransactor().Tx(ctx, func(tx *client.Tx) error {
		tx.Set(entities.Account{ID: memdb.NewID[entities.AccountID]()})
		return nil
	}))
	requireT.NoError(c3.NewTransactor().Tx(ctx, func(tx *client.Tx) error {
		tx.Set(entities.Account{ID: memdb.NewID[entities.AccountID]()})
		return nil
	}))

	cluster.StopClients(c2)
	cluster.StopPeers(peer3)

	requireT.NotEmpty(accCh1)
	acc = <-accCh1
	acc.Revision = 0
	requireT.Equal(accs[5], acc)

	requireT.NotEmpty(accCh2)
	acc = <-accCh2
	acc.Revision = 0
	requireT.Equal(accs[5], acc)
}

func TestPartitions(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	peer1 := system.NewPeer(t, "P1", types.Partitions{partition1: true, partition2: true})
	peer2 := system.NewPeer(t, "P2", types.Partitions{partition2: true, partition3: true})
	peer3 := system.NewPeer(t, "P3", types.Partitions{partition3: true, partition1: true})

	c1 := system.NewClient(t, peer1, "client1", partition1, nil)
	c2 := system.NewClient(t, peer2, "client2", partition2, nil)
	c3 := system.NewClient(t, peer3, "client3", partition3, nil)

	cluster, ctx := system.NewCluster(t, peer1, peer2, peer3)
	cluster.StartPeers(peer1, peer2, peer3)
	cluster.StartClients(c1, c2, c3)

	accs := [][]entities.Account{
		{
			{
				ID:        memdb.NewID[entities.AccountID](),
				FirstName: "P1FirstName0",
				LastName:  "P1LastName0",
			},
			{
				ID:        memdb.NewID[entities.AccountID](),
				FirstName: "P1FirstName1",
				LastName:  "P1LastName1",
			},
			{
				ID:        memdb.NewID[entities.AccountID](),
				FirstName: "P1FirstName2",
				LastName:  "P1LastName2",
			},
		},
		{
			{
				ID:        memdb.NewID[entities.AccountID](),
				FirstName: "P2FirstName0",
				LastName:  "P2LastName0",
			},
			{
				ID:        memdb.NewID[entities.AccountID](),
				FirstName: "P2FirstName1",
				LastName:  "P2LastName1",
			},
			{
				ID:        memdb.NewID[entities.AccountID](),
				FirstName: "P2FirstName2",
				LastName:  "P2LastName2",
			},
		},
		{
			{
				ID:        memdb.NewID[entities.AccountID](),
				FirstName: "P3FirstName0",
				LastName:  "P3LastName0",
			},
			{
				ID:        memdb.NewID[entities.AccountID](),
				FirstName: "P3FirstName1",
				LastName:  "P3LastName1",
			},
			{
				ID:        memdb.NewID[entities.AccountID](),
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
	requireT.NoError(clientGroup.Wait())

	cluster.StopClients(c1, c2, c3)

	c1 = system.NewClient(t, peer3, "client1", partition1, nil)
	c2 = system.NewClient(t, peer1, "client2", partition2, nil)
	c3 = system.NewClient(t, peer2, "client3", partition3, nil)

	cluster.StartClients(c1, c2, c3)

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

func TestTimeouts(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	peer1 := system.NewPeer(t, "P1", types.Partitions{partitionDefault: true})
	peer2 := system.NewPeer(t, "P2", types.Partitions{partitionDefault: true})
	c := system.NewClient(t, peer1, "client", partitionDefault, nil)

	cluster, ctx := system.NewCluster(t, peer1, peer2)
	cluster.StartPeers(peer1, peer2)
	cluster.StartClients(c)
	cluster.StopPeers(peer2)

	accountID := memdb.NewID[entities.AccountID]()

	tr := c.NewTransactor()
	requireT.ErrorIs(tr.Tx(ctx, func(tx *client.Tx) error {
		tx.Set(entities.Account{
			ID:        accountID,
			FirstName: "FirstName",
			LastName:  "LastName",
		})
		return nil
	}), client.ErrAwaitTimeout)

	cluster.StopPeers(peer1)

	requireT.Error(tr.Tx(ctx, func(tx *client.Tx) error {
		tx.Set(entities.Account{
			ID:        accountID,
			FirstName: "FirstName",
			LastName:  "LastName",
		})
		return nil
	}))

	requireT.ErrorIs(tr.Tx(ctx, func(tx *client.Tx) error {
		tx.Set(entities.Account{
			ID:        accountID,
			FirstName: "FirstName",
			LastName:  "LastName",
		})
		return nil
	}), client.ErrBroadcastTimeout)
}

func TestOutdatedTx(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	peer1 := system.NewPeer(t, "P1", types.Partitions{partitionDefault: true})
	peer2 := system.NewPeer(t, "P2", types.Partitions{partitionDefault: true})
	c1 := system.NewClient(t, peer1, "client1", partitionDefault, nil)
	c2 := system.NewClient(t, peer2, "client2", partitionDefault, nil)

	cluster, ctx := system.NewCluster(t, peer1, peer2)
	cluster.StartPeers(peer1, peer2)
	cluster.StartClients(c1, c2)

	accountID := memdb.NewID[entities.AccountID]()

	requireT.ErrorIs(c1.NewTransactor().Tx(ctx, func(tx *client.Tx) error {
		tx.Set(entities.Account{
			ID:        accountID,
			FirstName: "FirstName1",
			LastName:  "LastName1",
		})

		requireT.NoError(c2.NewTransactor().Tx(ctx, func(tx *client.Tx) error {
			tx.Set(entities.Account{
				ID:        accountID,
				FirstName: "FirstName2",
				LastName:  "LastName2",
			})
			return nil
		}))

		return nil
	}), client.ErrOutdatedTx)

	acc, exists := client.Get[entities.Account](c1.View(), accountID)
	requireT.True(exists)
	requireT.Equal(entities.Account{
		ID:        accountID,
		Revision:  1,
		FirstName: "FirstName2",
		LastName:  "LastName2",
	}, acc)

	acc, exists = client.Get[entities.Account](c2.View(), accountID)
	requireT.True(exists)
	requireT.Equal(entities.Account{
		ID:        accountID,
		Revision:  1,
		FirstName: "FirstName2",
		LastName:  "LastName2",
	}, acc)
}

func TestEmptyTx(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	p := system.NewPeer(t, "P", types.Partitions{partitionDefault: true})
	c := system.NewClient(t, p, "client", partitionDefault, nil)

	cluster, ctx := system.NewCluster(t, p)
	cluster.StartPeers(p)
	cluster.StartClients(c)
	cluster.StopPeers(p)

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx *client.Tx) error {
		return nil
	}))
}

func TestContinueClientSyncAfterPeerIsRestored(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	peer1 := system.NewPeer(t, "P1", types.Partitions{partitionDefault: true})
	peer2 := system.NewPeer(t, "P2", types.Partitions{partitionDefault: true})
	peer3 := system.NewPeer(t, "P3", types.Partitions{partitionDefault: true})

	c := system.NewClient(t, peer1, "client", partitionDefault, nil)

	cluster, ctx := system.NewCluster(t, peer1, peer2, peer3)
	cluster.StartPeers(peer1, peer2, peer3)
	cluster.StartClients(c)

	acc1 := entities.Account{
		ID:        memdb.NewID[entities.AccountID](),
		FirstName: "FirstName0",
		LastName:  "LastName0",
	}
	acc2 := entities.Account{
		ID:        memdb.NewID[entities.AccountID](),
		FirstName: "FirstName2",
		LastName:  "LastName2",
	}

	tr := c.NewTransactor()
	requireT.NoError(tr.Tx(ctx, func(tx *client.Tx) error {
		tx.Set(acc1)
		return nil
	}))

	acc1.FirstName = "FirstName1"
	acc1.LastName = "LastName1"

	requireT.NoError(tr.Tx(ctx, func(tx *client.Tx) error {
		tx.Set(acc1)
		return nil
	}))

	cluster.StopPeers(peer1)
	peer1.DropData()
	cluster.StartPeers(peer1)

	var err error
	for range 5 {
		err = tr.Tx(ctx, func(tx *client.Tx) error {
			tx.Set(acc2)
			return nil
		})
		if err == nil {
			break
		}
	}
	requireT.NoError(err)

	acc1.Revision = 2
	acc2.Revision = 1

	v := c.View()

	acc, exists := client.Get[entities.Account](v, acc1.ID)
	requireT.True(exists)
	requireT.Equal(acc1, acc)

	acc, exists = client.Get[entities.Account](v, acc2.ID)
	requireT.True(exists)
	requireT.Equal(acc2, acc)
}

func TestSplitAndResync(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	peer1 := system.NewPeer(t, "P1", types.Partitions{partitionDefault: true})
	peer2 := system.NewPeer(t, "P2", types.Partitions{partitionDefault: true})
	peer3 := system.NewPeer(t, "P3", types.Partitions{partitionDefault: true})

	cluster, ctx := system.NewCluster(t, peer1, peer2, peer3)
	cluster.ForceLeader(peer3)

	c1 := system.NewClient(t, peer1, "client1", partitionDefault, nil)
	c3 := system.NewClient(t, peer3, "client1", partitionDefault, nil)
	tr1 := c1.NewTransactor()
	tr3 := c3.NewTransactor()

	cluster.StartPeers(peer1, peer2, peer3)
	cluster.StartClients(c1, c3)

	acc1ID := memdb.NewID[entities.AccountID]()
	requireT.NoError(tr1.Tx(ctx, func(tx *client.Tx) error {
		tx.Set(entities.Account{ID: acc1ID})
		return nil
	}))
	acc3ID := memdb.NewID[entities.AccountID]()
	requireT.NoError(tr3.Tx(ctx, func(tx *client.Tx) error {
		tx.Set(entities.Account{ID: acc3ID})
		return nil
	}))

	cluster.ForceLeader(nil)
	cluster.DisableLink(peer3, peer1)
	cluster.DisableLink(peer3, peer2)

	acc4ID := memdb.NewID[entities.AccountID]()
	requireT.ErrorIs(tr3.Tx(ctx, func(tx *client.Tx) error {
		tx.Set(entities.Account{ID: acc4ID})
		return nil
	}), client.ErrAwaitTimeout)
	acc5ID := memdb.NewID[entities.AccountID]()
	requireT.NoError(tr1.Tx(ctx, func(tx *client.Tx) error {
		tx.Set(entities.Account{ID: acc5ID})
		return nil
	}))

	cluster.EnableLink(peer3, peer1)
	cluster.EnableLink(peer3, peer2)

	acc6ID := memdb.NewID[entities.AccountID]()
	var err error
	for range 5 {
		err = c3.NewTransactor().Tx(ctx, func(tx *client.Tx) error {
			tx.Set(entities.Account{ID: acc6ID})
			return nil
		})
		if err == nil {
			break
		}
	}
	requireT.NoError(err)
	requireT.NoError(tr1.Tx(ctx, func(tx *client.Tx) error {
		tx.Set(entities.Account{ID: memdb.NewID[entities.AccountID]()})
		return nil
	}))

	v := c1.View()
	_, exists := client.Get[entities.Account](v, acc1ID)
	requireT.True(exists)
	_, exists = client.Get[entities.Account](v, acc3ID)
	requireT.True(exists)
	_, exists = client.Get[entities.Account](v, acc4ID)
	requireT.False(exists)
	_, exists = client.Get[entities.Account](v, acc5ID)
	requireT.True(exists)
	_, exists = client.Get[entities.Account](v, acc6ID)
	requireT.True(exists)
}
