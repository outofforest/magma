package integration

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/db"
	"github.com/outofforest/magma/integration/entities"
	"github.com/outofforest/magma/integration/entities2"
	"github.com/outofforest/magma/pkg/cluster"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/memdb"
	"github.com/outofforest/qa"
)

func TestSeparateUpgradesSinglePeer(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)

	clusterConfig := clusterConfig
	clusterConfig.Directory = t.TempDir()

	// ======= DesiredVersion 0, Term 1

	clstr := cluster.NewTesting(group, t, clusterConfig)
	m := entities.NewMarshaller()

	p := clstr.NewPeer("P", types.Partitions{partitionDefault: types.PartitionRoleActive})
	c := clstr.NewClient(p, "client", partitionDefault, nil)

	clstr.StartPeers(p)
	clstr.StartClients(c)

	accountID := memdb.NewID[entities.AccountID]()

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		requireT.NoError(tx.Set(entities.Account{
			ID:        accountID,
			FirstName: "FirstName1",
			LastName:  "LastName1",
		}))
		return nil
	}))

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		acc, exists := db.Get[entities.Account](tx.View(), accountID)
		requireT.True(exists)

		acc.FirstName = "FirstName2"
		requireT.NoError(tx.Set(acc))
		return nil
	}))

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		acc, exists := db.Get[entities.Account](tx.View(), accountID)
		requireT.True(exists)

		acc.LastName = "LastName2"
		requireT.NoError(tx.Set(acc))
		return nil
	}))

	// ======= DesiredVersion 0, Term 2

	clstr.StopPeers(p)
	clstr.StartPeers(p)

	// Client c2 is here just to wait until peer is available again.
	c2 := clstr.NewClient(p, "client", partitionDefault, nil)
	clstr.StartClients(c2)
	clstr.StopClients(c2)

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		acc, exists := db.Get[entities.Account](tx.View(), accountID)
		requireT.True(exists)

		acc.FirstName = "FirstName3"
		requireT.NoError(tx.Set(acc))
		return nil
	}))

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		acc, exists := db.Get[entities.Account](tx.View(), accountID)
		requireT.True(exists)

		acc.LastName = "LastName3"
		requireT.NoError(tx.Set(acc))
		return nil
	}))

	group.Exit(nil)
	requireT.NoError(group.Wait())

	// ======= DesiredVersion 1, Term 3

	clusterConfig.Partitions = slices.Clone(clusterConfig.Partitions)
	clusterConfig.Partitions[0].Upgrades = []db.Upgrade{
		{
			Marshaller: m,
			UpgradeFunc: func(view *db.View, ch chan<- any) error {
				for a := range db.All[entities.Account](view) {
					a.FirstName += ".1"
					ch <- a
				}
				return nil
			},
		},
	}

	group = qa.NewGroup(ctx, t)
	clstr = cluster.NewTesting(group, t, clusterConfig)

	p = clstr.NewPeer("P", types.Partitions{partitionDefault: types.PartitionRoleActive})
	c = clstr.NewClient(p, "client", partitionDefault, nil)

	clstr.StartPeers(p)
	clstr.StartClients(c)

	v := c.View()
	account, exists := db.Get[entities.Account](v, accountID)
	requireT.True(exists)
	requireT.Equal(entities.Account{
		ID:        accountID,
		Revision:  0,
		FirstName: "FirstName3.1",
		LastName:  "LastName3",
	}, account)

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		acc, exists := db.Get[entities.Account](tx.View(), accountID)
		requireT.True(exists)

		acc.LastName = "LastName4"
		requireT.NoError(tx.Set(acc))
		return nil
	}))

	group.Exit(nil)
	requireT.NoError(group.Wait())

	// ======= DesiredVersion 2, Term 4

	clusterConfig.Partitions[0].Upgrades = append(clusterConfig.Partitions[0].Upgrades, db.Upgrade{
		Marshaller: m,
		UpgradeFunc: func(view *db.View, ch chan<- any) error {
			for a := range db.All[entities.Account](view) {
				a.LastName += ".2"
				ch <- a
			}
			return nil
		},
	})

	group = qa.NewGroup(ctx, t)
	clstr = cluster.NewTesting(group, t, clusterConfig)

	p = clstr.NewPeer("P", types.Partitions{partitionDefault: types.PartitionRoleActive})
	c = clstr.NewClient(p, "client", partitionDefault, nil)

	clstr.StartPeers(p)
	clstr.StartClients(c)

	v = c.View()
	account, exists = db.Get[entities.Account](v, accountID)
	requireT.True(exists)
	requireT.Equal(entities.Account{
		ID:        accountID,
		Revision:  0,
		FirstName: "FirstName3.1",
		LastName:  "LastName4.2",
	}, account)
}

func TestCombinedUpgradesSinglePeer(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)

	clusterConfig := clusterConfig
	clusterConfig.Directory = t.TempDir()

	// ======= DesiredVersion 0, Term 1

	clstr := cluster.NewTesting(group, t, clusterConfig)
	m := entities.NewMarshaller()

	p := clstr.NewPeer("P", types.Partitions{partitionDefault: types.PartitionRoleActive})
	c := clstr.NewClient(p, "client", partitionDefault, nil)

	clstr.StartPeers(p)
	clstr.StartClients(c)

	accountID := memdb.NewID[entities.AccountID]()

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		requireT.NoError(tx.Set(entities.Account{
			ID:        accountID,
			FirstName: "FirstName1",
			LastName:  "LastName1",
		}))
		return nil
	}))

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		acc, exists := db.Get[entities.Account](tx.View(), accountID)
		requireT.True(exists)

		acc.FirstName = "FirstName2"
		requireT.NoError(tx.Set(acc))
		return nil
	}))

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		acc, exists := db.Get[entities.Account](tx.View(), accountID)
		requireT.True(exists)

		acc.LastName = "LastName2"
		requireT.NoError(tx.Set(acc))
		return nil
	}))

	// ======= DesiredVersion 0, Term 2

	clstr.StopPeers(p)
	clstr.StartPeers(p)

	// Client c2 is here just to wait until peer is available again.
	c2 := clstr.NewClient(p, "client", partitionDefault, nil)
	clstr.StartClients(c2)
	clstr.StopClients(c2)

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		acc, exists := db.Get[entities.Account](tx.View(), accountID)
		requireT.True(exists)

		acc.FirstName = "FirstName3"
		requireT.NoError(tx.Set(acc))
		return nil
	}))

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		acc, exists := db.Get[entities.Account](tx.View(), accountID)
		requireT.True(exists)

		acc.LastName = "LastName3"
		requireT.NoError(tx.Set(acc))
		return nil
	}))

	group.Exit(nil)
	requireT.NoError(group.Wait())

	// ======= DesiredVersion 2, Term 3

	clusterConfig.Partitions = slices.Clone(clusterConfig.Partitions)
	clusterConfig.Partitions[0].Upgrades = []db.Upgrade{
		{
			Marshaller: m,
			UpgradeFunc: func(view *db.View, ch chan<- any) error {
				for a := range db.All[entities.Account](view) {
					a.FirstName += ".1"
					ch <- a
				}
				return nil
			},
		},
		{
			Marshaller: m,
			UpgradeFunc: func(view *db.View, ch chan<- any) error {
				for a := range db.All[entities.Account](view) {
					a.LastName += ".2"
					ch <- a
				}
				return nil
			},
		},
	}

	group = qa.NewGroup(ctx, t)
	clstr = cluster.NewTesting(group, t, clusterConfig)

	p = clstr.NewPeer("P", types.Partitions{partitionDefault: types.PartitionRoleActive})
	c = clstr.NewClient(p, "client", partitionDefault, nil)

	clstr.StartPeers(p)
	clstr.StartClients(c)

	v := c.View()
	account, exists := db.Get[entities.Account](v, accountID)
	requireT.True(exists)
	requireT.Equal(entities.Account{
		ID:        accountID,
		Revision:  0,
		FirstName: "FirstName3.1",
		LastName:  "LastName3.2",
	}, account)
}

func TestSeparateUpgrades3Peer(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)

	clusterConfig := clusterConfig
	clusterConfig.Directory = t.TempDir()

	// ======= DesiredVersion 0, Term 1

	clstr := cluster.NewTesting(group, t, clusterConfig)
	m := entities.NewMarshaller()

	p1 := clstr.NewPeer("P1", types.Partitions{partitionDefault: types.PartitionRoleActive})
	p2 := clstr.NewPeer("P2", types.Partitions{partitionDefault: types.PartitionRoleActive})
	p3 := clstr.NewPeer("P3", types.Partitions{partitionDefault: types.PartitionRoleActive})
	clstr.ForceLeader(p1)
	c := clstr.NewClient(p2, "client", partitionDefault, nil)

	clstr.StartPeers(p1, p2, p3)
	clstr.StartClients(c)

	accountID := memdb.NewID[entities.AccountID]()

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		requireT.NoError(tx.Set(entities.Account{
			ID:        accountID,
			FirstName: "FirstName1",
			LastName:  "LastName1",
		}))
		return nil
	}))

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		acc, exists := db.Get[entities.Account](tx.View(), accountID)
		requireT.True(exists)

		acc.FirstName = "FirstName2"
		requireT.NoError(tx.Set(acc))
		return nil
	}))

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		acc, exists := db.Get[entities.Account](tx.View(), accountID)
		requireT.True(exists)

		acc.LastName = "LastName2"
		requireT.NoError(tx.Set(acc))
		return nil
	}))

	// ======= DesiredVersion 0, Term 2

	clstr.StopPeers(p1, p2, p3)
	clstr.StartPeers(p1, p2, p3)

	// Client c2 is here just to wait until peer is available again.
	c2 := clstr.NewClient(p2, "client", partitionDefault, nil)
	clstr.StartClients(c2)
	clstr.StopClients(c2)

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		acc, exists := db.Get[entities.Account](tx.View(), accountID)
		requireT.True(exists)

		acc.FirstName = "FirstName3"
		requireT.NoError(tx.Set(acc))
		return nil
	}))

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		acc, exists := db.Get[entities.Account](tx.View(), accountID)
		requireT.True(exists)

		acc.LastName = "LastName3"
		requireT.NoError(tx.Set(acc))
		return nil
	}))

	group.Exit(nil)
	requireT.NoError(group.Wait())

	// ======= DesiredVersion 1, Term 3

	clusterConfig.Partitions = slices.Clone(clusterConfig.Partitions)
	clusterConfig.Partitions[0].Upgrades = []db.Upgrade{
		{
			Marshaller: m,
			UpgradeFunc: func(view *db.View, ch chan<- any) error {
				for a := range db.All[entities.Account](view) {
					a.FirstName += ".1"
					ch <- a
				}
				return nil
			},
		},
	}

	group = qa.NewGroup(ctx, t)
	clstr = cluster.NewTesting(group, t, clusterConfig)

	p1 = clstr.NewPeer("P1", types.Partitions{partitionDefault: types.PartitionRoleActive})
	p2 = clstr.NewPeer("P2", types.Partitions{partitionDefault: types.PartitionRoleActive})
	p3 = clstr.NewPeer("P3", types.Partitions{partitionDefault: types.PartitionRoleActive})
	clstr.ForceLeader(p1)
	c = clstr.NewClient(p2, "client", partitionDefault, nil)

	clstr.StartPeers(p1, p2, p3)
	clstr.StartClients(c)

	v := c.View()
	account, exists := db.Get[entities.Account](v, accountID)
	requireT.True(exists)
	requireT.Equal(entities.Account{
		ID:        accountID,
		Revision:  0,
		FirstName: "FirstName3.1",
		LastName:  "LastName3",
	}, account)

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		acc, exists := db.Get[entities.Account](tx.View(), accountID)
		requireT.True(exists)

		acc.LastName = "LastName4"
		requireT.NoError(tx.Set(acc))
		return nil
	}))

	group.Exit(nil)
	requireT.NoError(group.Wait())

	// ======= DesiredVersion 2, Term 4

	clusterConfig.Partitions[0].Upgrades = append(clusterConfig.Partitions[0].Upgrades, db.Upgrade{
		Marshaller: m,
		UpgradeFunc: func(view *db.View, ch chan<- any) error {
			for a := range db.All[entities.Account](view) {
				a.LastName += ".2"
				ch <- a
			}
			return nil
		},
	})

	group = qa.NewGroup(ctx, t)
	clstr = cluster.NewTesting(group, t, clusterConfig)

	p1 = clstr.NewPeer("P1", types.Partitions{partitionDefault: types.PartitionRoleActive})
	p2 = clstr.NewPeer("P2", types.Partitions{partitionDefault: types.PartitionRoleActive})
	p3 = clstr.NewPeer("P3", types.Partitions{partitionDefault: types.PartitionRoleActive})
	clstr.ForceLeader(p1)
	c = clstr.NewClient(p2, "client", partitionDefault, nil)

	clstr.StartPeers(p1, p2, p3)
	clstr.StartClients(c)

	v = c.View()
	account, exists = db.Get[entities.Account](v, accountID)
	requireT.True(exists)
	requireT.Equal(entities.Account{
		ID:        accountID,
		Revision:  0,
		FirstName: "FirstName3.1",
		LastName:  "LastName4.2",
	}, account)
}

func TestCombinedUpgrades3Peers(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)

	clusterConfig := clusterConfig
	clusterConfig.Directory = t.TempDir()

	// ======= DesiredVersion 0, Term 1

	clstr := cluster.NewTesting(group, t, clusterConfig)
	m := entities.NewMarshaller()

	p1 := clstr.NewPeer("P1", types.Partitions{partitionDefault: types.PartitionRoleActive})
	p2 := clstr.NewPeer("P2", types.Partitions{partitionDefault: types.PartitionRoleActive})
	p3 := clstr.NewPeer("P3", types.Partitions{partitionDefault: types.PartitionRoleActive})
	clstr.ForceLeader(p1)
	c := clstr.NewClient(p2, "client", partitionDefault, nil)

	clstr.StartPeers(p1, p2, p3)
	clstr.StartClients(c)

	accountID := memdb.NewID[entities.AccountID]()

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		requireT.NoError(tx.Set(entities.Account{
			ID:        accountID,
			FirstName: "FirstName1",
			LastName:  "LastName1",
		}))
		return nil
	}))

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		acc, exists := db.Get[entities.Account](tx.View(), accountID)
		requireT.True(exists)

		acc.FirstName = "FirstName2"
		requireT.NoError(tx.Set(acc))
		return nil
	}))

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		acc, exists := db.Get[entities.Account](tx.View(), accountID)
		requireT.True(exists)

		acc.LastName = "LastName2"
		requireT.NoError(tx.Set(acc))
		return nil
	}))

	// ======= DesiredVersion 0, Term 2

	clstr.StopPeers(p1, p2, p3)
	clstr.StartPeers(p1, p2, p3)

	// Client c2 is here just to wait until peer is available again.
	c2 := clstr.NewClient(p2, "client", partitionDefault, nil)
	clstr.StartClients(c2)
	clstr.StopClients(c2)

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		acc, exists := db.Get[entities.Account](tx.View(), accountID)
		requireT.True(exists)

		acc.FirstName = "FirstName3"
		requireT.NoError(tx.Set(acc))
		return nil
	}))

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		acc, exists := db.Get[entities.Account](tx.View(), accountID)
		requireT.True(exists)

		acc.LastName = "LastName3"
		requireT.NoError(tx.Set(acc))
		return nil
	}))

	group.Exit(nil)
	requireT.NoError(group.Wait())

	// ======= DesiredVersion 2, Term 3

	clusterConfig.Partitions = slices.Clone(clusterConfig.Partitions)
	clusterConfig.Partitions[0].Upgrades = []db.Upgrade{
		{
			Marshaller: m,
			UpgradeFunc: func(view *db.View, ch chan<- any) error {
				for a := range db.All[entities.Account](view) {
					a.FirstName += ".1"
					ch <- a
				}
				return nil
			},
		},
		{
			Marshaller: m,
			UpgradeFunc: func(view *db.View, ch chan<- any) error {
				for a := range db.All[entities.Account](view) {
					a.LastName += ".2"
					ch <- a
				}
				return nil
			},
		},
	}

	group = qa.NewGroup(ctx, t)
	clstr = cluster.NewTesting(group, t, clusterConfig)

	p1 = clstr.NewPeer("P1", types.Partitions{partitionDefault: types.PartitionRoleActive})
	p2 = clstr.NewPeer("P2", types.Partitions{partitionDefault: types.PartitionRoleActive})
	p3 = clstr.NewPeer("P3", types.Partitions{partitionDefault: types.PartitionRoleActive})
	clstr.ForceLeader(p1)
	c = clstr.NewClient(p2, "client", partitionDefault, nil)

	clstr.StartPeers(p1, p2, p3)
	clstr.StartClients(c)

	v := c.View()
	account, exists := db.Get[entities.Account](v, accountID)
	requireT.True(exists)
	requireT.Equal(entities.Account{
		ID:        accountID,
		Revision:  0,
		FirstName: "FirstName3.1",
		LastName:  "LastName3.2",
	}, account)
}

func TestColdPeerUpgrade(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)

	clusterConfig := clusterConfig
	clusterConfig.Directory = t.TempDir()

	// ======= DesiredVersion 0, Term 1

	clstr := cluster.NewTesting(group, t, clusterConfig)
	m := entities.NewMarshaller()

	p1 := clstr.NewPeer("P1", types.Partitions{partitionDefault: types.PartitionRoleActive})
	p2 := clstr.NewPeer("P2", types.Partitions{partitionDefault: types.PartitionRoleActive})
	clstr.NewPeer("P3", types.Partitions{partitionDefault: types.PartitionRoleActive})
	clstr.ForceLeader(p1)
	c := clstr.NewClient(p2, "client", partitionDefault, nil)

	clstr.StartPeers(p1, p2)
	clstr.StartClients(c)

	accountID := memdb.NewID[entities.AccountID]()

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		requireT.NoError(tx.Set(entities.Account{
			ID:        accountID,
			FirstName: "FirstName1",
			LastName:  "LastName1",
		}))
		return nil
	}))

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		acc, exists := db.Get[entities.Account](tx.View(), accountID)
		requireT.True(exists)

		acc.FirstName = "FirstName2"
		requireT.NoError(tx.Set(acc))
		return nil
	}))

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		acc, exists := db.Get[entities.Account](tx.View(), accountID)
		requireT.True(exists)

		acc.LastName = "LastName2"
		requireT.NoError(tx.Set(acc))
		return nil
	}))

	// ======= DesiredVersion 0, Term 2

	clstr.StopPeers(p1, p2)
	clstr.StartPeers(p1, p2)

	// Client c2 is here just to wait until peer is available again.
	c2 := clstr.NewClient(p2, "client", partitionDefault, nil)
	clstr.StartClients(c2)
	clstr.StopClients(c2)

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		acc, exists := db.Get[entities.Account](tx.View(), accountID)
		requireT.True(exists)

		acc.FirstName = "FirstName3"
		requireT.NoError(tx.Set(acc))
		return nil
	}))

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		acc, exists := db.Get[entities.Account](tx.View(), accountID)
		requireT.True(exists)

		acc.LastName = "LastName3"
		requireT.NoError(tx.Set(acc))
		return nil
	}))

	group.Exit(nil)
	requireT.NoError(group.Wait())

	// ======= DesiredVersion 1, Term 3

	clusterConfig.Partitions = slices.Clone(clusterConfig.Partitions)
	clusterConfig.Partitions[0].Upgrades = []db.Upgrade{
		{
			Marshaller: m,
			UpgradeFunc: func(view *db.View, ch chan<- any) error {
				for a := range db.All[entities.Account](view) {
					a.FirstName += ".1"
					ch <- a
				}
				return nil
			},
		},
	}

	group = qa.NewGroup(ctx, t)
	clstr = cluster.NewTesting(group, t, clusterConfig)

	p1 = clstr.NewPeer("P1", types.Partitions{partitionDefault: types.PartitionRoleActive})
	p2 = clstr.NewPeer("P2", types.Partitions{partitionDefault: types.PartitionRoleActive})
	clstr.NewPeer("P3", types.Partitions{partitionDefault: types.PartitionRoleActive})
	clstr.ForceLeader(p1)
	c = clstr.NewClient(p2, "client", partitionDefault, nil)

	clstr.StartPeers(p1, p2)
	clstr.StartClients(c)

	v := c.View()
	account, exists := db.Get[entities.Account](v, accountID)
	requireT.True(exists)
	requireT.Equal(entities.Account{
		ID:        accountID,
		Revision:  0,
		FirstName: "FirstName3.1",
		LastName:  "LastName3",
	}, account)

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		acc, exists := db.Get[entities.Account](tx.View(), accountID)
		requireT.True(exists)

		acc.LastName = "LastName4"
		requireT.NoError(tx.Set(acc))
		return nil
	}))

	group.Exit(nil)
	requireT.NoError(group.Wait())

	// ======= DesiredVersion 2, Term 4

	clusterConfig.Partitions[0].Upgrades = append(clusterConfig.Partitions[0].Upgrades, db.Upgrade{
		Marshaller: m,
		UpgradeFunc: func(view *db.View, ch chan<- any) error {
			for a := range db.All[entities.Account](view) {
				a.LastName += ".2"
				ch <- a
			}
			return nil
		},
	})

	group = qa.NewGroup(ctx, t)
	clstr = cluster.NewTesting(group, t, clusterConfig)

	p1 = clstr.NewPeer("P1", types.Partitions{partitionDefault: types.PartitionRoleActive})
	p2 = clstr.NewPeer("P2", types.Partitions{partitionDefault: types.PartitionRoleActive})
	p3 := clstr.NewPeer("P3", types.Partitions{partitionDefault: types.PartitionRoleActive})
	clstr.ForceLeader(p1)
	c = clstr.NewClient(p2, "client", partitionDefault, nil)

	clstr.StartPeers(p1, p2)
	clstr.StartClients(c)

	v = c.View()
	account, exists = db.Get[entities.Account](v, accountID)
	requireT.True(exists)
	requireT.Equal(entities.Account{
		ID:        accountID,
		Revision:  0,
		FirstName: "FirstName3.1",
		LastName:  "LastName4.2",
	}, account)

	c = clstr.NewClient(p3, "client", partitionDefault, nil)

	clstr.StartPeers(p3)
	clstr.StartClients(c)

	v = c.View()
	account, exists = db.Get[entities.Account](v, accountID)
	requireT.True(exists)
	requireT.Equal(entities.Account{
		ID:        accountID,
		Revision:  0,
		FirstName: "FirstName3.1",
		LastName:  "LastName4.2",
	}, account)
}

func TestUpgradeComplex(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)

	clusterConfig := clusterConfig
	clusterConfig.Directory = t.TempDir()

	// ======= DesiredVersion 0, Term 1

	clstr := cluster.NewTesting(group, t, clusterConfig)
	m := entities.NewMarshaller()
	m2 := entities2.NewMarshaller()

	p1 := clstr.NewPeer("P1", types.Partitions{partitionDefault: types.PartitionRoleActive})
	p2 := clstr.NewPeer("P2", types.Partitions{partitionDefault: types.PartitionRoleActive})
	p3 := clstr.NewPeer("P3", types.Partitions{partitionDefault: types.PartitionRoleActive})
	clstr.ForceLeader(p1)
	c := clstr.NewClient(p2, "client", partitionDefault, nil)

	clstr.StartPeers(p1, p2, p3)
	clstr.StartClients(c)

	accountID1 := memdb.NewID[entities.AccountID]()
	accountID2 := memdb.NewID[entities.AccountID]()
	accountID3 := memdb.NewID[entities.AccountID]()

	blobID1 := memdb.NewID[memdb.ID]()
	blobID2 := memdb.NewID[memdb.ID]()
	blobID3 := memdb.NewID[memdb.ID]()

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		requireT.NoError(tx.Set(entities.Account{
			ID:        accountID1,
			FirstName: "FirstName1",
			LastName:  "LastName1",
		}))
		return nil
	}))

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		v := tx.View()

		requireT.NoError(tx.Set(entities.Account{
			ID:        accountID2,
			FirstName: "FirstName2",
			LastName:  "LastName2",
		}))

		for acc := range db.All[entities.Account](v) {
			acc.LastName += ".2"
			requireT.NoError(tx.Set(acc))
		}
		return nil
	}))

	// ======= DesiredVersion 0, Term 2

	clstr.StopPeers(p1, p2, p3)
	clstr.StartPeers(p1, p2, p3)

	// Client c2 is here just to wait until peer is available again.
	c2 := clstr.NewClient(p2, "client", partitionDefault, nil)
	clstr.StartClients(c2)
	clstr.StopClients(c2)

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		requireT.NoError(tx.Set(entities.Blob{
			ID:   blobID1,
			Data: []byte{0x01, 0x01},
		}))
		requireT.NoError(tx.Set(entities.Account{
			ID:        accountID3,
			FirstName: "FirstName3",
			LastName:  "LastName3",
		}))
		requireT.NoError(tx.Set(entities.Blob{
			ID:   blobID2,
			Data: []byte{0x02, 0x02},
		}))

		return nil
	}))

	group.Exit(nil)
	requireT.NoError(group.Wait())

	// ======= DesiredVersion 1, Term 3

	clusterConfig.Partitions = slices.Clone(clusterConfig.Partitions)
	clusterConfig.Partitions[0].Upgrades = []db.Upgrade{
		{
			Marshaller: m,
			UpgradeFunc: func(view *db.View, ch chan<- any) error {
				for a := range db.All[entities.Account](view) {
					if a.ID == accountID1 {
						a.FirstName += ".1"
					}
					ch <- a
				}
				for b := range db.All[entities.Blob](view) {
					b.Data = slices.Clone(b.Data)
					b.Data = append(b.Data, b.Data[0]+1)
					ch <- b
				}
				return nil
			},
		},
	}

	group = qa.NewGroup(ctx, t)
	clstr = cluster.NewTesting(group, t, clusterConfig)

	p1 = clstr.NewPeer("P1", types.Partitions{partitionDefault: types.PartitionRoleActive})
	p2 = clstr.NewPeer("P2", types.Partitions{partitionDefault: types.PartitionRoleActive})
	p3 = clstr.NewPeer("P3", types.Partitions{partitionDefault: types.PartitionRoleActive})
	c = clstr.NewClient(p2, "client", partitionDefault, nil)

	clstr.StartPeers(p1, p2, p3)
	clstr.StartClients(c)

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		acc, exists := db.Get[entities.Account](tx.View(), accountID2)
		requireT.True(exists)

		acc.LastName += ".2"
		requireT.NoError(tx.Set(acc))

		requireT.NoError(tx.Set(entities.Blob{
			ID:   blobID3,
			Data: []byte{0x03, 0x03},
		}))

		return nil
	}))

	group.Exit(nil)
	requireT.NoError(group.Wait())

	// ======= DesiredVersion 2, Term 4

	clusterConfig.Partitions[0].Upgrades = append(clusterConfig.Partitions[0].Upgrades, db.Upgrade{
		Marshaller: m2,
		UpgradeFunc: func(view *db.View, ch chan<- any) error {
			for a := range db.All[entities.Account](view) {
				ch <- entities2.Account{
					ID:        entities2.AccountID(a.ID),
					Name:      a.FirstName + " " + a.LastName,
					FirstName: a.FirstName,
					LastName:  a.LastName + ".3",
				}
			}
			for b := range db.All[entities.Blob](view) {
				data := slices.Clone(b.Data)
				data[0]++
				ch <- entities2.Blob{
					ID:   b.ID,
					Data: data,
				}
			}
			return nil
		},
	})

	group = qa.NewGroup(ctx, t)
	clstr = cluster.NewTesting(group, t, clusterConfig)

	p1 = clstr.NewPeer("P1", types.Partitions{partitionDefault: types.PartitionRoleActive})
	p2 = clstr.NewPeer("P2", types.Partitions{partitionDefault: types.PartitionRoleActive})
	p3 = clstr.NewPeer("P3", types.Partitions{partitionDefault: types.PartitionRoleActive})
	c = clstr.NewClient(p2, "client", partitionDefault, nil)

	clstr.StartPeers(p1, p2, p3)
	clstr.StartClients(c)

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		blob, exists := db.Get[entities2.Blob](tx.View(), blobID1)
		requireT.True(exists)

		blob.Data = blob.Data[1:]
		requireT.NoError(tx.Set(blob))

		return nil
	}))

	v := c.View()

	acc, exists := db.Get[entities2.Account](v, accountID1)
	requireT.True(exists)
	requireT.Equal(entities2.Account{
		ID:        entities2.AccountID(accountID1),
		Revision:  0,
		Name:      "FirstName1.1 LastName1.2",
		FirstName: "FirstName1.1",
		LastName:  "LastName1.2.3",
	}, acc)

	acc, exists = db.Get[entities2.Account](v, accountID2)
	requireT.True(exists)
	requireT.Equal(entities2.Account{
		ID:        entities2.AccountID(accountID2),
		Revision:  0,
		Name:      "FirstName2 LastName2.2",
		FirstName: "FirstName2",
		LastName:  "LastName2.2.3",
	}, acc)

	acc, exists = db.Get[entities2.Account](v, accountID3)
	requireT.True(exists)
	requireT.Equal(entities2.Account{
		ID:        entities2.AccountID(accountID3),
		Revision:  0,
		Name:      "FirstName3 LastName3",
		FirstName: "FirstName3",
		LastName:  "LastName3.3",
	}, acc)

	blob, exists := db.Get[entities2.Blob](v, blobID1)
	requireT.True(exists)
	requireT.Equal(entities2.Blob{
		ID:       blobID1,
		Revision: 1,
		Data:     []byte{0x01, 0x02},
	}, blob)

	blob, exists = db.Get[entities2.Blob](v, blobID2)
	requireT.True(exists)
	requireT.Equal(entities2.Blob{
		ID:       blobID2,
		Revision: 0,
		Data:     []byte{0x03, 0x02, 0x03},
	}, blob)

	blob, exists = db.Get[entities2.Blob](v, blobID3)
	requireT.True(exists)
	requireT.Equal(entities2.Blob{
		ID:       blobID3,
		Revision: 0,
		Data:     []byte{0x04, 0x03},
	}, blob)
}
