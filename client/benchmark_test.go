package client

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/outofforest/logger"
	"github.com/outofforest/magma"
	"github.com/outofforest/magma/client/entities"
	"github.com/outofforest/magma/client/indices"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/parallel"
)

func TestCluster(t *testing.T) {
	t.Skip()
	requireT := require.New(t)
	ctx := logger.WithLogger(t.Context(), logger.New(logger.DefaultConfig))

	peer1 := types.ServerID("P1")
	peer2 := types.ServerID("P2")
	peer3 := types.ServerID("P3")
	peer4 := types.ServerID("P4")

	p2p1, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)
	defer p2p1.Close()

	c2p1, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)
	defer c2p1.Close()

	p2p2, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)
	defer p2p2.Close()

	c2p2, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)
	defer c2p2.Close()

	p2p3, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)
	defer p2p3.Close()

	c2p3, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)
	defer c2p3.Close()

	p2p4, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)
	defer p2p4.Close()

	c2p4, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)
	defer c2p4.Close()

	config := types.Config{
		Servers: []types.ServerConfig{
			{
				ID: peer1,
				Partitions: types.Partitions{
					"default": true,
				},
				P2PAddress: p2p1.Addr().String(),
			},
			{
				ID: peer2,
				Partitions: types.Partitions{
					"default": true,
				},
				P2PAddress: p2p2.Addr().String(),
			},
			{
				ID: peer3,
				Partitions: types.Partitions{
					"default": true,
				},
				P2PAddress: p2p3.Addr().String(),
			},
			{
				ID: peer4,
				Partitions: types.Partitions{
					"default": true,
				},
				P2PAddress: p2p4.Addr().String(),
			},
		},
		MaxMessageSize: 128 * 1024,
	}

	group := parallel.NewGroup(ctx)
	defer func() {
		group.Exit(nil)
		if err := group.Wait(); err != nil {
			logger.Get(ctx).Error("Error", zap.Error(err))
		}
	}()

	fmt.Printf("==== %s ====\n", peer1)
	fmt.Printf("==== %s ====\n", peer2)
	fmt.Printf("==== %s ====\n", peer3)
	fmt.Printf("==== %s ====\n", peer4)

	if err := os.RemoveAll("test"); err != nil {
		panic(err)
	}

	const pageSize = 16 * 1024
	group.Spawn("peer1", parallel.Fail, func(ctx context.Context) error {
		config, dir := makeConfig(config, peer1)
		return magma.Run(ctx, config, p2p1, c2p1, dir, pageSize)
	})
	group.Spawn("peer2", parallel.Fail, func(ctx context.Context) error {
		config, dir := makeConfig(config, peer2)
		return magma.Run(ctx, config, p2p2, c2p2, dir, pageSize)
	})
	group.Spawn("peer3", parallel.Fail, func(ctx context.Context) error {
		config, dir := makeConfig(config, peer3)
		return magma.Run(ctx, config, p2p3, c2p3, dir, pageSize)
	})

	var acc entities.Account
	firstNameIndex, err := indices.NewFieldIndex("firstName", &acc, &acc.FirstName)
	requireT.NoError(err)
	lastNameIndex, err := indices.NewFieldIndex("lastName", &acc, &acc.LastName)
	requireT.NoError(err)
	nameIndex, err := indices.NewMultiIndex(firstNameIndex, lastNameIndex)
	requireT.NoError(err)
	ifFirstName, err := indices.NewIfIndex("test1", firstNameIndex, func(acc *entities.Account) bool {
		return acc.FirstName == "Test1"
	})
	requireT.NoError(err)

	cl, err := New(Config{
		Service:          "test",
		PeerAddress:      c2p1.Addr().String(),
		PartitionID:      "default",
		MaxMessageSize:   config.MaxMessageSize,
		BroadcastTimeout: 3 * time.Second,
		AwaitTimeout:     10 * time.Second,
		Marshaller:       entities.NewMarshaller(),
		Indices: []indices.Index{
			firstNameIndex,
			nameIndex,
			ifFirstName,
		},
		TriggerFunc: func(ctx context.Context, v *View) error {
			fmt.Println("================")
			return nil
		},
	})
	requireT.NoError(err)

	group.Spawn("client", parallel.Fail, cl.Run)

	time.Sleep(5 * time.Second)
	fmt.Println("Start")

	const clientCount = 10
	groupClients := parallel.NewSubgroup(group.Spawn, "clients", parallel.Continue)
	for range clientCount {
		groupClients.Spawn("client", parallel.Continue, func(ctx context.Context) error {
			tr := cl.NewTransactor()
			id := NewID[entities.AccountID]()
			for i := range 50 {
				err := tr.Tx(ctx, func(tx *Tx) error {
					if acc, exists := Get[entities.Account](tx.View, id); exists {
						fmt.Println(acc)
					}

					/*
						if acc, exists := Find[entities.Account](tx.View, firstNameIndex,
							fmt.Sprintf("First-%d", i-1)); exists {
							fmt.Println(acc)
						}
						for acc := range All[entities.Account](tx.View) {
							fmt.Println(acc)
						}
						for acc := range Iterate[entities.Account](tx.View, firstNameIndex) {
							fmt.Println(acc)
						}

						it := AllIterator[entities.Account](tx.View)
						for {
							acc, exists := it()
							if !exists {
								break
							}
							fmt.Println(acc)
						}

						it = Iterator[entities.Account](tx.View, firstNameIndex)
						for {
							acc, exists := it()
							if !exists {
								break
							}
							fmt.Println(acc)
						}

						for acc := range Iterate[entities.Account](tx.View, nameIndex, "Test1") {
							fmt.Println(acc)
						}

						for acc := range Iterate[entities.Account](tx.View, ifFirstName) {
							fmt.Println(acc)
						}
					*/
					tx.Set(entities.Account{
						ID:        id,
						Revision:  types.Revision(i),
						FirstName: fmt.Sprintf("First-%d", i),
						LastName:  "Last",
					})
					tx.Set(entities.Account{
						ID:        NewID[entities.AccountID](),
						FirstName: "Test1",
						LastName:  "Test2",
					})
					tx.Set(entities.Account{
						ID:        NewID[entities.AccountID](),
						FirstName: "Test1",
						LastName:  "Test2",
					})
					tx.Set(entities.Account{
						ID:        NewID[entities.AccountID](),
						FirstName: "Test1",
						LastName:  "Test2",
					})
					tx.Set(entities.Account{
						ID:        NewID[entities.AccountID](),
						FirstName: "Test1",
						LastName:  "Test2",
					})
					tx.Set(entities.Account{
						ID:        NewID[entities.AccountID](),
						FirstName: "Test1",
						LastName:  "Test2",
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
	if err := groupClients.Wait(); err != nil {
		logger.Get(ctx).Error("Error", zap.Error(err))
		return
	}

	fmt.Println("===================")

	group.Spawn("peer4", parallel.Fail, func(ctx context.Context) error {
		config, dir := makeConfig(config, peer4)
		return magma.Run(ctx, config, p2p4, c2p4, dir, pageSize)
	})

	time.Sleep(10 * time.Second)
	fmt.Println("exit")
}

func makeConfig(config types.Config, peerID types.ServerID) (types.Config, string) {
	config.ServerID = peerID
	return config, filepath.Join("test", string(peerID))
}
