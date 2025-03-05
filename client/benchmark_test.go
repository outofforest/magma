package client

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/outofforest/logger"
	"github.com/outofforest/magma"
	"github.com/outofforest/magma/client/entities"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/parallel"
	"github.com/outofforest/resonance"
)

func TestCluster(t *testing.T) {
	t.Skip()
	requireT := require.New(t)
	ctx, cancel := context.WithCancel(logger.WithLogger(context.Background(), logger.New(logger.DefaultConfig)))
	t.Cleanup(cancel)

	peer1 := types.ServerID(uuid.New())
	peer2 := types.ServerID(uuid.New())
	peer3 := types.ServerID(uuid.New())
	peer4 := types.ServerID(uuid.New())

	p2p1, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)
	defer p2p1.Close()

	tx2p1, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)
	defer tx2p1.Close()

	c2p1, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)
	defer c2p1.Close()

	p2p2, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)
	defer p2p2.Close()

	tx2p2, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)
	defer tx2p2.Close()

	c2p2, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)
	defer c2p2.Close()

	p2p3, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)
	defer p2p3.Close()

	tx2p3, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)
	defer tx2p3.Close()

	c2p3, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)
	defer c2p3.Close()

	p2p4, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)
	defer p2p4.Close()

	tx2p4, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)
	defer tx2p4.Close()

	c2p4, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)
	defer c2p4.Close()

	config := types.Config{
		Servers: []types.PeerConfig{
			{
				ID:          peer1,
				P2PAddress:  p2p1.Addr().String(),
				Tx2PAddress: tx2p1.Addr().String(),
			},
			{
				ID:          peer2,
				P2PAddress:  p2p2.Addr().String(),
				Tx2PAddress: tx2p2.Addr().String(),
			},
			{
				ID:          peer3,
				P2PAddress:  p2p3.Addr().String(),
				Tx2PAddress: tx2p3.Addr().String(),
			},
			{
				ID:          peer4,
				P2PAddress:  p2p4.Addr().String(),
				Tx2PAddress: tx2p4.Addr().String(),
			},
		},
		P2P: resonance.Config{
			MaxMessageSize: 1024 * 1024,
		},
		C2P: resonance.Config{
			MaxMessageSize: 128 * 1024,
		},
		MaxLogSizePerMessage: 512 * 1024,
		MaxLogSizeOnWire:     10 * 1024 * 1024,
		PassthroughTimeout:   3 * time.Second,
	}

	group := parallel.NewGroup(ctx)
	defer func() {
		group.Exit(nil)
		if err := group.Wait(); err != nil {
			logger.Get(ctx).Error("Error", zap.Error(err))
		}
	}()

	fmt.Printf("==== %s ====\n", uuid.UUID(peer1))
	fmt.Printf("==== %s ====\n", uuid.UUID(peer2))
	fmt.Printf("==== %s ====\n", uuid.UUID(peer3))
	fmt.Printf("==== %s ====\n", uuid.UUID(peer4))

	group.Spawn("peer1", parallel.Fail, func(ctx context.Context) error {
		return magma.Run(ctx, makeConfig(config, peer1), p2p1, tx2p1, c2p1)
	})
	group.Spawn("peer2", parallel.Fail, func(ctx context.Context) error {
		return magma.Run(ctx, makeConfig(config, peer2), p2p2, tx2p2, c2p2)
	})
	group.Spawn("peer3", parallel.Fail, func(ctx context.Context) error {
		return magma.Run(ctx, makeConfig(config, peer3), p2p3, tx2p3, c2p3)
	})

	client := New(Config{
		PeerAddress:      c2p1.Addr().String(),
		C2P:              config.C2P,
		BroadcastTimeout: 3 * time.Second,
	}, entities.NewMarshaller())
	group.Spawn("client", parallel.Fail, client.Run)

	time.Sleep(5 * time.Second)
	fmt.Println("Start")

	for range 1_000_000 {
		err := client.Broadcast(group.Context(), []any{
			&entities.Account{
				FirstName: "Test1",
				LastName:  "Test2",
			},
			&entities.Account{
				FirstName: "Test1",
				LastName:  "Test2",
			},
			&entities.Account{
				FirstName: "Test1",
				LastName:  "Test2",
			},
			&entities.Account{
				FirstName: "Test1",
				LastName:  "Test2",
			},
			&entities.Account{
				FirstName: "Test1",
				LastName:  "Test2",
			},
		})
		if err != nil {
			fmt.Println(err)
		}
	}

	time.Sleep(5 * time.Second)

	fmt.Println("===================")

	group.Spawn("peer4", parallel.Fail, func(ctx context.Context) error {
		return magma.Run(ctx, makeConfig(config, peer4), p2p4, tx2p4, c2p4)
	})

	time.Sleep(time.Minute)
}

func makeConfig(config types.Config, peerID types.ServerID) types.Config {
	config.ServerID = peerID
	config.StateDir = filepath.Join("test", uuid.UUID(peerID).String())
	return config
}
