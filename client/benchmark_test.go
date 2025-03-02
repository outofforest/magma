package client

import (
	"context"
	"fmt"
	"net"
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

	p2c1, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)
	defer p2c1.Close()

	p2p2, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)
	defer p2p2.Close()

	p2c2, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)
	defer p2c2.Close()

	p2p3, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)
	defer p2p3.Close()

	p2c3, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)
	defer p2c3.Close()

	p2p4, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)
	defer p2p4.Close()

	p2c4, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)
	defer p2c4.Close()

	servers := []types.PeerConfig{
		{
			ID:         peer1,
			P2PAddress: p2p1.Addr().String(),
		},
		{
			ID:         peer2,
			P2PAddress: p2p2.Addr().String(),
		},
		{
			ID:         peer3,
			P2PAddress: p2p3.Addr().String(),
		},
		{
			ID:         peer4,
			P2PAddress: p2p4.Addr().String(),
		},
	}

	group := parallel.NewGroup(ctx)
	defer func() {
		group.Exit(nil)
		if err := group.Wait(); err != nil {
			logger.Get(ctx).Error("Error", zap.Error(err))
		}
	}()

	group.Spawn("peer1", parallel.Fail, func(ctx context.Context) error {
		return magma.Run(ctx, types.Config{ServerID: peer1, Servers: servers, StateDir: "./test/peer1"}, p2p1, p2c1)
	})
	group.Spawn("peer2", parallel.Fail, func(ctx context.Context) error {
		return magma.Run(ctx, types.Config{ServerID: peer2, Servers: servers, StateDir: "./test/peer2"}, p2p2, p2c2)
	})
	group.Spawn("peer3", parallel.Fail, func(ctx context.Context) error {
		return magma.Run(ctx, types.Config{ServerID: peer3, Servers: servers, StateDir: "./test/peer3"}, p2p3, p2c3)
	})

	client := New(Config{
		PeerAddress: p2c1.Addr().String(),
		TxMessageConfig: resonance.Config{
			MaxMessageSize: 4096,
		},
	}, entities.NewMarshaller())
	group.Spawn("client", parallel.Fail, client.Run)

	time.Sleep(5 * time.Second)
	fmt.Println("Start")

	for range 100000 {
		err := client.Broadcast(ctx, []any{
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
		time.Sleep(time.Millisecond)
	}

	time.Sleep(5 * time.Second)

	fmt.Println("===================")

	group.Spawn("peer4", parallel.Fail, func(ctx context.Context) error {
		return magma.Run(ctx, types.Config{ServerID: peer4, Servers: servers, StateDir: "./test/peer4"}, p2p4, p2c4)
	})

	time.Sleep(time.Minute)
}
