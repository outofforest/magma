package magma

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/logger"
	"github.com/outofforest/magma/gossip"
	"github.com/outofforest/magma/gossip/p2c"
	rafttypes "github.com/outofforest/magma/raft/types"
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
	group.Spawn("peer1", parallel.Fail, func(ctx context.Context) error {
		return Run(ctx, types.Config{ServerID: peer1, Servers: servers}, p2p1, p2c1)
	})
	group.Spawn("peer2", parallel.Fail, func(ctx context.Context) error {
		return Run(ctx, types.Config{ServerID: peer2, Servers: servers}, p2p2, p2c2)
	})
	group.Spawn("peer3", parallel.Fail, func(ctx context.Context) error {
		return Run(ctx, types.Config{ServerID: peer3, Servers: servers}, p2p3, p2c3)
	})

	time.Sleep(5 * time.Second)
	fmt.Println("Start")

	_ = resonance.RunClient[p2c.Marshaller](ctx, p2c1.Addr().String(), gossip.P2CConfig,
		func(ctx context.Context, recvCh <-chan any, c *resonance.Connection[p2c.Marshaller]) error {
			fmt.Println("Connected")
			for i := range 1000 {
				c.Send(&rafttypes.ClientRequest{
					Data: []byte{byte(i)},
				})
				for range 100000 {
				}
			}
			return nil
		},
	)

	time.Sleep(5 * time.Second)

	fmt.Println("===================")

	group.Spawn("peer4", parallel.Fail, func(ctx context.Context) error {
		return Run(ctx, types.Config{ServerID: peer4, Servers: servers}, p2p4, p2c4)
	})

	time.Sleep(time.Minute)
}
