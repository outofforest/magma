package magma

import (
	"context"
	"net"

	"github.com/outofforest/magma/gossip"
	"github.com/outofforest/magma/helpers"
	"github.com/outofforest/magma/raft"
	"github.com/outofforest/magma/raft/engine"
	"github.com/outofforest/magma/raft/reactor"
	"github.com/outofforest/magma/raft/state"
	"github.com/outofforest/magma/types"
)

// Run runs magma.
func Run(ctx context.Context, config types.Config, p2pListener, p2cListener net.Listener) error {
	majority := len(config.Servers)/2 + 1
	return raft.Run(
		ctx,
		engine.New(
			reactor.New(config.ServerID, majority, &state.State{}, &reactor.RealTimeSource{}),
			helpers.Peers(config),
		),
		majority,
		gossip.New(config, p2pListener, p2cListener),
	)
}
