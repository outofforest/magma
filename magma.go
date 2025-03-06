package magma

import (
	"context"
	"net"

	"github.com/outofforest/magma/gossip"
	"github.com/outofforest/magma/helpers"
	"github.com/outofforest/magma/raft"
	"github.com/outofforest/magma/raft/reactor"
	"github.com/outofforest/magma/raft/state"
	"github.com/outofforest/magma/types"
)

// Run runs magma.
func Run(ctx context.Context, config types.Config, p2pListener, tx2pListener, c2pListener net.Listener) error {
	s, closeState, err := state.Open(config.StateDir)
	if err != nil {
		return err
	}
	defer closeState()

	return raft.Run(
		ctx,
		reactor.New(config.ServerID, helpers.Peers(config), s, config.MaxLogSizePerMessage, config.MaxLogSizeOnWire),
		gossip.New(config, p2pListener, tx2pListener, c2pListener, config.StateDir),
	)
}
