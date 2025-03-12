package magma

import (
	"context"
	"net"

	"github.com/outofforest/magma/gossip"
	"github.com/outofforest/magma/raft"
	"github.com/outofforest/magma/raft/reactor"
	"github.com/outofforest/magma/state"
	"github.com/outofforest/magma/state/events"
	"github.com/outofforest/magma/state/repository"
	"github.com/outofforest/magma/types"
)

// Run runs magma.
func Run(
	ctx context.Context,
	config types.Config,
	p2pListener, c2pListener net.Listener,
	repo *repository.Repository,
	em *events.Store,
) (retErr error) {
	s, sCloser, err := state.New(repo, em)
	if err != nil {
		return err
	}
	defer defCloser(sCloser, &retErr)

	return raft.Run(
		ctx,
		reactor.New(config, s),
		gossip.New(config, p2pListener, c2pListener, repo),
	)
}

func defCloser(sCloser state.Closer, retErr *error) {
	if err := sCloser(); err != nil && *retErr == nil {
		*retErr = err
	}
}
