package magma

import (
	"context"
	"net"
	"path/filepath"

	"github.com/outofforest/magma/gossip"
	"github.com/outofforest/magma/raft"
	"github.com/outofforest/magma/raft/partition"
	"github.com/outofforest/magma/raft/reactor"
	rafttypes "github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/state"
	"github.com/outofforest/magma/state/events"
	"github.com/outofforest/magma/state/repository"
	"github.com/outofforest/magma/types"
)

const queueCapacity = 10

// Run runs magma.
func Run(
	ctx context.Context,
	config types.Config,
	p2pListener, c2pListener net.Listener,
	dir string,
	pageSize uint64,
) (retErr error) {
	partitions := map[types.PartitionID]partition.State{}

	for _, pID := range config.Partitions {
		dir := filepath.Join(dir, string(pID))
		repo, err := repository.Open(filepath.Join(dir, "repo"), pageSize)
		if err != nil {
			return err
		}
		em, err := events.Open(filepath.Join(dir, "events"))
		if err != nil {
			return err
		}

		s, sCloser, err := state.New(repo, em)
		if err != nil {
			return err
		}
		defer defCloser(sCloser, &retErr)

		servers := make([]types.ServerID, 0, len(config.Servers))
		for _, s := range config.Servers {
			servers = append(servers, s.ID)
		}

		partitions[pID] = partition.State{
			Repo:       repo,
			Reactor:    reactor.New(config.ServerID, servers, s),
			CmdP2PCh:   make(chan rafttypes.Command, queueCapacity),
			CmdC2PCh:   make(chan rafttypes.Command, queueCapacity),
			ResultCh:   make(chan reactor.Result, 1),
			RoleCh:     make(chan rafttypes.Role, 1),
			MajorityCh: make(chan bool, 1),
		}
	}

	return raft.Run(
		ctx,
		partitions,
		gossip.New(config, p2pListener, c2pListener, partitions),
	)
}

func defCloser(sCloser state.Closer, retErr *error) {
	if err := sCloser(); err != nil && *retErr == nil {
		*retErr = err
	}
}
