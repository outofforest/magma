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
	"github.com/outofforest/proton"
)

const queueCapacity = 10

// Generate generates serialization code for entities.
func Generate(filePath string, entities ...any) {
	msgs := make([]proton.Msg, 0, len(entities))
	for _, e := range entities {
		msgs = append(msgs, proton.Message(e, "ID", "Revision"))
	}
	proton.Generate(filePath, msgs...)
}

// Run runs magma.
func Run(
	ctx context.Context,
	config types.Config,
	p2pListener, c2pListener net.Listener,
	dir string,
	pageSize uint64,
) (retErr error) {
	pStates := map[types.PartitionID]partition.State{}
	var partitions []types.PartitionID
	for _, s := range config.Servers {
		if s.ID == config.ServerID {
			partitions = s.Partitions
			break
		}
	}

	for _, pID := range partitions {
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

		servers := make([]types.ServerConfig, 0, len(config.Servers))
		serverIDs := make([]types.ServerID, 0, len(config.Servers))
		for _, s := range config.Servers {
			for _, p := range s.Partitions {
				if p == pID {
					servers = append(servers, s)
					serverIDs = append(serverIDs, s.ID)
					break
				}
			}
		}

		pStates[pID] = partition.State{
			Repo:       repo,
			Reactor:    reactor.New(config.ServerID, serverIDs, s),
			Servers:    servers,
			CmdP2PCh:   make(chan rafttypes.Command, queueCapacity),
			CmdC2PCh:   make(chan rafttypes.Command, queueCapacity),
			ResultCh:   make(chan reactor.Result, 1),
			RoleCh:     make(chan rafttypes.Role, 1),
			MajorityCh: make(chan bool, 1),
		}
	}

	return raft.Run(
		ctx,
		pStates,
		gossip.New(config.ServerID, config.MaxMessageSize, p2pListener, c2pListener, pStates),
	)
}

func defCloser(sCloser state.Closer, retErr *error) {
	if err := sCloser(); err != nil && *retErr == nil {
		*retErr = err
	}
}
