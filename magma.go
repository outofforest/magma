package magma

import (
	"context"
	"net"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/outofforest/magma/config"
	"github.com/outofforest/magma/gossip"
	"github.com/outofforest/magma/raft"
	"github.com/outofforest/magma/raft/partition"
	"github.com/outofforest/magma/raft/reactor"
	rafttypes "github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/state"
	"github.com/outofforest/magma/state/events"
	"github.com/outofforest/magma/state/repository"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/parallel"
	"github.com/outofforest/proton"
)

const queueCapacity = 10

// Entity creates entity definition.
func Entity[T any](ignoreFields ...string) proton.Msg {
	msg := proton.Message[T](ignoreFields...)
	msg.IgnoreFields["ID"] = true
	msg.IgnoreFields["Revision"] = true
	return msg
}

// Generate generates serialization code for entities.
func Generate(filePath string, entities ...proton.Msg) {
	proton.Generate(filePath, entities...)
}

// Run runs magma.
func Run(
	ctx context.Context,
	cfg config.Config,
	p2pListener, c2pListener net.Listener,
) (retErr error) {
	if cfg.ServerID == types.ZeroServerID {
		return errors.New("server ID is not set")
	}
	if len(cfg.Servers) == 0 {
		return errors.New("list of servers is empty")
	}
	if cfg.MaxMessageSize == 0 {
		return errors.New("max message size is not set")
	}
	if cfg.MaxUncommittedLog == 0 {
		return errors.New("max uncommitted log is not set")
	}

	pStates := map[types.PartitionID]partition.State{}
	partitions := make([]config.PartitionConfig, 0, len(cfg.Partitions))
	for _, s := range cfg.Servers {
		if s.ID == cfg.ServerID {
		loop:
			for pID := range s.Partitions {
				for _, p := range cfg.Partitions {
					if p.ID == pID {
						partitions = append(partitions, p)
						continue loop
					}
				}
				return errors.Errorf("partition config not found for partition %s", pID)
			}
			break
		}
	}

	repos := make([]*repository.Repository, 0, len(partitions))

	for _, p := range partitions {
		dir := filepath.Join(cfg.Directory, string(p.ID))
		repo, err := repository.Open(filepath.Join(dir, "repo"), cfg.PageSize, p, cfg.MaxMessageSize)
		if err != nil {
			return err
		}
		repos = append(repos, repo)

		em, err := events.Open(filepath.Join(dir, "events"))
		if err != nil {
			return err
		}
		defer em.Close()

		s, sCloser, err := state.New(repo, em)
		if err != nil {
			return err
		}
		defer defCloser(sCloser, &retErr)

		peers := make([]config.ServerConfig, 0, len(cfg.Servers))
		activePeers := make([]types.ServerID, 0, len(cfg.Servers))
		activeServersMap := map[types.ServerID]struct{}{}
		passivePeers := make([]types.ServerID, 0, len(cfg.Servers))
		var role types.PartitionRole
		for _, s := range cfg.Servers {
			if pRole, exists := s.Partitions[p.ID]; exists {
				if pRole == types.PartitionRoleActive {
					activeServersMap[s.ID] = struct{}{}
				}
				if cfg.ServerID == s.ID {
					role = pRole
					continue
				}
				peers = append(peers, s)
				if pRole == types.PartitionRoleActive {
					activePeers = append(activePeers, s.ID)
				} else {
					passivePeers = append(passivePeers, s.ID)
				}
			}
		}

		pStates[p.ID] = partition.State{
			Repo: repo,
			Reactor: reactor.New(cfg.ServerID, activePeers, passivePeers, cfg.MaxUncommittedLog,
				types.Version(len(p.Upgrades)), s),
			Peers:         peers,
			ActiveServers: activeServersMap,
			PartitionRole: role,
			CmdP2PCh:      make(chan rafttypes.Command, queueCapacity),
			CmdC2PCh:      make(chan rafttypes.Command, queueCapacity),
			ResultCh:      make(chan reactor.Result, 1),
			RoleCh:        make(chan rafttypes.Role, 1),
			MajorityCh:    make(chan bool, 1),
		}
	}

	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		for _, repo := range repos {
			spawn("repository", parallel.Fail, repo.Run)
		}
		spawn("raft", parallel.Fail, func(ctx context.Context) error {
			return raft.Run(
				ctx,
				pStates,
				gossip.New(cfg.P2PCA, cfg.C2PCA, cfg.ServerID, cfg.MaxMessageSize, p2pListener, c2pListener, pStates),
			)
		})

		return nil
	})
}

func defCloser(sCloser state.Closer, retErr *error) {
	if err := sCloser(); err != nil && *retErr == nil {
		*retErr = err
	}
}
