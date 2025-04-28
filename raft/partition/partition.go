package partition

import (
	"github.com/outofforest/magma/raft/reactor"
	"github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/state/repository"
	magmatypes "github.com/outofforest/magma/types"
)

// State keeps the partition state used by other components.
type State struct {
	Servers []magmatypes.ServerConfig
	Repo    *repository.Repository
	Reactor *reactor.Reactor

	CmdP2PCh, CmdC2PCh chan types.Command
	ResultCh           chan reactor.Result
	RoleCh             chan types.Role
	MajorityCh         chan bool
}
