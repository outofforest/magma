package reactor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/raft/state"
	"github.com/outofforest/magma/raft/types"
)

func TestID(t *testing.T) {
	requireT := require.New(t)

	r, _ := newReactor(&state.State{})
	r.id = serverID

	requireT.Equal(serverID, r.ID())
}

func TestRole(t *testing.T) {
	requireT := require.New(t)

	r, _ := newReactor(&state.State{})
	r.role = types.RoleLeader

	requireT.Equal(types.RoleLeader, r.Role())
}

func TestLeaderID(t *testing.T) {
	requireT := require.New(t)

	r, _ := newReactor(&state.State{})
	r.leaderID = serverID

	requireT.Equal(serverID, r.LeaderID())
}
