package reactor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/raft/state"
	"github.com/outofforest/magma/raft/types"
)

func TestInfo(t *testing.T) {
	requireT := require.New(t)

	r, _ := newReactor(&state.State{})
	r.role = types.RoleLeader
	r.leaderID = serverID

	role, leaderID := r.Info()
	requireT.Equal(types.RoleLeader, role)
	requireT.Equal(serverID, leaderID)
}
