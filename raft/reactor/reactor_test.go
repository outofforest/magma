package reactor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/raft/state"
	"github.com/outofforest/magma/raft/types"
)

func TestNewPeers(t *testing.T) {
	requireT := require.New(t)

	r := New(serverID, []types.ServerID{peer1ID, peer2ID, peer3ID, peer4ID}, &state.State{}, &TestTimeSource{})

	requireT.Equal(serverID, r.id)
	requireT.Equal([]types.ServerID{
		peer1ID, peer2ID, peer3ID, peer4ID,
	}, r.peers)

	r = New(serverID, []types.ServerID{peer1ID, peer2ID, serverID, peer3ID, peer4ID}, &state.State{}, &TestTimeSource{})

	requireT.Equal(serverID, r.id)
	requireT.Equal([]types.ServerID{
		peer1ID, peer2ID, peer3ID, peer4ID,
	}, r.peers)
}

func TestNewMinority(t *testing.T) {
	requireT := require.New(t)

	r := New(serverID, []types.ServerID{}, &state.State{}, &TestTimeSource{})
	requireT.Zero(r.minority)

	r = New(serverID, []types.ServerID{serverID}, &state.State{}, &TestTimeSource{})
	requireT.Zero(r.minority)

	r = New(serverID, []types.ServerID{peer1ID}, &state.State{}, &TestTimeSource{})
	requireT.Equal(1, r.minority)

	r = New(serverID, []types.ServerID{peer1ID, serverID}, &state.State{}, &TestTimeSource{})
	requireT.Equal(1, r.minority)

	r = New(serverID, []types.ServerID{peer1ID, peer2ID}, &state.State{}, &TestTimeSource{})
	requireT.Equal(1, r.minority)

	r = New(serverID, []types.ServerID{peer1ID, peer2ID, serverID}, &state.State{}, &TestTimeSource{})
	requireT.Equal(1, r.minority)

	r = New(serverID, []types.ServerID{peer1ID, peer2ID, peer3ID}, &state.State{}, &TestTimeSource{})
	requireT.Equal(2, r.minority)

	r = New(serverID, []types.ServerID{peer1ID, peer2ID, peer3ID, serverID}, &state.State{}, &TestTimeSource{})
	requireT.Equal(2, r.minority)

	r = New(serverID, []types.ServerID{peer1ID, peer2ID, peer3ID, peer4ID}, &state.State{}, &TestTimeSource{})
	requireT.Equal(2, r.minority)

	r = New(serverID, []types.ServerID{peer1ID, peer2ID, peer3ID, peer4ID, serverID}, &state.State{}, &TestTimeSource{})
	requireT.Equal(2, r.minority)
}
