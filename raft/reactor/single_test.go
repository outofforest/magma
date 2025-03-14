package reactor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/raft/state"
	"github.com/outofforest/magma/raft/types"
	magmatypes "github.com/outofforest/magma/types"
)

func newReactorSingleMode(s *state.State) *Reactor {
	config := config
	config.Servers = []magmatypes.PeerConfig{{ID: serverID}}
	return New(config, s)
}

func TestSingleModeApplyElectionTimeoutTransitionToLeader(t *testing.T) {
	requireT := require.New(t)
	s, log := newState()
	r := newReactorSingleMode(s)

	result, err := r.Apply(magmatypes.ZeroServerID, types.ElectionTick(1))
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.EqualValues(2, r.ignoreElectionTick)
	requireT.EqualValues(1, r.ignoreHeartbeatTick)
	requireT.EqualValues(1, s.CurrentTerm())
	requireT.EqualValues(1, r.votedForMe)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 2,
		},
	}, result)
	requireT.Empty(r.sync)
	requireT.Empty(r.matchIndex)
	requireT.EqualValues(1, r.lastLogTerm)
	requireT.EqualValues(2, r.nextLogIndex)

	granted, err := s.VoteFor(peer1ID)
	requireT.NoError(err)
	requireT.False(granted)

	granted, err = s.VoteFor(serverID)
	requireT.NoError(err)
	requireT.True(granted)

	logEqual(requireT, []byte{0x01, 0x01}, log)
}

func TestSingleModeApplyClientRequestAppend(t *testing.T) {
	requireT := require.New(t)
	s, log := newState()
	requireT.NoError(s.SetCurrentTerm(4))
	_, _, err := s.Append([]byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x03, 0x03, 0x02, 0x00, 0x00,
	}, true)
	requireT.NoError(err)
	r := newReactorSingleMode(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	result, err := r.Apply(magmatypes.ZeroServerID, &types.ClientRequest{
		Data: []byte{0x02, 0x01, 0x00},
	})
	requireT.NoError(err)

	requireT.Equal(types.RoleLeader, r.role)
	requireT.EqualValues(1, r.ignoreHeartbeatTick)
	requireT.EqualValues(4, s.CurrentTerm())
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 19,
		},
	}, result)
	requireT.Empty(r.sync)
	requireT.Empty(r.matchIndex)
	requireT.EqualValues(4, r.lastLogTerm)
	requireT.EqualValues(22, r.nextLogIndex)

	logEqual(requireT, []byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x03, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x04, 0x02, 0x01, 0x00,
	}, log)
}

/*
func TestSingleModeApplyHeartbeatTimeoutDoNothing(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(4))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x03, 0x03, 0x02, 0x00, 0x00,
	}, true)
	requireT.NoError(err)
	r := newReactorSingleMode(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	result, err := r.Apply(magmatypes.ZeroServerID, types.HeartbeatTick(1))
	requireT.NoError(err)

	requireT.Equal(types.RoleLeader, r.role)
	requireT.EqualValues(1, r.heartbeatTick)
	requireT.EqualValues(4, s.CurrentTerm())
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 19,
		},
	}, result)
	requireT.Empty(r.sync)
	requireT.Empty(r.matchIndex)
	requireT.Equal(types.CommitInfo{CommittedCount: 19}, r.commitInfo)
	requireT.EqualValues(4, r.lastLogTerm)
	requireT.EqualValues(19, r.nextLogIndex)

	_, _, entries, err := s.Entries(0, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x03, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x04,
	}, entries)
}
*/
