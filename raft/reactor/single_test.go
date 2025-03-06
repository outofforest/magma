package reactor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/raft/state"
	"github.com/outofforest/magma/raft/types"
	magmatypes "github.com/outofforest/magma/types"
)

func newReactorSingleMode(s *state.State) *Reactor {
	return New(serverID, nil, s, maxReadLogSize, maxReadLogSize)
}

func TestSingleModeApplyElectionTimeoutTransitionToLeader(t *testing.T) {
	requireT := require.New(t)
	s := newState()
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
			CommittedCount: 1,
		},
	}, result)
	requireT.Empty(r.sync)
	requireT.Empty(r.matchIndex)
	requireT.EqualValues(1, r.lastLogTerm)
	requireT.EqualValues(1, r.nextLogIndex)

	granted, err := s.VoteFor(peer1ID)
	requireT.NoError(err)
	requireT.False(granted)

	granted, err = s.VoteFor(serverID)
	requireT.NoError(err)
	requireT.True(granted)

	_, _, entries, err := s.Entries(0, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00}, entries)
}

func TestSingleModeApplyClientRequestAppend(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	_, _, err := s.Append(0, 0, 1, []byte{0x00})
	requireT.NoError(err)
	_, _, err = s.Append(1, 1, 2, []byte{0x00, 0x00})
	requireT.NoError(err)
	_, _, err = s.Append(3, 2, 3, []byte{0x00, 0x00})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(4))
	r := newReactorSingleMode(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	result, err := r.Apply(magmatypes.ZeroServerID, &types.ClientRequest{
		Data: []byte{0x01, 0x01},
	})
	requireT.NoError(err)

	requireT.Equal(types.RoleLeader, r.role)
	requireT.EqualValues(1, r.ignoreHeartbeatTick)
	requireT.EqualValues(4, s.CurrentTerm())
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 8,
		},
	}, result)
	requireT.Empty(r.sync)
	requireT.Empty(r.matchIndex)
	requireT.EqualValues(4, r.lastLogTerm)
	requireT.EqualValues(8, r.nextLogIndex)

	_, _, entries, err := s.Entries(0, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00}, entries)
	_, _, entries, err = s.Entries(1, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00, 0x00}, entries)
	_, _, entries, err = s.Entries(3, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00, 0x00}, entries)
	_, _, entries, err = s.Entries(5, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00, 0x01, 0x01}, entries)
}

func TestSingleModeApplyHeartbeatTimeoutDoNothing(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	_, _, err := s.Append(0, 0, 1, []byte{0x00})
	requireT.NoError(err)
	_, _, err = s.Append(1, 1, 2, []byte{0x00, 0x00})
	requireT.NoError(err)
	_, _, err = s.Append(3, 2, 3, []byte{0x00, 0x00})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(4))
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
			CommittedCount: 6,
		},
	}, result)
	requireT.Empty(r.sync)
	requireT.Empty(r.matchIndex)
	requireT.Equal(types.CommitInfo{CommittedCount: 6}, r.commitInfo)
	requireT.EqualValues(4, r.lastLogTerm)
	requireT.EqualValues(6, r.nextLogIndex)

	_, _, entries, err := s.Entries(0, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00}, entries)
	_, _, entries, err = s.Entries(1, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00, 0x00}, entries)
	_, _, entries, err = s.Entries(3, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00, 0x00}, entries)
	_, _, entries, err = s.Entries(5, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00}, entries)
}
