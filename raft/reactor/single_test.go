package reactor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/raft/state"
	"github.com/outofforest/magma/raft/types"
	magmatypes "github.com/outofforest/magma/types"
)

func newReactorSingleMode(s *state.State) (*Reactor, TimeAdvancer) {
	timeSource := &TestTimeSource{}
	return New(serverID, nil, s, maxReadLogSize, maxReadLogSize, timeSource), timeSource
}

func TestSingleModeApplyElectionTimeoutTransitionToLeader(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	r, ts := newReactorSingleMode(s)

	electionTimeoutTime := ts.Add(time.Hour)
	expectedElectionTime := ts.Add(time.Hour)

	result, err := r.Apply(magmatypes.ZeroServerID, types.ElectionTimeout(electionTimeoutTime))
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Equal(expectedElectionTime, r.heartBeatTime)
	requireT.EqualValues(1, s.CurrentTerm())
	requireT.EqualValues(1, r.votedForMe)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex: 1,
		},
	}, result)
	requireT.Empty(r.sync)
	requireT.Equal(map[magmatypes.ServerID]types.Index{
		serverID: 1,
	}, r.matchIndex)
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
	r, ts := newReactorSingleMode(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	expectedHeartbeatTime := ts.Add(time.Hour)

	result, err := r.Apply(magmatypes.ZeroServerID, &types.ClientRequest{
		Data: []byte{0x01, 0x01},
	})
	requireT.NoError(err)

	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(expectedHeartbeatTime, r.heartBeatTime)
	requireT.EqualValues(4, s.CurrentTerm())
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex: 8,
		},
	}, result)
	requireT.Empty(r.sync)
	requireT.Equal(map[magmatypes.ServerID]types.Index{
		serverID: 8,
	}, r.matchIndex)
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
	r, ts := newReactorSingleMode(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	heartbeatTimeoutTime := ts.Add(time.Hour)
	expectedHeartbeatTime := ts.Add(time.Hour)

	result, err := r.Apply(magmatypes.ZeroServerID, types.HeartbeatTimeout(heartbeatTimeoutTime))
	requireT.NoError(err)

	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(expectedHeartbeatTime, r.heartBeatTime)
	requireT.EqualValues(4, s.CurrentTerm())
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex: 6,
		},
	}, result)
	requireT.Empty(r.sync)
	requireT.Equal(map[magmatypes.ServerID]types.Index{
		serverID: 6,
	}, r.matchIndex)
	requireT.Equal(types.CommitInfo{NextLogIndex: 6}, r.commitInfo)
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
