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
	return New(serverID, 1, s, timeSource), timeSource
}

func TestSingleModeApplyElectionTimeoutTransitionToLeader(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	r, ts := newReactorSingleMode(s)

	electionTimeoutTime := ts.Add(time.Hour)
	expectedElectionTime := ts.Add(time.Hour)

	msg, err := r.ApplyElectionTimeout(electionTimeoutTime)
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Equal(expectedElectionTime, r.heartBeatTime)
	requireT.EqualValues(1, s.CurrentTerm())
	requireT.EqualValues(1, r.votedForMe)
	requireT.Nil(msg)
	requireT.Empty(r.nextIndex)
	requireT.Equal(map[magmatypes.ServerID]types.Index{
		serverID: 1,
	}, r.matchIndex)
	requireT.EqualValues(1, r.committedCount)
	requireT.EqualValues(1, r.lastLogTerm)
	requireT.EqualValues(1, r.nextLogIndex)

	granted, err := s.VoteFor(peer1ID)
	requireT.NoError(err)
	requireT.False(granted)

	granted, err = s.VoteFor(serverID)
	requireT.NoError(err)
	requireT.True(granted)

	_, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]types.LogItem{
		{Term: 1},
	}, entries)
}

func TestSingleModeApplyClientRequestAppend(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, err := s.Append(0, 0, []types.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 2},
		{Term: 3},
		{Term: 3},
	})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(4))
	r, ts := newReactorSingleMode(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	expectedHeartbeatTime := ts.Add(time.Hour)

	msg, err := r.ApplyClientRequest(&types.ClientRequest{
		Data: []byte{0x01},
	})
	requireT.NoError(err)

	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(expectedHeartbeatTime, r.heartBeatTime)
	requireT.EqualValues(4, s.CurrentTerm())
	requireT.Nil(msg)
	requireT.Empty(r.nextIndex)
	requireT.Equal(map[magmatypes.ServerID]types.Index{
		serverID: 7,
	}, r.matchIndex)
	requireT.EqualValues(7, r.committedCount)
	requireT.EqualValues(4, r.lastLogTerm)
	requireT.EqualValues(7, r.nextLogIndex)

	_, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]types.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 2},
		{Term: 3},
		{Term: 3},
		{Term: 4},
		{
			Term: 4,
			Data: []byte{0x01},
		},
	}, entries)
}

func TestSingleModeApplyHeartbeatTimeoutDoNothing(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, err := s.Append(0, 0, []types.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 2},
		{Term: 3},
		{Term: 3},
	})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(4))
	r, ts := newReactorSingleMode(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	heartbeatTimeoutTime := ts.Add(time.Hour)
	expectedHeartbeatTime := ts.Add(time.Hour)

	msg, err := r.ApplyHeartbeatTimeout(heartbeatTimeoutTime)
	requireT.NoError(err)

	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(expectedHeartbeatTime, r.heartBeatTime)
	requireT.EqualValues(4, s.CurrentTerm())
	requireT.Nil(msg)
	requireT.Empty(r.nextIndex)
	requireT.Equal(map[magmatypes.ServerID]types.Index{
		serverID: 6,
	}, r.matchIndex)
	requireT.EqualValues(6, r.committedCount)
	requireT.EqualValues(4, r.lastLogTerm)
	requireT.EqualValues(6, r.nextLogIndex)

	_, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]types.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 2},
		{Term: 3},
		{Term: 3},
		{Term: 4},
	}, entries)
}
