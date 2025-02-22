package reactor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/raft/state"
	"github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/raft/wire/p2c"
	"github.com/outofforest/magma/raft/wire/p2p"
)

func TestSingleModeApplyElectionTimeoutTransitionToLeader(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	r, ts := newReactor(s)

	electionTimeoutTime := ts.Add(time.Hour)
	expectedElectionTime := ts.Add(time.Hour)

	msg, err := r.ApplyElectionTimeout(electionTimeoutTime, nil)
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Equal(expectedElectionTime, r.heartBeatTime)
	requireT.EqualValues(1, s.CurrentTerm())
	requireT.EqualValues(1, r.votedForMe)
	requireT.Equal(p2p.ZeroMessageID, msg.MessageID)
	requireT.Empty(r.nextIndex)
	requireT.Equal(map[types.ServerID]types.Index{
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
	requireT.EqualValues([]state.LogItem{
		{Term: 1},
	}, entries)
}

func TestSingleModeApplyClientRequestAppend(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 2},
		{Term: 3},
		{Term: 3},
	})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(4))
	r, ts := newReactor(s)
	_, err = r.transitionToLeader(nil)
	requireT.NoError(err)

	expectedHeartbeatTime := ts.Add(time.Hour)

	msg, err := r.ApplyClientRequest(p2c.ClientRequest{
		Data: []byte{0x01},
	}, nil)
	requireT.NoError(err)

	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(expectedHeartbeatTime, r.heartBeatTime)
	requireT.EqualValues(4, s.CurrentTerm())
	requireT.Equal(p2p.ZeroMessageID, msg.MessageID)
	requireT.Empty(r.nextIndex)
	requireT.Equal(map[types.ServerID]types.Index{
		serverID: 7,
	}, r.matchIndex)
	requireT.EqualValues(7, r.committedCount)
	requireT.EqualValues(4, r.lastLogTerm)
	requireT.EqualValues(7, r.nextLogIndex)

	_, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]state.LogItem{
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
	_, _, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 2},
		{Term: 3},
		{Term: 3},
	})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(4))
	r, ts := newReactor(s)
	_, err = r.transitionToLeader(nil)
	requireT.NoError(err)

	heartbeatTimeoutTime := ts.Add(time.Hour)
	expectedHeartbeatTime := ts.Add(time.Hour)

	msg, err := r.ApplyHeartbeatTimeout(heartbeatTimeoutTime, nil)
	requireT.NoError(err)

	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(expectedHeartbeatTime, r.heartBeatTime)
	requireT.EqualValues(4, s.CurrentTerm())
	requireT.Equal(p2p.ZeroMessageID, msg.MessageID)
	requireT.Empty(r.nextIndex)
	requireT.Equal(map[types.ServerID]types.Index{
		serverID: 6,
	}, r.matchIndex)
	requireT.EqualValues(6, r.committedCount)
	requireT.EqualValues(4, r.lastLogTerm)
	requireT.EqualValues(6, r.nextLogIndex)

	_, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 2},
		{Term: 3},
		{Term: 3},
		{Term: 4},
	}, entries)
}
