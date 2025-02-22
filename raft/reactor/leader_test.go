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

func TestLeaderSetup(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 2},
	})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(3))
	r, ts := newReactor(s)

	r.role = types.RoleCandidate
	r.leaderID = peer1ID
	r.votedForMe = 10
	r.indexTermStarted = 12
	r.nextIndex[peer1ID] = 100
	r.matchIndex[peer1ID] = 100

	requireT.EqualValues(2, r.lastLogTerm)
	requireT.EqualValues(3, r.nextLogIndex)
	r.committedCount = 2

	expectedHeartbeatTime := ts.Add(time.Hour)
	msg, err := r.transitionToLeader()
	requireT.NoError(err)

	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(serverID, r.leaderID)
	requireT.EqualValues(10, r.votedForMe)
	requireT.Equal(expectedHeartbeatTime, r.heartBeatTime)
	requireT.Equal(p2p.AppendEntriesRequest{
		MessageID:    msg.MessageID,
		Term:         3,
		NextLogIndex: 3,
		LastLogTerm:  2,
		Entries: []state.LogItem{
			{
				Term: 3,
				Data: nil,
			},
		},
		LeaderCommit: 2,
	}, msg)
	requireT.EqualValues(3, r.indexTermStarted)
	requireT.Empty(r.nextIndex)
	requireT.Equal(map[types.ServerID]types.Index{
		serverID: 4,
	}, r.matchIndex)

	requireT.EqualValues(3, r.lastLogTerm)
	requireT.EqualValues(4, r.nextLogIndex)
	requireT.EqualValues(2, r.committedCount)

	requireT.EqualValues(3, s.CurrentTerm())

	_, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 2},
		{Term: 3},
	}, entries)
}

func TestLeaderApplyAppendEntriesRequestTransitionToFollowerOnFutureTerm(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 2},
	})
	requireT.NoError(err)

	r, ts := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	expectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	msg, err := r.ApplyAppendEntriesRequest(peer1ID, p2p.AppendEntriesRequest{
		MessageID:    messageID,
		Term:         4,
		NextLogIndex: 3,
		LastLogTerm:  2,
		Entries: []state.LogItem{
			{
				Term: 3,
				Data: []byte{0x01},
			},
			{
				Term: 3,
				Data: []byte{0x02},
			},
			{
				Term: 4,
				Data: []byte{0x03},
			},
		},
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(p2p.AppendEntriesResponse{
		MessageID:    messageID,
		Term:         4,
		NextLogIndex: 6,
	}, msg)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Equal(peer1ID, r.leaderID)

	requireT.EqualValues(4, s.CurrentTerm())
	_, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 2},
		{
			Term: 3,
			Data: []byte{0x01},
		},
		{
			Term: 3,
			Data: []byte{0x02},
		},
		{
			Term: 4,
			Data: []byte{0x03},
		},
	}, entries)
}

func TestLeaderApplyAppendEntriesResponseTransitionToFollowerOnFutureTerm(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(1))
	r, ts := newReactor(s)
	_, err := r.transitionToLeader()
	requireT.NoError(err)

	expectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()

	msg, err := r.ApplyAppendEntriesResponse(peer1ID, p2p.AppendEntriesResponse{
		MessageID:    messageID,
		Term:         3,
		NextLogIndex: 2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Zero(r.votedForMe)
	requireT.Equal(p2p.ZeroMessageID, msg.MessageID)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Equal(serverID, r.leaderID)

	requireT.EqualValues(3, s.CurrentTerm())
}

func TestLeaderApplyVoteRequestTransitionToFollowerOnFutureTerm(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(1))
	r, ts := newReactor(s)
	_, err := r.transitionToLeader()
	requireT.NoError(err)
	requireT.EqualValues(1, s.CurrentTerm())

	expectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	msg, err := r.ApplyVoteRequest(peer1ID, p2p.VoteRequest{
		MessageID:    messageID,
		Term:         3,
		NextLogIndex: 1,
		LastLogTerm:  1,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(p2p.VoteResponse{
		MessageID:   messageID,
		Term:        3,
		VoteGranted: true,
	}, msg)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Equal(serverID, r.leaderID)

	requireT.EqualValues(3, s.CurrentTerm())

	granted, err := s.VoteFor(peer2ID)
	requireT.NoError(err)
	requireT.False(granted)

	granted, err = s.VoteFor(peer1ID)
	requireT.NoError(err)
	requireT.True(granted)
}

func TestLeaderApplyAppendEntriesResponseSendRemainingLogs(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
		{Term: 4},
	})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(5))
	r, _ := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	messageID := p2p.NewMessageID()
	r.nextIndex[peer1ID] = 0

	msg, err := r.ApplyAppendEntriesResponse(peer1ID, p2p.AppendEntriesResponse{
		MessageID:    messageID,
		Term:         5,
		NextLogIndex: 2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(p2p.AppendEntriesRequest{
		MessageID:    msg.MessageID,
		Term:         5,
		NextLogIndex: 2,
		LastLogTerm:  2,
		Entries: []state.LogItem{
			{Term: 3},
			{Term: 4},
			{Term: 5},
		},
	}, msg)
	requireT.EqualValues(2, r.nextIndex[peer1ID])
	requireT.EqualValues(2, r.matchIndex[peer1ID])
	requireT.Zero(r.committedCount)
	requireT.Equal(serverID, r.leaderID)
}

func TestLeaderApplyAppendEntriesResponseSendEarlierLogs(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
		{Term: 4},
	})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(5))
	r, _ := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	messageID := p2p.NewMessageID()

	msg, err := r.ApplyAppendEntriesResponse(peer1ID, p2p.AppendEntriesResponse{
		MessageID:    messageID,
		Term:         5,
		NextLogIndex: 2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(p2p.AppendEntriesRequest{
		MessageID:    msg.MessageID,
		Term:         5,
		NextLogIndex: 2,
		LastLogTerm:  2,
		Entries: []state.LogItem{
			{Term: 3},
			{Term: 4},
			{Term: 5},
		},
	}, msg)
	requireT.EqualValues(2, r.nextIndex[peer1ID])
	requireT.EqualValues(0, r.matchIndex[peer1ID])
	requireT.Zero(r.committedCount)
	requireT.Equal(serverID, r.leaderID)
}

func TestLeaderApplyAppendEntriesResponseNothingMoreToSend(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
		{Term: 4},
	})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(5))
	r, _ := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	clear(r.nextIndex)

	messageID := p2p.NewMessageID()

	msg, err := r.ApplyAppendEntriesResponse(peer1ID, p2p.AppendEntriesResponse{
		MessageID:    messageID,
		Term:         5,
		NextLogIndex: 5,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(p2p.ZeroMessageID, msg.MessageID)
	requireT.EqualValues(5, r.nextIndex[peer1ID])
	requireT.EqualValues(5, r.matchIndex[peer1ID])
	requireT.Zero(r.committedCount)
	requireT.Equal(serverID, r.leaderID)
}

func TestLeaderApplyAppendEntriesResponseCommitToLast(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
		{Term: 4},
	})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(5))
	r, _ := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	clear(r.nextIndex)

	r.committedCount = 0
	r.matchIndex[serverID] = 5

	_, err = r.ApplyAppendEntriesResponse(peer1ID, p2p.AppendEntriesResponse{
		MessageID:    p2p.NewMessageID(),
		Term:         5,
		NextLogIndex: 5,
	})
	requireT.NoError(err)
	requireT.EqualValues(5, r.matchIndex[peer1ID])
	requireT.Zero(r.committedCount)

	_, err = r.ApplyAppendEntriesResponse(peer2ID, p2p.AppendEntriesResponse{
		MessageID:    p2p.NewMessageID(),
		Term:         5,
		NextLogIndex: 5,
	})
	requireT.NoError(err)
	requireT.EqualValues(5, r.matchIndex[peer2ID])
	requireT.EqualValues(5, r.committedCount)
}

func TestLeaderApplyAppendEntriesResponseCommitToPrevious(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
		{Term: 4},
	})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(5))
	r, _ := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)
	_, _, err = s.Append(0, 0, []state.LogItem{
		{Term: 5},
	})
	requireT.NoError(err)

	clear(r.nextIndex)

	r.committedCount = 0
	r.matchIndex[serverID] = 5

	_, err = r.ApplyAppendEntriesResponse(peer1ID, p2p.AppendEntriesResponse{
		MessageID:    p2p.NewMessageID(),
		Term:         5,
		NextLogIndex: 5,
	})
	requireT.NoError(err)
	requireT.EqualValues(5, r.matchIndex[peer1ID])
	requireT.Zero(r.committedCount)

	_, err = r.ApplyAppendEntriesResponse(peer2ID, p2p.AppendEntriesResponse{
		MessageID:    p2p.NewMessageID(),
		Term:         5,
		NextLogIndex: 5,
	})
	requireT.NoError(err)
	requireT.EqualValues(5, r.matchIndex[peer2ID])
	requireT.EqualValues(5, r.committedCount)
}

func TestLeaderApplyAppendEntriesResponseCommitToCommonHeight(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
		{Term: 4},
	})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(5))
	r, _ := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)
	_, _, err = s.Append(0, 0, []state.LogItem{
		{Term: 5},
		{Term: 5},
	})
	requireT.NoError(err)

	clear(r.nextIndex)

	r.committedCount = 0
	r.matchIndex[serverID] = 5

	_, err = r.ApplyAppendEntriesResponse(peer1ID, p2p.AppendEntriesResponse{
		MessageID:    p2p.NewMessageID(),
		Term:         5,
		NextLogIndex: 6,
	})
	requireT.NoError(err)
	requireT.EqualValues(6, r.matchIndex[peer1ID])
	requireT.Zero(r.committedCount)

	_, err = r.ApplyAppendEntriesResponse(peer2ID, p2p.AppendEntriesResponse{
		MessageID:    p2p.NewMessageID(),
		Term:         5,
		NextLogIndex: 7,
	})
	requireT.NoError(err)
	requireT.EqualValues(7, r.matchIndex[peer2ID])
	requireT.EqualValues(5, r.committedCount)
}

func TestLeaderApplyAppendEntriesResponseNoCommitToOldTerm(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
		{Term: 4},
	})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(5))
	r, _ := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.nextIndex = map[types.ServerID]types.Index{
		peer1ID: 0,
		peer2ID: 0,
		peer3ID: 0,
		peer4ID: 0,
	}

	r.committedCount = 0
	r.matchIndex[serverID] = 5

	_, err = r.ApplyAppendEntriesResponse(peer1ID, p2p.AppendEntriesResponse{
		MessageID:    p2p.NewMessageID(),
		Term:         5,
		NextLogIndex: 5,
	})
	requireT.NoError(err)
	requireT.EqualValues(5, r.matchIndex[peer1ID])
	requireT.Zero(r.committedCount)

	_, err = r.ApplyAppendEntriesResponse(peer2ID, p2p.AppendEntriesResponse{
		MessageID:    p2p.NewMessageID(),
		Term:         5,
		NextLogIndex: 4,
	})
	requireT.NoError(err)
	requireT.EqualValues(4, r.matchIndex[peer2ID])
	requireT.Zero(r.committedCount)
}

func TestLeaderApplyAppendEntriesResponseNoCommitBelowPreviousOne(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
		{Term: 4},
	})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(5))
	r, _ := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)
	_, _, err = s.Append(0, 0, []state.LogItem{
		{Term: 5},
		{Term: 5},
	})
	requireT.NoError(err)

	clear(r.nextIndex)

	r.committedCount = 7
	r.matchIndex[serverID] = 7

	_, err = r.ApplyAppendEntriesResponse(peer1ID, p2p.AppendEntriesResponse{
		MessageID:    p2p.NewMessageID(),
		Term:         5,
		NextLogIndex: 5,
	})
	requireT.NoError(err)
	requireT.EqualValues(5, r.matchIndex[peer1ID])
	requireT.EqualValues(7, r.committedCount)

	_, err = r.ApplyAppendEntriesResponse(peer2ID, p2p.AppendEntriesResponse{
		MessageID:    p2p.NewMessageID(),
		Term:         5,
		NextLogIndex: 6,
	})
	requireT.NoError(err)
	requireT.EqualValues(6, r.matchIndex[peer2ID])
	requireT.EqualValues(7, r.committedCount)
}

func TestLeaderApplyHeartbeatTimeoutAfterHeartbeatTime(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
		{Term: 4},
	})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(5))
	r, ts := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.nextIndex = map[types.ServerID]types.Index{
		peer1ID: 5,
		peer2ID: 5,
		peer3ID: 5,
		peer4ID: 5,
	}

	r.heartBeatTime = ts.Add(time.Hour)
	heartbeatTimeoutTime := ts.Add(time.Hour)
	expectedHeartbeatTime := ts.Add(time.Hour)

	msg, err := r.ApplyHeartbeatTimeout(heartbeatTimeoutTime)
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(p2p.AppendEntriesRequest{
		MessageID:    msg.MessageID,
		Term:         5,
		NextLogIndex: 5,
		LastLogTerm:  5,
		Entries:      []state.LogItem{},
	}, msg)
	requireT.Equal(expectedHeartbeatTime, r.heartBeatTime)
}

func TestLeaderApplyHeartbeatTimeoutBeforeHeartbeatTime(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
		{Term: 4},
	})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(5))
	r, ts := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.nextIndex = map[types.ServerID]types.Index{
		peer1ID: 5,
		peer2ID: 5,
		peer3ID: 5,
		peer4ID: 5,
	}

	heartbeatTimeoutTime := ts.Add(time.Hour)
	r.heartBeatTime = ts.Add(time.Hour)
	notExpectedHeartbeatTime := ts.Add(time.Hour)

	msg, err := r.ApplyHeartbeatTimeout(heartbeatTimeoutTime)
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(p2p.ZeroMessageID, msg.MessageID)
	requireT.NotEqual(notExpectedHeartbeatTime, r.heartBeatTime)
}

func TestLeaderApplyClientRequestAppendAndBroadcast(t *testing.T) {
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
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	expectedHeartbeatTime := ts.Add(time.Hour)

	msg, err := r.ApplyClientRequest(p2c.ClientRequest{
		Data: []byte{0x01},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(p2p.AppendEntriesRequest{
		MessageID:    msg.MessageID,
		Term:         4,
		NextLogIndex: 6,
		LastLogTerm:  4,
		Entries: []state.LogItem{
			{
				Term: 4,
				Data: []byte{0x01},
			},
		},
	}, msg)
	requireT.Equal(expectedHeartbeatTime, r.heartBeatTime)
	requireT.Empty(r.nextIndex)
	requireT.Equal(map[types.ServerID]types.Index{
		serverID: 7,
	}, r.matchIndex)
	requireT.EqualValues(4, r.lastLogTerm)
	requireT.EqualValues(7, r.nextLogIndex)
	requireT.EqualValues(0, r.committedCount)

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

func TestLeaderApplyPeerConnected(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
		{Term: 4},
	})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(5))
	r, _ := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.nextIndex = map[types.ServerID]types.Index{
		peer1ID: 1,
		peer2ID: 2,
		peer3ID: 3,
		peer4ID: 4,
	}
	r.matchIndex = map[types.ServerID]types.Index{
		peer1ID: 1,
		peer2ID: 2,
		peer3ID: 3,
		peer4ID: 4,
	}

	msg, err := r.ApplyPeerConnected(peer1ID)
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(map[types.ServerID]types.Index{
		peer1ID: 5,
		peer2ID: 2,
		peer3ID: 3,
		peer4ID: 4,
	}, r.nextIndex)
	requireT.Equal(map[types.ServerID]types.Index{
		peer1ID: 0,
		peer2ID: 2,
		peer3ID: 3,
		peer4ID: 4,
	}, r.matchIndex)
	requireT.Equal(p2p.AppendEntriesRequest{
		MessageID:    msg.MessageID,
		Term:         5,
		NextLogIndex: 5,
		LastLogTerm:  5,
		Entries:      []state.LogItem{},
	}, msg)
	requireT.Equal(serverID, r.leaderID)
}
