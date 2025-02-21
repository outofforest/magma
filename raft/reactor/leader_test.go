package reactor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/raft/state"
	"github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/raft/wire/p2p"
)

func TestLeaderSetup(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, success, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 2},
	})
	requireT.NoError(err)
	requireT.True(success)
	requireT.NoError(s.SetCurrentTerm(3))
	r, ts := newReactor(s)

	r.role = types.RoleCandidate
	r.leaderID = peer1ID
	r.votedForMe = 10
	r.nextIndex[peer1ID] = 100
	r.matchIndex[peer1ID] = 100
	r.callInProgress[peer1ID] = p2p.NewMessageID()

	requireT.EqualValues(2, r.lastLogTerm)
	requireT.EqualValues(3, r.nextLogIndex)
	r.committedCount = 2

	expectedHeartbeatTime := ts.Add(time.Hour)
	messages, err := r.transitionToLeader()
	requireT.NoError(err)

	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(serverID, r.leaderID)
	requireT.EqualValues(10, r.votedForMe)
	requireT.Equal(expectedHeartbeatTime, r.heartBeatTime)
	requireT.NotEmpty(messages)
	messageID := messages[0].Msg.(p2p.AppendEntriesRequest).MessageID
	requireT.Equal([]p2p.Message{
		{
			PeerID: peer1ID,
			Msg: p2p.AppendEntriesRequest{
				MessageID:    messageID,
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
			},
		},
		{
			PeerID: peer2ID,
			Msg: p2p.AppendEntriesRequest{
				MessageID:    messageID,
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
			},
		},
		{
			PeerID: peer3ID,
			Msg: p2p.AppendEntriesRequest{
				MessageID:    messageID,
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
			},
		},
		{
			PeerID: peer4ID,
			Msg: p2p.AppendEntriesRequest{
				MessageID:    messageID,
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
			},
		},
	}, messages)
	requireT.Equal(map[types.ServerID]p2p.MessageID{
		peer1ID: messageID,
		peer2ID: messageID,
		peer3ID: messageID,
		peer4ID: messageID,
	}, r.callInProgress)
	requireT.Equal(map[types.ServerID]types.Index{
		peer1ID: 3,
		peer2ID: 3,
		peer3ID: 3,
		peer4ID: 3,
	}, r.nextIndex)
	requireT.Equal(map[types.ServerID]types.Index{
		serverID: 4,
	}, r.matchIndex)

	requireT.EqualValues(3, r.lastLogTerm)
	requireT.EqualValues(4, r.nextLogIndex)
	requireT.EqualValues(2, r.committedCount)

	requireT.EqualValues(3, s.CurrentTerm())
}

func TestLeaderApplyAppendEntriesRequestTransitionToFollowerOnFutureTerm(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, success, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 2},
	})
	requireT.NoError(err)
	requireT.True(success)

	r, ts := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	expectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.AppendEntriesRequest{
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
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Equal([]p2p.Message{
		{
			PeerID: peer1ID,
			Msg: p2p.AppendEntriesResponse{
				MessageID:    messageID,
				Term:         4,
				NextLogIndex: 6,
			},
		},
	}, messages)
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
	r.callInProgress[peer1ID] = messageID

	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.AppendEntriesResponse{
			MessageID:    messageID,
			Term:         3,
			NextLogIndex: 2,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Zero(r.votedForMe)
	requireT.Empty(messages)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Empty(r.callInProgress)
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
	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.VoteRequest{
			MessageID:    messageID,
			Term:         3,
			NextLogIndex: 1,
			LastLogTerm:  1,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Equal([]p2p.Message{
		{
			PeerID: peer1ID,
			Msg: p2p.VoteResponse{
				MessageID:   messageID,
				Term:        3,
				VoteGranted: true,
			},
		},
	}, messages)
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

func TestLeaderApplyAppendEntriesResponseSendRemainingLogsOnSuccess(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, success, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
		{Term: 4},
	})
	requireT.NoError(err)
	requireT.True(success)
	requireT.NoError(s.SetCurrentTerm(5))
	r, _ := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	messageID := p2p.NewMessageID()
	r.callInProgress[peer1ID] = messageID
	r.nextIndex[peer1ID] = 0

	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.AppendEntriesResponse{
			MessageID:    messageID,
			Term:         5,
			NextLogIndex: 2,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, role)
	requireT.NotEmpty(messages)
	messageID = messages[0].Msg.(p2p.AppendEntriesRequest).MessageID
	requireT.Equal([]p2p.Message{
		{
			PeerID: peer1ID,
			Msg: p2p.AppendEntriesRequest{
				MessageID:    messageID,
				Term:         5,
				NextLogIndex: 2,
				LastLogTerm:  2,
				Entries: []state.LogItem{
					{Term: 3},
					{Term: 4},
					{Term: 5},
				},
			},
		},
	}, messages)
	requireT.EqualValues(2, r.nextIndex[peer1ID])
	requireT.EqualValues(2, r.matchIndex[peer1ID])
	requireT.Equal(messageID, r.callInProgress[peer1ID])
	requireT.Zero(r.committedCount)
	requireT.Equal(serverID, r.leaderID)
}

func TestLeaderApplyAppendEntriesResponseSendLogsOnFailure(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, success, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
		{Term: 4},
	})
	requireT.NoError(err)
	requireT.True(success)
	requireT.NoError(s.SetCurrentTerm(5))
	r, _ := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	messageID := p2p.NewMessageID()
	r.callInProgress[peer1ID] = messageID

	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.AppendEntriesResponse{
			MessageID:    messageID,
			Term:         5,
			NextLogIndex: 2,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, role)
	requireT.NotEmpty(messages)
	messageID = messages[0].Msg.(p2p.AppendEntriesRequest).MessageID
	requireT.Equal([]p2p.Message{
		{
			PeerID: peer1ID,
			Msg: p2p.AppendEntriesRequest{
				MessageID:    messageID,
				Term:         5,
				NextLogIndex: 2,
				LastLogTerm:  2,
				Entries: []state.LogItem{
					{Term: 3},
					{Term: 4},
					{Term: 5},
				},
			},
		},
	}, messages)
	requireT.EqualValues(2, r.nextIndex[peer1ID])
	requireT.EqualValues(0, r.matchIndex[peer1ID])
	requireT.Equal(messageID, r.callInProgress[peer1ID])
	requireT.Zero(r.committedCount)
	requireT.Equal(serverID, r.leaderID)
}

func TestLeaderApplyAppendEntriesResponseNothingMoreToSend(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, success, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
		{Term: 4},
	})
	requireT.NoError(err)
	requireT.True(success)
	requireT.NoError(s.SetCurrentTerm(5))
	r, _ := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	clear(r.nextIndex)

	messageID := p2p.NewMessageID()
	r.callInProgress[peer1ID] = messageID

	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.AppendEntriesResponse{
			MessageID:    messageID,
			Term:         5,
			NextLogIndex: 5,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, role)
	requireT.Empty(messages)
	requireT.EqualValues(5, r.nextIndex[peer1ID])
	requireT.EqualValues(5, r.matchIndex[peer1ID])
	requireT.Equal(p2p.ZeroMessageID, r.callInProgress[peer1ID])
	requireT.Zero(r.committedCount)
	requireT.Equal(serverID, r.leaderID)
}

func TestLeaderApplyAppendEntriesResponseCommitToLast(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, success, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
		{Term: 4},
	})
	requireT.NoError(err)
	requireT.True(success)
	requireT.NoError(s.SetCurrentTerm(5))
	r, _ := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	clear(r.nextIndex)

	r.committedCount = 0
	r.matchIndex[serverID] = 5

	messageID := p2p.NewMessageID()
	r.callInProgress[peer1ID] = messageID

	_, _, err = r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.AppendEntriesResponse{
			MessageID:    messageID,
			Term:         5,
			NextLogIndex: 5,
		},
	})
	requireT.NoError(err)
	requireT.EqualValues(5, r.matchIndex[peer1ID])
	requireT.Zero(r.committedCount)

	messageID = p2p.NewMessageID()
	r.callInProgress[peer2ID] = messageID

	_, _, err = r.Apply(p2p.Message{
		PeerID: peer2ID,
		Msg: p2p.AppendEntriesResponse{
			MessageID:    messageID,
			Term:         5,
			NextLogIndex: 5,
		},
	})
	requireT.NoError(err)
	requireT.EqualValues(5, r.matchIndex[peer2ID])
	requireT.EqualValues(5, r.committedCount)
}

func TestLeaderApplyAppendEntriesResponseCommitToPrevious(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, success, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
		{Term: 4},
	})
	requireT.NoError(err)
	requireT.True(success)
	requireT.NoError(s.SetCurrentTerm(5))
	r, _ := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)
	_, _, success, err = s.Append(0, 0, []state.LogItem{
		{Term: 5},
	})
	requireT.NoError(err)
	requireT.True(success)

	clear(r.nextIndex)

	r.committedCount = 0
	r.matchIndex[serverID] = 5

	messageID := p2p.NewMessageID()
	r.callInProgress[peer1ID] = messageID

	_, _, err = r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.AppendEntriesResponse{
			MessageID:    messageID,
			Term:         5,
			NextLogIndex: 5,
		},
	})
	requireT.NoError(err)
	requireT.EqualValues(5, r.matchIndex[peer1ID])
	requireT.Zero(r.committedCount)

	messageID = p2p.NewMessageID()
	r.callInProgress[peer2ID] = messageID

	_, _, err = r.Apply(p2p.Message{
		PeerID: peer2ID,
		Msg: p2p.AppendEntriesResponse{
			MessageID:    messageID,
			Term:         5,
			NextLogIndex: 5,
		},
	})
	requireT.NoError(err)
	requireT.EqualValues(5, r.matchIndex[peer2ID])
	requireT.EqualValues(5, r.committedCount)
}

func TestLeaderApplyAppendEntriesResponseCommitToCommonHeight(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, success, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
		{Term: 4},
	})
	requireT.NoError(err)
	requireT.True(success)
	requireT.NoError(s.SetCurrentTerm(5))
	r, _ := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)
	_, _, success, err = s.Append(0, 0, []state.LogItem{
		{Term: 5},
		{Term: 5},
	})
	requireT.NoError(err)
	requireT.True(success)

	clear(r.nextIndex)

	r.committedCount = 0
	r.matchIndex[serverID] = 5

	messageID := p2p.NewMessageID()
	r.callInProgress[peer1ID] = messageID

	_, _, err = r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.AppendEntriesResponse{
			MessageID:    messageID,
			Term:         5,
			NextLogIndex: 6,
		},
	})
	requireT.NoError(err)
	requireT.EqualValues(6, r.matchIndex[peer1ID])
	requireT.Zero(r.committedCount)

	messageID = p2p.NewMessageID()
	r.callInProgress[peer2ID] = messageID

	_, _, err = r.Apply(p2p.Message{
		PeerID: peer2ID,
		Msg: p2p.AppendEntriesResponse{
			MessageID:    messageID,
			Term:         5,
			NextLogIndex: 7,
		},
	})
	requireT.NoError(err)
	requireT.EqualValues(7, r.matchIndex[peer2ID])
	requireT.EqualValues(5, r.committedCount)
}

func TestLeaderApplyAppendEntriesResponseNoCommitToOldTerm(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, success, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
		{Term: 4},
	})
	requireT.NoError(err)
	requireT.True(success)
	requireT.NoError(s.SetCurrentTerm(5))
	r, _ := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	clear(r.nextIndex)

	r.committedCount = 0
	r.matchIndex[serverID] = 5

	messageID := p2p.NewMessageID()
	r.callInProgress[peer1ID] = messageID

	_, _, err = r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.AppendEntriesResponse{
			MessageID:    messageID,
			Term:         5,
			NextLogIndex: 5,
		},
	})
	requireT.NoError(err)
	requireT.EqualValues(5, r.matchIndex[peer1ID])
	requireT.Zero(r.committedCount)

	messageID = p2p.NewMessageID()
	r.callInProgress[peer2ID] = messageID

	_, _, err = r.Apply(p2p.Message{
		PeerID: peer2ID,
		Msg: p2p.AppendEntriesResponse{
			MessageID:    messageID,
			Term:         5,
			NextLogIndex: 4,
		},
	})
	requireT.NoError(err)
	requireT.EqualValues(4, r.matchIndex[peer2ID])
	requireT.Zero(r.committedCount)
}

func TestLeaderApplyAppendEntriesResponseNoCommitBelowPreviousOne(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, success, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
		{Term: 4},
	})
	requireT.NoError(err)
	requireT.True(success)
	requireT.NoError(s.SetCurrentTerm(5))
	r, _ := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)
	_, _, success, err = s.Append(0, 0, []state.LogItem{
		{Term: 5},
		{Term: 5},
	})
	requireT.NoError(err)
	requireT.True(success)

	clear(r.nextIndex)

	r.committedCount = 7
	r.matchIndex[serverID] = 7

	messageID := p2p.NewMessageID()
	r.callInProgress[peer1ID] = messageID

	_, _, err = r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.AppendEntriesResponse{
			MessageID:    messageID,
			Term:         5,
			NextLogIndex: 5,
		},
	})
	requireT.NoError(err)
	requireT.EqualValues(5, r.matchIndex[peer1ID])
	requireT.EqualValues(7, r.committedCount)

	messageID = p2p.NewMessageID()
	r.callInProgress[peer2ID] = messageID

	_, _, err = r.Apply(p2p.Message{
		PeerID: peer2ID,
		Msg: p2p.AppendEntriesResponse{
			MessageID:    messageID,
			Term:         5,
			NextLogIndex: 6,
		},
	})
	requireT.NoError(err)
	requireT.EqualValues(6, r.matchIndex[peer2ID])
	requireT.EqualValues(7, r.committedCount)
}

func TestLeaderApplyHeartbeatTimeoutAfterHeartbeatTime(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, success, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
		{Term: 4},
	})
	requireT.NoError(err)
	requireT.True(success)
	requireT.NoError(s.SetCurrentTerm(5))
	r, ts := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	clear(r.callInProgress)
	r.nextIndex = map[types.ServerID]types.Index{
		peer1ID: 5,
		peer2ID: 5,
		peer3ID: 5,
		peer4ID: 5,
	}

	r.heartBeatTime = ts.Add(time.Hour)
	heartbeatTimeoutTime := ts.Add(time.Hour)
	expectedHeartbeatTime := ts.Add(time.Hour)

	role, messages, err := r.Apply(p2p.Message{
		Msg: types.HeartbeatTimeout{
			Time: heartbeatTimeoutTime,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, role)
	requireT.NotEmpty(messages)
	messageID := messages[0].Msg.(p2p.AppendEntriesRequest).MessageID
	requireT.Equal([]p2p.Message{
		{
			PeerID: peer1ID,
			Msg: p2p.AppendEntriesRequest{
				MessageID:    messageID,
				Term:         5,
				NextLogIndex: 5,
				LastLogTerm:  5,
				Entries:      []state.LogItem{},
			},
		},
		{
			PeerID: peer2ID,
			Msg: p2p.AppendEntriesRequest{
				MessageID:    messageID,
				Term:         5,
				NextLogIndex: 5,
				LastLogTerm:  5,
				Entries:      []state.LogItem{},
			},
		},
		{
			PeerID: peer3ID,
			Msg: p2p.AppendEntriesRequest{
				MessageID:    messageID,
				Term:         5,
				NextLogIndex: 5,
				LastLogTerm:  5,
				Entries:      []state.LogItem{},
			},
		},
		{
			PeerID: peer4ID,
			Msg: p2p.AppendEntriesRequest{
				MessageID:    messageID,
				Term:         5,
				NextLogIndex: 5,
				LastLogTerm:  5,
				Entries:      []state.LogItem{},
			},
		},
	}, messages)
	requireT.Equal(expectedHeartbeatTime, r.heartBeatTime)
}

func TestLeaderApplyHeartbeatTimeoutBeforeHeartbeatTime(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, success, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
		{Term: 4},
	})
	requireT.NoError(err)
	requireT.True(success)
	requireT.NoError(s.SetCurrentTerm(5))
	r, ts := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	clear(r.callInProgress)
	r.nextIndex = map[types.ServerID]types.Index{
		peer1ID: 5,
		peer2ID: 5,
		peer3ID: 5,
		peer4ID: 5,
	}

	heartbeatTimeoutTime := ts.Add(time.Hour)
	r.heartBeatTime = ts.Add(time.Hour)
	notExpectedHeartbeatTime := ts.Add(time.Hour)

	role, messages, err := r.Apply(p2p.Message{
		Msg: types.HeartbeatTimeout{
			Time: heartbeatTimeoutTime,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, role)
	requireT.Empty(messages)
	requireT.NotEqual(notExpectedHeartbeatTime, r.heartBeatTime)
}

func TestLeaderApplyPeerConnected(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, success, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 3},
		{Term: 4},
	})
	requireT.NoError(err)
	requireT.True(success)
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

	oldMessageID := p2p.NewMessageID()
	r.callInProgress = map[types.ServerID]p2p.MessageID{
		peer1ID: oldMessageID,
		peer2ID: oldMessageID,
		peer3ID: oldMessageID,
		peer4ID: oldMessageID,
	}

	role, messages, err := r.Apply(p2p.Message{
		Msg: peer1ID,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, role)
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
	requireT.NotEmpty(messages)
	messageID := messages[0].Msg.(p2p.AppendEntriesRequest).MessageID
	requireT.Equal(map[types.ServerID]p2p.MessageID{
		peer1ID: messageID,
		peer2ID: oldMessageID,
		peer3ID: oldMessageID,
		peer4ID: oldMessageID,
	}, r.callInProgress)
	requireT.Equal([]p2p.Message{
		{
			PeerID: peer1ID,
			Msg: p2p.AppendEntriesRequest{
				MessageID:    messageID,
				Term:         5,
				NextLogIndex: 5,
				LastLogTerm:  5,
				Entries:      []state.LogItem{},
			},
		},
	}, messages)
	requireT.Equal(serverID, r.leaderID)
}
