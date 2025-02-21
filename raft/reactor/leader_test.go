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
				Success:      true,
			},
		},
	}, messages)
	requireT.Equal(expectedElectionTime, r.electionTime)

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
			Success:      true,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Zero(r.votedForMe)
	requireT.Empty(messages)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Empty(r.callInProgress)

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

	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.AppendEntriesResponse{
			MessageID:    messageID,
			Term:         5,
			NextLogIndex: 2,
			Success:      true,
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
			Success:      false,
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

	messageID := p2p.NewMessageID()
	r.callInProgress[peer1ID] = messageID

	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.AppendEntriesResponse{
			MessageID:    messageID,
			Term:         5,
			NextLogIndex: 5,
			Success:      true,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, role)
	requireT.Empty(messages)
	requireT.EqualValues(5, r.nextIndex[peer1ID])
	requireT.EqualValues(5, r.matchIndex[peer1ID])
	requireT.Equal(p2p.ZeroMessageID, r.callInProgress[peer1ID])
}
