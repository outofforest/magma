package reactor

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/raft/state"
	"github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/raft/wire/p2p"
)

func TestCandidateSetup(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(1))
	r, ts := newReactor(s)

	r.role = types.RoleLeader
	r.votedForMe = 10
	r.nextIndex[peer1ID] = 100
	r.matchIndex[peer1ID] = 100
	r.callInProgress[peer1ID] = p2p.NewMessageID()

	r.lastLogTerm = 3
	r.nextLogIndex = 10
	r.committedCount = 5

	expectedElectionTime := ts.Add(time.Hour)
	messages, err := r.transitionToCandidate()
	requireT.NoError(err)

	requireT.Equal(types.RoleCandidate, r.role)
	requireT.EqualValues(1, r.votedForMe)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Empty(r.nextIndex)
	requireT.Empty(r.matchIndex)
	requireT.NotEmpty(messages)
	messageID := messages[0].Msg.(p2p.VoteRequest).MessageID
	requireT.Equal([]p2p.Message{
		{
			PeerID: peer1ID,
			Msg: p2p.VoteRequest{
				MessageID:    messageID,
				Term:         2,
				NextLogIndex: 10,
				LastLogTerm:  3,
			},
		},
		{
			PeerID: peer2ID,
			Msg: p2p.VoteRequest{
				MessageID:    messageID,
				Term:         2,
				NextLogIndex: 10,
				LastLogTerm:  3,
			},
		},
		{
			PeerID: peer3ID,
			Msg: p2p.VoteRequest{
				MessageID:    messageID,
				Term:         2,
				NextLogIndex: 10,
				LastLogTerm:  3,
			},
		},
		{
			PeerID: peer4ID,
			Msg: p2p.VoteRequest{
				MessageID:    messageID,
				Term:         2,
				NextLogIndex: 10,
				LastLogTerm:  3,
			},
		},
	}, messages)
	requireT.Equal(map[types.ServerID]p2p.MessageID{
		peer1ID: messageID,
		peer2ID: messageID,
		peer3ID: messageID,
		peer4ID: messageID,
	}, r.callInProgress)

	requireT.EqualValues(3, r.lastLogTerm)
	requireT.EqualValues(10, r.nextLogIndex)
	requireT.EqualValues(5, r.committedCount)

	requireT.EqualValues(2, s.CurrentTerm())

	granted, err := s.VoteFor(peer1ID)
	requireT.NoError(err)
	requireT.False(granted)

	granted, err = s.VoteFor(serverID)
	requireT.NoError(err)
	requireT.True(granted)
}

func TestCandidateApplyAppendEntriesRequestTransitionToFollowerOnFutureTerm(t *testing.T) {
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
	_, err = r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(3, s.CurrentTerm())

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

func TestCandidateApplyVoteRequestTransitionToFollowerOnFutureTerm(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(1))
	r, ts := newReactor(s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	expectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.VoteRequest{
			MessageID:    messageID,
			Term:         3,
			NextLogIndex: 0,
			LastLogTerm:  0,
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

func TestCandidateApplyVoteResponseTransitionToFollowerOnFutureTerm(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(1))
	r, ts := newReactor(s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	expectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.VoteResponse{
			MessageID:   messageID,
			Term:        3,
			VoteGranted: true,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Zero(r.votedForMe)
	requireT.Empty(messages)
	requireT.Equal(expectedElectionTime, r.electionTime)

	requireT.EqualValues(3, s.CurrentTerm())

	granted, err := s.VoteFor(peer2ID)
	requireT.NoError(err)
	requireT.True(granted)
}

func TestCandidateApplyVoteResponseIgnoreVoteFromPastTerm(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(1))
	r, ts := newReactor(s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	notExpectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.VoteResponse{
			MessageID:   messageID,
			Term:        1,
			VoteGranted: true,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, role)
	requireT.EqualValues(1, r.votedForMe)
	requireT.Empty(messages)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)

	requireT.EqualValues(2, s.CurrentTerm())

	granted, err := s.VoteFor(peer1ID)
	requireT.NoError(err)
	requireT.False(granted)
}

func TestCandidateApplyVoteResponseIgnoreNotExpectedResponse(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(1))
	r, ts := newReactor(s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	notExpectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.VoteResponse{
			MessageID:   messageID,
			Term:        2,
			VoteGranted: true,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, role)
	requireT.EqualValues(1, r.votedForMe)
	requireT.Empty(messages)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)

	requireT.EqualValues(2, s.CurrentTerm())
}

func TestCandidateApplyVoteResponseNotGranted(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(1))
	r, ts := newReactor(s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	notExpectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	r.callInProgress[peer1ID] = messageID

	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.VoteResponse{
			MessageID:   messageID,
			Term:        2,
			VoteGranted: false,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, role)
	requireT.EqualValues(1, r.votedForMe)
	requireT.Empty(messages)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)
	requireT.Equal(p2p.ZeroMessageID, r.callInProgress[peer1ID])

	requireT.EqualValues(2, s.CurrentTerm())
}

func TestCandidateApplyVoteResponseGranted(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(1))
	r, ts := newReactor(s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	notExpectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	r.callInProgress[peer1ID] = messageID

	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.VoteResponse{
			MessageID:   messageID,
			Term:        2,
			VoteGranted: true,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, role)
	requireT.EqualValues(2, r.votedForMe)
	requireT.Empty(messages)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)
	requireT.Equal(p2p.ZeroMessageID, r.callInProgress[peer1ID])

	requireT.EqualValues(2, s.CurrentTerm())
}

func TestCandidateApplyVoteResponseGrantedInNextTerm(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(1))
	r, ts := newReactor(s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	notExpectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	r.callInProgress[peer1ID] = messageID

	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.VoteResponse{
			MessageID:   messageID,
			Term:        2,
			VoteGranted: true,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, role)
	requireT.EqualValues(2, r.votedForMe)
	requireT.Empty(messages)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)
	requireT.Equal(p2p.ZeroMessageID, r.callInProgress[peer1ID])
	requireT.EqualValues(2, s.CurrentTerm())

	expectedElectionTime := ts.Add(time.Hour)

	_, err = r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(3, s.CurrentTerm())

	ts.Add(time.Hour)

	messageID = p2p.NewMessageID()
	r.callInProgress[peer1ID] = messageID

	role, messages, err = r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.VoteResponse{
			MessageID:   messageID,
			Term:        3,
			VoteGranted: true,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, role)
	requireT.EqualValues(2, r.votedForMe)
	requireT.Empty(messages)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Equal(p2p.ZeroMessageID, r.callInProgress[peer1ID])

	requireT.EqualValues(3, s.CurrentTerm())
}

func TestCandidateApplyVoteResponseGrantedFromMajority(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(1))
	r, ts := newReactor(s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	notExpectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	r.callInProgress[peer1ID] = messageID

	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.VoteResponse{
			MessageID:   messageID,
			Term:        2,
			VoteGranted: true,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, role)
	requireT.EqualValues(2, r.votedForMe)
	requireT.Empty(messages)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)
	requireT.Equal(p2p.ZeroMessageID, r.callInProgress[peer1ID])
	requireT.EqualValues(2, s.CurrentTerm())

	expectedHeartbeatTime := ts.Add(time.Hour)

	messageID = p2p.NewMessageID()
	r.callInProgress[peer2ID] = messageID

	role, messages, err = r.Apply(p2p.Message{
		PeerID: peer2ID,
		Msg: p2p.VoteResponse{
			MessageID:   messageID,
			Term:        2,
			VoteGranted: true,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, role)
	requireT.EqualValues(3, r.votedForMe)
	requireT.NotEmpty(messages)
	messageID = messages[0].Msg.(p2p.AppendEntriesRequest).MessageID
	requireT.Equal([]p2p.Message{
		{
			PeerID: peer1ID,
			Msg: p2p.AppendEntriesRequest{
				MessageID:    messageID,
				Term:         2,
				NextLogIndex: 0,
				LastLogTerm:  0,
				Entries: []state.LogItem{
					{
						Term: 2,
						Data: nil,
					},
				},
				LeaderCommit: 0,
			},
		},
		{
			PeerID: peer2ID,
			Msg: p2p.AppendEntriesRequest{
				MessageID:    messageID,
				Term:         2,
				NextLogIndex: 0,
				LastLogTerm:  0,
				Entries: []state.LogItem{
					{
						Term: 2,
						Data: nil,
					},
				},
				LeaderCommit: 0,
			},
		},
		{
			PeerID: peer3ID,
			Msg: p2p.AppendEntriesRequest{
				MessageID:    messageID,
				Term:         2,
				NextLogIndex: 0,
				LastLogTerm:  0,
				Entries: []state.LogItem{
					{
						Term: 2,
						Data: nil,
					},
				},
				LeaderCommit: 0,
			},
		},
		{
			PeerID: peer4ID,
			Msg: p2p.AppendEntriesRequest{
				MessageID:    messageID,
				Term:         2,
				NextLogIndex: 0,
				LastLogTerm:  0,
				Entries: []state.LogItem{
					{
						Term: 2,
						Data: nil,
					},
				},
				LeaderCommit: 0,
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
		peer1ID: 0,
		peer2ID: 0,
		peer3ID: 0,
		peer4ID: 0,
	}, r.nextIndex)
	requireT.Equal(map[types.ServerID]types.Index{
		serverID: 1,
	}, r.matchIndex)
	requireT.Equal(expectedHeartbeatTime, r.heartBeatTime)
	requireT.EqualValues(2, r.lastLogTerm)
	requireT.EqualValues(1, r.nextLogIndex)

	requireT.EqualValues(2, s.CurrentTerm())
}

func TestCandidateApplyHeartbeatTimeoutDoesNothing(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	r, ts := newReactor(s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(1, s.CurrentTerm())

	heartbeatTimeoutTime := ts.Add(time.Hour)
	notExpectedHeartbeatTime := ts.Add(time.Hour)

	role, messages, err := r.Apply(p2p.Message{
		Msg: types.HeartbeatTimeout{
			Time: heartbeatTimeoutTime,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, role)
	requireT.NotEqual(notExpectedHeartbeatTime, r.electionTime)
	requireT.Empty(messages)
}

func TestCandidateApplyPeerConnectedDoesNothing(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	r, _ := newReactor(s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(1, s.CurrentTerm())

	role, messages, err := r.Apply(p2p.Message{
		Msg: types.ServerID(uuid.New()),
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, role)
	requireT.Empty(messages)
}
