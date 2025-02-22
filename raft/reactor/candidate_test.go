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
	r.leaderID = serverID
	r.votedForMe = 10
	r.nextIndex[peer1ID] = 100
	r.matchIndex[peer1ID] = 100

	r.lastLogTerm = 3
	r.nextLogIndex = 10
	r.committedCount = 5

	expectedElectionTime := ts.Add(time.Hour)
	msg, err := r.transitionToCandidate(peers)
	requireT.NoError(err)

	requireT.Equal(types.RoleCandidate, r.role)
	requireT.Equal(types.ZeroServerID, r.leaderID)
	requireT.EqualValues(1, r.votedForMe)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Empty(r.nextIndex)
	requireT.Empty(r.matchIndex)
	requireT.Equal(p2p.VoteRequest{
		MessageID:    msg.MessageID,
		Term:         2,
		NextLogIndex: 10,
		LastLogTerm:  3,
	}, msg)

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

	_, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.Nil(entries)
}

func TestCandidateApplyAppendEntriesRequestTransitionToFollowerOnFutureTerm(t *testing.T) {
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
	_, err = r.transitionToCandidate(peers)
	requireT.NoError(err)
	requireT.EqualValues(3, s.CurrentTerm())

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

func TestCandidateApplyVoteRequestTransitionToFollowerOnFutureTerm(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(1))
	r, ts := newReactor(s)
	_, err := r.transitionToCandidate(peers)
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	expectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	msg, err := r.ApplyVoteRequest(peer1ID, p2p.VoteRequest{
		MessageID:    messageID,
		Term:         3,
		NextLogIndex: 0,
		LastLogTerm:  0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(p2p.VoteResponse{
		MessageID:   messageID,
		Term:        3,
		VoteGranted: true,
	}, msg)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Equal(types.ZeroServerID, r.leaderID)

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
	_, err := r.transitionToCandidate(peers)
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	expectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	msg, err := r.ApplyVoteResponse(peer1ID, p2p.VoteResponse{
		MessageID:   messageID,
		Term:        3,
		VoteGranted: true,
	}, peers)
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Zero(r.votedForMe)
	requireT.Equal(p2p.ZeroMessageID, msg.MessageID)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Equal(types.ZeroServerID, r.leaderID)

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
	_, err := r.transitionToCandidate(peers)
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	notExpectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	msg, err := r.ApplyVoteResponse(peer1ID, p2p.VoteResponse{
		MessageID:   messageID,
		Term:        1,
		VoteGranted: true,
	}, peers)
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.EqualValues(1, r.votedForMe)
	requireT.Equal(p2p.ZeroMessageID, msg.MessageID)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)
	requireT.Equal(types.ZeroServerID, r.leaderID)

	requireT.EqualValues(2, s.CurrentTerm())

	granted, err := s.VoteFor(peer1ID)
	requireT.NoError(err)
	requireT.False(granted)
}

func TestCandidateApplyVoteResponseNotGranted(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(1))
	r, ts := newReactor(s)
	_, err := r.transitionToCandidate(peers)
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	notExpectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()

	msg, err := r.ApplyVoteResponse(peer1ID, p2p.VoteResponse{
		MessageID:   messageID,
		Term:        2,
		VoteGranted: false,
	}, peers)
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.EqualValues(1, r.votedForMe)
	requireT.Equal(p2p.ZeroMessageID, msg.MessageID)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)
	requireT.Equal(types.ZeroServerID, r.leaderID)

	requireT.EqualValues(2, s.CurrentTerm())
}

func TestCandidateApplyVoteResponseGranted(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(1))
	r, ts := newReactor(s)
	_, err := r.transitionToCandidate(peers)
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	notExpectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()

	msg, err := r.ApplyVoteResponse(peer1ID, p2p.VoteResponse{
		MessageID:   messageID,
		Term:        2,
		VoteGranted: true,
	}, peers)
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.EqualValues(2, r.votedForMe)
	requireT.Equal(p2p.ZeroMessageID, msg.MessageID)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)
	requireT.Equal(types.ZeroServerID, r.leaderID)

	requireT.EqualValues(2, s.CurrentTerm())
}

func TestCandidateApplyVoteResponseGrantedInNextTerm(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(1))
	r, ts := newReactor(s)
	_, err := r.transitionToCandidate(peers)
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	notExpectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()

	msg, err := r.ApplyVoteResponse(peer1ID, p2p.VoteResponse{
		MessageID:   messageID,
		Term:        2,
		VoteGranted: true,
	}, peers)
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.EqualValues(2, r.votedForMe)
	requireT.Equal(p2p.ZeroMessageID, msg.MessageID)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)
	requireT.EqualValues(2, s.CurrentTerm())

	expectedElectionTime := ts.Add(time.Hour)

	_, err = r.transitionToCandidate(peers)
	requireT.NoError(err)
	requireT.EqualValues(3, s.CurrentTerm())

	ts.Add(time.Hour)

	messageID = p2p.NewMessageID()

	msg, err = r.ApplyVoteResponse(peer1ID, p2p.VoteResponse{
		MessageID:   messageID,
		Term:        3,
		VoteGranted: true,
	}, peers)
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.EqualValues(2, r.votedForMe)
	requireT.Empty(msg)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Equal(types.ZeroServerID, r.leaderID)

	requireT.EqualValues(3, s.CurrentTerm())
}

func TestCandidateApplyVoteResponseGrantedFromMajority(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(1))
	r, ts := newReactor(s)
	_, err := r.transitionToCandidate(peers)
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	notExpectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()

	msg, err := r.ApplyVoteResponse(peer1ID, p2p.VoteResponse{
		MessageID:   messageID,
		Term:        2,
		VoteGranted: true,
	}, peers)
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.EqualValues(2, r.votedForMe)
	requireT.Empty(msg)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)
	requireT.Equal(types.ZeroServerID, r.leaderID)
	requireT.EqualValues(2, s.CurrentTerm())

	expectedHeartbeatTime := ts.Add(time.Hour)

	messageID = p2p.NewMessageID()

	msg, err = r.ApplyVoteResponse(peer2ID, p2p.VoteResponse{
		MessageID:   messageID,
		Term:        2,
		VoteGranted: true,
	}, peers)
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.EqualValues(3, r.votedForMe)
	requireT.Equal(p2p.AppendEntriesRequest{
		MessageID:    msg.MessageID,
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
	}, msg)
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
	_, err := r.transitionToCandidate(peers)
	requireT.NoError(err)
	requireT.EqualValues(1, s.CurrentTerm())

	heartbeatTimeoutTime := ts.Add(time.Hour)
	notExpectedHeartbeatTime := ts.Add(time.Hour)

	msg, err := r.ApplyHeartbeatTimeout(heartbeatTimeoutTime, peers)
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.NotEqual(notExpectedHeartbeatTime, r.electionTime)
	requireT.Equal(p2p.ZeroMessageID, msg.MessageID)
}

func TestCandidateApplyPeerConnectedDoesNothing(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	r, _ := newReactor(s)
	_, err := r.transitionToCandidate(peers)
	requireT.NoError(err)
	requireT.Equal(types.ZeroServerID, r.leaderID)
	requireT.EqualValues(1, s.CurrentTerm())

	msg, err := r.ApplyPeerConnected(types.ServerID(uuid.New()))
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.Equal(p2p.ZeroMessageID, msg.MessageID)
}
