package reactor

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/raft/state"
	"github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/raft/wire/p2c"
	"github.com/outofforest/magma/raft/wire/p2p"
)

var (
	serverID = types.ServerID(uuid.New())
	peer1ID  = types.ServerID(uuid.New())
	peer2ID  = types.ServerID(uuid.New())
	peer3ID  = types.ServerID(uuid.New())
	peer4ID  = types.ServerID(uuid.New())

	peers = []types.ServerID{peer1ID, peer2ID, peer3ID, peer4ID}
)

func newReactor(s *state.State) (*Reactor, TimeAdvancer) {
	timeSource := &TestTimeSource{}
	return New(serverID, len(peers)/2+1, s, timeSource), timeSource
}

func TestFollowerInitialRole(t *testing.T) {
	requireT := require.New(t)
	r, ts := newReactor(&state.State{})
	expectedElectionTime := ts.Add(0)

	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(expectedElectionTime, r.electionTime)
}

func TestFollowerSetup(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	r, ts := newReactor(s)

	r.role = types.RoleCandidate
	r.votedForMe = 2
	r.nextIndex[peer1ID] = 100
	r.matchIndex[peer1ID] = 100

	r.lastLogTerm = 3
	r.nextLogIndex = 10
	r.committedCount = 5

	expectedElectionTime := ts.Add(time.Hour)
	r.transitionToFollower()

	requireT.Equal(types.RoleFollower, r.role)
	requireT.Zero(r.votedForMe)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Empty(r.nextIndex)
	requireT.Empty(r.matchIndex)

	requireT.EqualValues(3, r.lastLogTerm)
	requireT.EqualValues(10, r.nextLogIndex)
	requireT.EqualValues(5, r.committedCount)

	requireT.EqualValues(0, s.CurrentTerm())

	_, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.Nil(entries)
}

func TestFollowerAppendEntriesRequestAppendEntriesToEmptyLog(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	r, ts := newReactor(s)
	expectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	msg, err := r.ApplyAppendEntriesRequest(peer1ID, p2p.AppendEntriesRequest{
		MessageID:    messageID,
		Term:         1,
		NextLogIndex: 0,
		LastLogTerm:  0,
		Entries: []state.LogItem{
			{
				Term: 1,
				Data: []byte{0x01},
			},
		},
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(p2p.AppendEntriesResponse{
		MessageID:    messageID,
		Term:         1,
		NextLogIndex: 1,
	}, msg)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Equal(peer1ID, r.leaderID)

	requireT.EqualValues(1, s.CurrentTerm())
	_, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]state.LogItem{
		{
			Term: 1,
			Data: []byte{0x01},
		},
	}, entries)
}

func TestFollowerAppendEntriesRequestAppendEntriesToNonEmptyLog(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(1))
	_, _, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 1},
	})
	requireT.NoError(err)

	r, ts := newReactor(s)
	expectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	msg, err := r.ApplyAppendEntriesRequest(peer1ID, p2p.AppendEntriesRequest{
		MessageID:    messageID,
		Term:         1,
		NextLogIndex: 2,
		LastLogTerm:  1,
		Entries: []state.LogItem{
			{
				Term: 1,
				Data: []byte{0x01},
			},
			{
				Term: 1,
				Data: []byte{0x02},
			},
			{
				Term: 1,
				Data: []byte{0x03},
			},
		},
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(p2p.AppendEntriesResponse{
		MessageID:    messageID,
		Term:         1,
		NextLogIndex: 5,
	}, msg)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Equal(peer1ID, r.leaderID)

	requireT.EqualValues(1, s.CurrentTerm())
	_, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]state.LogItem{
		{Term: 1},
		{Term: 1},
		{
			Term: 1,
			Data: []byte{0x01},
		},
		{
			Term: 1,
			Data: []byte{0x02},
		},
		{
			Term: 1,
			Data: []byte{0x03},
		},
	}, entries)
}

func TestFollowerAppendEntriesRequestAppendEntriesToNonEmptyLogOnFutureTerm(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(3))
	_, _, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 2},
	})
	requireT.NoError(err)

	r, ts := newReactor(s)
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

func TestFollowerAppendEntriesRequestReplaceEntries(t *testing.T) {
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
	expectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	msg, err := r.ApplyAppendEntriesRequest(peer1ID, p2p.AppendEntriesRequest{
		MessageID:    messageID,
		Term:         4,
		NextLogIndex: 2,
		LastLogTerm:  2,
		Entries: []state.LogItem{
			{
				Term: 4,
				Data: []byte{0x01},
			},
		},
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(p2p.AppendEntriesResponse{
		MessageID:    messageID,
		Term:         4,
		NextLogIndex: 3,
	}, msg)
	requireT.Equal(expectedElectionTime, r.electionTime)

	requireT.EqualValues(4, s.CurrentTerm())
	_, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]state.LogItem{
		{Term: 1},
		{Term: 2},
		{
			Term: 4,
			Data: []byte{0x01},
		},
	}, entries)
}

func TestFollowerAppendEntriesRequestDiscardEntriesOnTermMismatch(t *testing.T) {
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
	notExpectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	msg, err := r.ApplyAppendEntriesRequest(peer1ID, p2p.AppendEntriesRequest{
		MessageID:    messageID,
		Term:         4,
		NextLogIndex: 3,
		LastLogTerm:  3,
		Entries: []state.LogItem{
			{
				Term: 4,
				Data: []byte{0x01},
			},
		},
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(p2p.AppendEntriesResponse{
		MessageID:    messageID,
		Term:         4,
		NextLogIndex: 1,
	}, msg)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)

	requireT.EqualValues(4, s.CurrentTerm())
	_, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]state.LogItem{
		{Term: 1},
	}, entries)
}

func TestFollowerAppendEntriesRequestDiscardEntriesOnTermMismatchTwice(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 2},
		{Term: 3},
		{Term: 3},
	})
	requireT.NoError(err)

	r, _ := newReactor(s)

	// First time.

	messageID := p2p.NewMessageID()
	msg, err := r.ApplyAppendEntriesRequest(peer1ID, p2p.AppendEntriesRequest{
		MessageID:    messageID,
		Term:         5,
		NextLogIndex: 5,
		LastLogTerm:  4,
		Entries: []state.LogItem{
			{
				Term: 5,
				Data: []byte{0x01},
			},
		},
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(p2p.AppendEntriesResponse{
		MessageID:    messageID,
		Term:         5,
		NextLogIndex: 3,
	}, msg)
	requireT.EqualValues(3, r.nextLogIndex)
	requireT.EqualValues(2, r.lastLogTerm)

	requireT.EqualValues(5, s.CurrentTerm())
	_, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 2},
	}, entries)

	// Second time.

	messageID = p2p.NewMessageID()
	msg, err = r.ApplyAppendEntriesRequest(peer1ID, p2p.AppendEntriesRequest{
		MessageID:    messageID,
		Term:         6,
		NextLogIndex: 3,
		LastLogTerm:  3,
		Entries: []state.LogItem{
			{
				Term: 6,
				Data: []byte{0x01},
			},
		},
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(p2p.AppendEntriesResponse{
		MessageID:    messageID,
		Term:         6,
		NextLogIndex: 1,
	}, msg)
	requireT.EqualValues(1, r.nextLogIndex)
	requireT.EqualValues(1, r.lastLogTerm)

	requireT.EqualValues(6, s.CurrentTerm())
	_, entries, err = s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]state.LogItem{
		{Term: 1},
	}, entries)
}

func TestFollowerAppendEntriesRequestRejectIfNoPreviousEntry(t *testing.T) {
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
	notExpectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	msg, err := r.ApplyAppendEntriesRequest(peer1ID, p2p.AppendEntriesRequest{
		MessageID:    messageID,
		Term:         4,
		NextLogIndex: 1000,
		LastLogTerm:  3,
		Entries: []state.LogItem{
			{
				Term: 4,
				Data: []byte{0x01},
			},
		},
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(p2p.AppendEntriesResponse{
		MessageID:    messageID,
		Term:         4,
		NextLogIndex: 3,
	}, msg)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)
	requireT.Equal(peer1ID, r.leaderID)

	requireT.EqualValues(4, s.CurrentTerm())
	_, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 2},
	}, entries)
}

func TestFollowerAppendEntriesRequestUpdateCurrentTermOnHeartbeat(t *testing.T) {
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
	expectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	msg, err := r.ApplyAppendEntriesRequest(peer1ID, p2p.AppendEntriesRequest{
		MessageID:    messageID,
		Term:         4,
		NextLogIndex: 3,
		LastLogTerm:  2,
		Entries:      nil,
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(p2p.AppendEntriesResponse{
		MessageID:    messageID,
		Term:         4,
		NextLogIndex: 3,
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
	}, entries)
}

func TestFollowerAppendEntriesRequestDoNothingOnHeartbeat(t *testing.T) {
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
	expectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	msg, err := r.ApplyAppendEntriesRequest(peer1ID, p2p.AppendEntriesRequest{
		MessageID:    messageID,
		Term:         2,
		NextLogIndex: 3,
		LastLogTerm:  2,
		Entries:      nil,
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(p2p.AppendEntriesResponse{
		MessageID:    messageID,
		Term:         2,
		NextLogIndex: 3,
	}, msg)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Equal(peer1ID, r.leaderID)

	requireT.EqualValues(2, s.CurrentTerm())
	_, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 2},
	}, entries)
}

func TestFollowerAppendEntriesRequestDoNothingOnLowerTerm(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(4))
	_, _, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 2},
	})
	requireT.NoError(err)

	r, ts := newReactor(s)
	notExpectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	msg, err := r.ApplyAppendEntriesRequest(peer2ID, p2p.AppendEntriesRequest{
		MessageID:    messageID,
		Term:         3,
		NextLogIndex: 3,
		LastLogTerm:  2,
		Entries: []state.LogItem{
			{
				Term: 3,
				Data: []byte{0x01},
			},
		},
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(p2p.AppendEntriesResponse{
		MessageID:    messageID,
		Term:         4,
		NextLogIndex: 3,
	}, msg)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)
	requireT.Equal(types.ZeroServerID, r.leaderID)

	requireT.EqualValues(4, s.CurrentTerm())
	_, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 2},
	}, entries)
}

func TestFollowerAppendEntriesRequestSetCommitedCountToLeaderCommit(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(1))
	_, _, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 1},
		{Term: 1},
	})
	requireT.NoError(err)

	r, _ := newReactor(s)
	r.committedCount = 1

	messageID := p2p.NewMessageID()
	msg, err := r.ApplyAppendEntriesRequest(peer1ID, p2p.AppendEntriesRequest{
		MessageID:    messageID,
		Term:         1,
		NextLogIndex: 3,
		LastLogTerm:  1,
		Entries: []state.LogItem{
			{
				Term: 1,
				Data: []byte{0x01},
			},
		},
		LeaderCommit: 3,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(p2p.AppendEntriesResponse{
		MessageID:    messageID,
		Term:         1,
		NextLogIndex: 4,
	}, msg)

	requireT.EqualValues(1, s.CurrentTerm())
	_, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]state.LogItem{
		{Term: 1},
		{Term: 1},
		{Term: 1},
		{
			Term: 1,
			Data: []byte{0x01},
		},
	}, entries)

	requireT.EqualValues(3, r.committedCount)
}

func TestFollowerAppendEntriesRequestSetCommitedCountToLeaderCommitOnHeartbeat(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(1))
	_, _, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 1},
		{Term: 1},
	})
	requireT.NoError(err)

	r, _ := newReactor(s)
	r.committedCount = 1

	messageID := p2p.NewMessageID()
	msg, err := r.ApplyAppendEntriesRequest(peer1ID, p2p.AppendEntriesRequest{
		MessageID:    messageID,
		Term:         1,
		NextLogIndex: 3,
		LastLogTerm:  1,
		Entries:      nil,
		LeaderCommit: 2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(p2p.AppendEntriesResponse{
		MessageID:    messageID,
		Term:         1,
		NextLogIndex: 3,
	}, msg)

	requireT.EqualValues(1, s.CurrentTerm())
	_, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]state.LogItem{
		{Term: 1},
		{Term: 1},
		{Term: 1},
	}, entries)

	requireT.EqualValues(2, r.committedCount)
}

func TestFollowerAppendEntriesRequestSetCommitedCountToLogLength(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(1))
	_, _, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 1},
		{Term: 1},
	})
	requireT.NoError(err)

	r, _ := newReactor(s)
	r.committedCount = 1

	messageID := p2p.NewMessageID()
	msg, err := r.ApplyAppendEntriesRequest(peer1ID, p2p.AppendEntriesRequest{
		MessageID:    messageID,
		Term:         1,
		NextLogIndex: 3,
		LastLogTerm:  1,
		Entries: []state.LogItem{
			{
				Term: 1,
				Data: []byte{0x01},
			},
		},
		LeaderCommit: 100,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(p2p.AppendEntriesResponse{
		MessageID:    messageID,
		Term:         1,
		NextLogIndex: 4,
	}, msg)

	requireT.EqualValues(1, s.CurrentTerm())
	_, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]state.LogItem{
		{Term: 1},
		{Term: 1},
		{Term: 1},
		{
			Term: 1,
			Data: []byte{0x01},
		},
	}, entries)

	requireT.EqualValues(4, r.committedCount)
}

func TestFollowerAppendEntriesRequestSetCommitedCountToLogLengthOnHeartbeat(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(1))
	_, _, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 1},
		{Term: 1},
	})
	requireT.NoError(err)

	r, _ := newReactor(s)
	r.committedCount = 1

	messageID := p2p.NewMessageID()
	msg, err := r.ApplyAppendEntriesRequest(peer1ID, p2p.AppendEntriesRequest{
		MessageID:    messageID,
		Term:         1,
		NextLogIndex: 3,
		LastLogTerm:  1,
		Entries:      nil,
		LeaderCommit: 100,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(p2p.AppendEntriesResponse{
		MessageID:    messageID,
		Term:         1,
		NextLogIndex: 3,
	}, msg)

	requireT.EqualValues(1, s.CurrentTerm())
	_, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]state.LogItem{
		{Term: 1},
		{Term: 1},
		{Term: 1},
	}, entries)

	requireT.EqualValues(3, r.committedCount)
}

func TestFollowerApplyVoteRequestGrantedOnEmptyLog(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(1))
	r, ts := newReactor(s)
	expectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	msg, err := r.ApplyVoteRequest(peer1ID, p2p.VoteRequest{
		MessageID:    messageID,
		Term:         1,
		NextLogIndex: 0,
		LastLogTerm:  0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(p2p.VoteResponse{
		MessageID:   messageID,
		Term:        1,
		VoteGranted: true,
	}, msg)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Equal(types.ZeroServerID, r.leaderID)

	requireT.EqualValues(1, s.CurrentTerm())

	granted, err := s.VoteFor(peer2ID)
	requireT.NoError(err)
	requireT.False(granted)

	granted, err = s.VoteFor(peer1ID)
	requireT.NoError(err)
	requireT.True(granted)
}

func TestFollowerApplyVoteRequestGrantedOnEqualLog(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 1},
		{Term: 2},
	})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(2))
	r, ts := newReactor(s)
	expectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	msg, err := r.ApplyVoteRequest(peer1ID, p2p.VoteRequest{
		MessageID:    messageID,
		Term:         2,
		NextLogIndex: 3,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(p2p.VoteResponse{
		MessageID:   messageID,
		Term:        2,
		VoteGranted: true,
	}, msg)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Equal(types.ZeroServerID, r.leaderID)

	requireT.EqualValues(2, s.CurrentTerm())

	granted, err := s.VoteFor(peer2ID)
	requireT.NoError(err)
	requireT.False(granted)

	granted, err = s.VoteFor(peer1ID)
	requireT.NoError(err)
	requireT.True(granted)
}

func TestFollowerApplyVoteRequestGrantedOnLongerLog(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 1},
		{Term: 2},
	})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(2))
	r, ts := newReactor(s)
	expectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	msg, err := r.ApplyVoteRequest(peer1ID, p2p.VoteRequest{
		MessageID:    messageID,
		Term:         2,
		NextLogIndex: 4,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(p2p.VoteResponse{
		MessageID:   messageID,
		Term:        2,
		VoteGranted: true,
	}, msg)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Equal(types.ZeroServerID, r.leaderID)

	requireT.EqualValues(2, s.CurrentTerm())

	granted, err := s.VoteFor(peer2ID)
	requireT.NoError(err)
	requireT.False(granted)

	granted, err = s.VoteFor(peer1ID)
	requireT.NoError(err)
	requireT.True(granted)
}

func TestFollowerApplyVoteRequestGrantedOnFutureTerm(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(2))
	r, ts := newReactor(s)
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

func TestFollowerApplyVoteRequestGrantedTwice(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 1},
		{Term: 2},
	})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(2))
	r, ts := newReactor(s)
	expectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	msg, err := r.ApplyVoteRequest(peer1ID, p2p.VoteRequest{
		MessageID:    messageID,
		Term:         2,
		NextLogIndex: 3,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(p2p.VoteResponse{
		MessageID:   messageID,
		Term:        2,
		VoteGranted: true,
	}, msg)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.EqualValues(2, s.CurrentTerm())

	expectedElectionTime = ts.Add(time.Hour)

	messageID = p2p.NewMessageID()
	msg, err = r.ApplyVoteRequest(peer1ID, p2p.VoteRequest{
		MessageID:    messageID,
		Term:         2,
		NextLogIndex: 3,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(p2p.VoteResponse{
		MessageID:   messageID,
		Term:        2,
		VoteGranted: true,
	}, msg)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Equal(types.ZeroServerID, r.leaderID)
	requireT.EqualValues(2, s.CurrentTerm())
}

func TestFollowerApplyVoteRequestGrantVoteToOtherCandidateInNextTerm(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 1},
		{Term: 2},
	})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(2))
	r, ts := newReactor(s)
	expectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	msg, err := r.ApplyVoteRequest(peer1ID, p2p.VoteRequest{
		MessageID:    messageID,
		Term:         2,
		NextLogIndex: 3,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(p2p.VoteResponse{
		MessageID:   messageID,
		Term:        2,
		VoteGranted: true,
	}, msg)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.EqualValues(2, s.CurrentTerm())

	expectedElectionTime = ts.Add(time.Hour)

	messageID = p2p.NewMessageID()
	msg, err = r.ApplyVoteRequest(peer2ID, p2p.VoteRequest{
		MessageID:    messageID,
		Term:         3,
		NextLogIndex: 3,
		LastLogTerm:  2,
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
}

func TestFollowerApplyVoteRequestRejectedOnPastTerm(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(2))
	r, ts := newReactor(s)
	notExpectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	msg, err := r.ApplyVoteRequest(peer1ID, p2p.VoteRequest{
		MessageID:    messageID,
		Term:         1,
		NextLogIndex: 0,
		LastLogTerm:  0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(p2p.VoteResponse{
		MessageID:   messageID,
		Term:        2,
		VoteGranted: false,
	}, msg)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)
	requireT.Equal(types.ZeroServerID, r.leaderID)

	requireT.EqualValues(2, s.CurrentTerm())

	granted, err := s.VoteFor(peer2ID)
	requireT.NoError(err)
	requireT.True(granted)
}

func TestFollowerApplyVoteRequestRejectedOnLowerLastLogTerm(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 1},
		{Term: 2},
	})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(2))
	r, ts := newReactor(s)
	notExpectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	msg, err := r.ApplyVoteRequest(peer1ID, p2p.VoteRequest{
		MessageID:    messageID,
		Term:         3,
		NextLogIndex: 3,
		LastLogTerm:  1,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(p2p.VoteResponse{
		MessageID:   messageID,
		Term:        3,
		VoteGranted: false,
	}, msg)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)
	requireT.Equal(types.ZeroServerID, r.leaderID)

	requireT.EqualValues(3, s.CurrentTerm())

	granted, err := s.VoteFor(peer2ID)
	requireT.NoError(err)
	requireT.True(granted)
}

func TestFollowerApplyVoteRequestRejectedOnShorterLog(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 1},
		{Term: 2},
		{Term: 2},
	})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(2))
	r, ts := newReactor(s)
	notExpectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	msg, err := r.ApplyVoteRequest(peer1ID, p2p.VoteRequest{
		MessageID:    messageID,
		Term:         2,
		NextLogIndex: 3,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(p2p.VoteResponse{
		MessageID:   messageID,
		Term:        2,
		VoteGranted: false,
	}, msg)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)
	requireT.Equal(types.ZeroServerID, r.leaderID)

	requireT.EqualValues(2, s.CurrentTerm())

	granted, err := s.VoteFor(peer2ID)
	requireT.NoError(err)
	requireT.True(granted)
}

func TestFollowerApplyVoteRequestRejectOtherCandidates(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 1},
		{Term: 2},
	})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(2))
	r, ts := newReactor(s)
	expectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	msg, err := r.ApplyVoteRequest(peer1ID, p2p.VoteRequest{
		MessageID:    messageID,
		Term:         2,
		NextLogIndex: 3,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(p2p.VoteResponse{
		MessageID:   messageID,
		Term:        2,
		VoteGranted: true,
	}, msg)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.EqualValues(2, s.CurrentTerm())

	ts.Add(time.Hour)

	messageID = p2p.NewMessageID()
	msg, err = r.ApplyVoteRequest(peer2ID, p2p.VoteRequest{
		MessageID:    messageID,
		Term:         2,
		NextLogIndex: 3,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(p2p.VoteResponse{
		MessageID:   messageID,
		Term:        2,
		VoteGranted: false,
	}, msg)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Equal(types.ZeroServerID, r.leaderID)
	requireT.EqualValues(2, s.CurrentTerm())
}

func TestFollowerApplyElectionTimeoutAfterElectionTime(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	r, ts := newReactor(s)

	electionTimeoutTime := ts.Add(time.Hour)
	expectedElectionTime := ts.Add(time.Hour)

	msg, err := r.ApplyElectionTimeout(electionTimeoutTime)
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.EqualValues(1, s.CurrentTerm())
	requireT.EqualValues(1, r.votedForMe)
	requireT.Equal(p2p.VoteRequest{
		MessageID:    msg.MessageID,
		Term:         1,
		NextLogIndex: 0,
		LastLogTerm:  0,
	}, msg)

	granted, err := s.VoteFor(peer1ID)
	requireT.NoError(err)
	requireT.False(granted)

	granted, err = s.VoteFor(serverID)
	requireT.NoError(err)
	requireT.True(granted)
}

func TestFollowerApplyElectionTimeoutBeforeElectionTime(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	r, ts := newReactor(s)

	electionTimeoutTime := ts.Add(time.Hour)
	r.electionTime = ts.Add(time.Hour)
	notExpectedElectionTime := ts.Add(time.Hour)

	msg, err := r.ApplyElectionTimeout(electionTimeoutTime)
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)
	requireT.EqualValues(0, s.CurrentTerm())
	requireT.EqualValues(0, r.votedForMe)
	requireT.Equal(p2p.ZeroMessageID, msg.MessageID)

	granted, err := s.VoteFor(peer1ID)
	requireT.NoError(err)
	requireT.True(granted)
}

func TestFollowerApplyHeartbeatTimeoutDoesNothing(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	r, ts := newReactor(s)

	heartbeatTimeoutTime := ts.Add(time.Hour)
	notExpectedHeartbeatTime := ts.Add(time.Hour)

	msg, err := r.ApplyHeartbeatTimeout(heartbeatTimeoutTime)
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.NotEqual(notExpectedHeartbeatTime, r.electionTime)
	requireT.Equal(p2p.ZeroMessageID, msg.MessageID)
}

func TestFollowerApplyPeerConnectedDoesNothing(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	r, _ := newReactor(s)

	msg, err := r.ApplyPeerConnected(types.ServerID(uuid.New()))
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(p2p.ZeroMessageID, msg.MessageID)
}

func TestFollowerApplyClientRequestIgnoreIfNotLeader(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(1))
	r, ts := newReactor(s)

	notExpectedHeartbeatTime := ts.Add(time.Hour)

	msg, err := r.ApplyClientRequest(p2c.ClientRequest{
		Data: []byte{0x01},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(p2p.ZeroMessageID, msg.MessageID)
	requireT.NotEqual(notExpectedHeartbeatTime, r.heartBeatTime)
	requireT.Zero(r.nextLogIndex)
	requireT.Empty(r.nextLogIndex)
	requireT.Empty(r.matchIndex)
	requireT.Zero(r.committedCount)
}
