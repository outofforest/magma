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

var (
	serverID = types.ServerID(uuid.New())
	peer1ID  = types.ServerID(uuid.New())
	peer2ID  = types.ServerID(uuid.New())
	peer3ID  = types.ServerID(uuid.New())
	peer4ID  = types.ServerID(uuid.New())
)

func newReactor(s *state.State) (*Reactor, TimeAdvancer) {
	timeSource := &TestTimeSource{}
	return New(serverID, []types.ServerID{peer1ID, peer2ID, peer3ID, peer4ID}, s, timeSource), timeSource
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
	r, ts := newReactor(&state.State{})

	r.role = types.RoleCandidate
	r.votedForMe = 2
	r.nextIndex[peer1ID] = 100
	r.matchIndex[peer1ID] = 100
	r.callInProgress[peer1ID] = p2p.NewMessageID()

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
	requireT.Empty(r.callInProgress)

	requireT.EqualValues(3, r.lastLogTerm)
	requireT.EqualValues(10, r.nextLogIndex)
	requireT.EqualValues(5, r.committedCount)
}

func TestFollowerAppendEntriesRequestAppendEntriesToEmptyLog(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	r, ts := newReactor(s)
	expectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.AppendEntriesRequest{
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
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Equal([]p2p.Message{
		{
			PeerID: peer1ID,
			Msg: p2p.AppendEntriesResponse{
				MessageID:    messageID,
				Term:         1,
				NextLogIndex: 1,
				Success:      true,
			},
		},
	}, messages)
	requireT.Equal(expectedElectionTime, r.electionTime)

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
	_, _, success, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 1},
	})
	requireT.NoError(err)
	requireT.True(success)

	r, ts := newReactor(s)
	expectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.AppendEntriesRequest{
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
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Equal([]p2p.Message{
		{
			PeerID: peer1ID,
			Msg: p2p.AppendEntriesResponse{
				MessageID:    messageID,
				Term:         1,
				NextLogIndex: 5,
				Success:      true,
			},
		},
	}, messages)
	requireT.Equal(expectedElectionTime, r.electionTime)

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

func TestFollowerAppendEntriesRequestAppendEntriesToNonEmptyLogOnDifferentTerm(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(3))
	_, _, success, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 2},
	})
	requireT.NoError(err)
	requireT.True(success)

	r, ts := newReactor(s)
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

func TestFollowerAppendEntriesRequestReplaceEntries(t *testing.T) {
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
	expectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.AppendEntriesRequest{
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
				NextLogIndex: 3,
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
	_, _, success, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 2},
	})
	requireT.NoError(err)
	requireT.True(success)

	r, ts := newReactor(s)
	notExpectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.AppendEntriesRequest{
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
				NextLogIndex: 1,
				Success:      false,
			},
		},
	}, messages)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)

	requireT.EqualValues(4, s.CurrentTerm())
	_, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]state.LogItem{
		{Term: 1},
	}, entries)
}

func TestFollowerAppendEntriesRequestRejectIfNoPreviousEntry(t *testing.T) {
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
	notExpectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.AppendEntriesRequest{
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
				NextLogIndex: 3,
				Success:      false,
			},
		},
	}, messages)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)

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
	_, _, success, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 2},
	})
	requireT.NoError(err)
	requireT.True(success)

	r, ts := newReactor(s)
	expectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.AppendEntriesRequest{
			MessageID:    messageID,
			Term:         4,
			NextLogIndex: 3,
			LastLogTerm:  2,
			Entries:      nil,
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
				NextLogIndex: 3,
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
	}, entries)
}

func TestFollowerAppendEntriesRequestDoNothingOnHeartbeat(t *testing.T) {
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
	expectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.AppendEntriesRequest{
			MessageID:    messageID,
			Term:         2,
			NextLogIndex: 3,
			LastLogTerm:  2,
			Entries:      nil,
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
				Term:         2,
				NextLogIndex: 3,
				Success:      true,
			},
		},
	}, messages)
	requireT.Equal(expectedElectionTime, r.electionTime)

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
	_, _, success, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 2},
		{Term: 2},
	})
	requireT.NoError(err)
	requireT.True(success)

	r, ts := newReactor(s)
	notExpectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer2ID,
		Msg: p2p.AppendEntriesRequest{
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
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Equal([]p2p.Message{
		{
			PeerID: peer2ID,
			Msg: p2p.AppendEntriesResponse{
				MessageID:    messageID,
				Term:         4,
				NextLogIndex: 3,
				Success:      false,
			},
		},
	}, messages)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)

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
	_, _, success, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 1},
		{Term: 1},
	})
	requireT.NoError(err)
	requireT.True(success)

	r, _ := newReactor(s)
	r.committedCount = 1

	messageID := p2p.NewMessageID()
	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.AppendEntriesRequest{
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
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Equal([]p2p.Message{
		{
			PeerID: peer1ID,
			Msg: p2p.AppendEntriesResponse{
				MessageID:    messageID,
				Term:         1,
				NextLogIndex: 4,
				Success:      true,
			},
		},
	}, messages)

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
	_, _, success, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 1},
		{Term: 1},
	})
	requireT.NoError(err)
	requireT.True(success)

	r, _ := newReactor(s)
	r.committedCount = 1

	messageID := p2p.NewMessageID()
	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.AppendEntriesRequest{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 3,
			LastLogTerm:  1,
			Entries:      nil,
			LeaderCommit: 2,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Equal([]p2p.Message{
		{
			PeerID: peer1ID,
			Msg: p2p.AppendEntriesResponse{
				MessageID:    messageID,
				Term:         1,
				NextLogIndex: 3,
				Success:      true,
			},
		},
	}, messages)

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
	_, _, success, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 1},
		{Term: 1},
	})
	requireT.NoError(err)
	requireT.True(success)

	r, _ := newReactor(s)
	r.committedCount = 1

	messageID := p2p.NewMessageID()
	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.AppendEntriesRequest{
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
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Equal([]p2p.Message{
		{
			PeerID: peer1ID,
			Msg: p2p.AppendEntriesResponse{
				MessageID:    messageID,
				Term:         1,
				NextLogIndex: 4,
				Success:      true,
			},
		},
	}, messages)

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
	_, _, success, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 1},
		{Term: 1},
	})
	requireT.NoError(err)
	requireT.True(success)

	r, _ := newReactor(s)
	r.committedCount = 1

	messageID := p2p.NewMessageID()
	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.AppendEntriesRequest{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 3,
			LastLogTerm:  1,
			Entries:      nil,
			LeaderCommit: 100,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Equal([]p2p.Message{
		{
			PeerID: peer1ID,
			Msg: p2p.AppendEntriesResponse{
				MessageID:    messageID,
				Term:         1,
				NextLogIndex: 3,
				Success:      true,
			},
		},
	}, messages)

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
	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.VoteRequest{
			MessageID:    messageID,
			Term:         1,
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
				Term:        1,
				VoteGranted: true,
			},
		},
	}, messages)
	requireT.Equal(expectedElectionTime, r.electionTime)

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
	_, _, success, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 1},
		{Term: 2},
	})
	requireT.NoError(err)
	requireT.True(success)
	requireT.NoError(s.SetCurrentTerm(2))
	r, ts := newReactor(s)
	expectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.VoteRequest{
			MessageID:    messageID,
			Term:         2,
			NextLogIndex: 3,
			LastLogTerm:  2,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Equal([]p2p.Message{
		{
			PeerID: peer1ID,
			Msg: p2p.VoteResponse{
				MessageID:   messageID,
				Term:        2,
				VoteGranted: true,
			},
		},
	}, messages)
	requireT.Equal(expectedElectionTime, r.electionTime)

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
	_, _, success, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 1},
		{Term: 2},
	})
	requireT.NoError(err)
	requireT.True(success)
	requireT.NoError(s.SetCurrentTerm(2))
	r, ts := newReactor(s)
	expectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.VoteRequest{
			MessageID:    messageID,
			Term:         2,
			NextLogIndex: 4,
			LastLogTerm:  2,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Equal([]p2p.Message{
		{
			PeerID: peer1ID,
			Msg: p2p.VoteResponse{
				MessageID:   messageID,
				Term:        2,
				VoteGranted: true,
			},
		},
	}, messages)
	requireT.Equal(expectedElectionTime, r.electionTime)

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

func TestFollowerApplyVoteRequestGrantedTwice(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, success, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 1},
		{Term: 2},
	})
	requireT.NoError(err)
	requireT.True(success)
	requireT.NoError(s.SetCurrentTerm(2))
	r, ts := newReactor(s)
	expectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.VoteRequest{
			MessageID:    messageID,
			Term:         2,
			NextLogIndex: 3,
			LastLogTerm:  2,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Equal([]p2p.Message{
		{
			PeerID: peer1ID,
			Msg: p2p.VoteResponse{
				MessageID:   messageID,
				Term:        2,
				VoteGranted: true,
			},
		},
	}, messages)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.EqualValues(2, s.CurrentTerm())

	expectedElectionTime = ts.Add(time.Hour)

	messageID = p2p.NewMessageID()
	role, messages, err = r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.VoteRequest{
			MessageID:    messageID,
			Term:         2,
			NextLogIndex: 3,
			LastLogTerm:  2,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Equal([]p2p.Message{
		{
			PeerID: peer1ID,
			Msg: p2p.VoteResponse{
				MessageID:   messageID,
				Term:        2,
				VoteGranted: true,
			},
		},
	}, messages)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.EqualValues(2, s.CurrentTerm())
}

func TestFollowerApplyVoteRequestGrantVoteToOtherCandidateInNextTerm(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, success, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 1},
		{Term: 2},
	})
	requireT.NoError(err)
	requireT.True(success)
	requireT.NoError(s.SetCurrentTerm(2))
	r, ts := newReactor(s)
	expectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.VoteRequest{
			MessageID:    messageID,
			Term:         2,
			NextLogIndex: 3,
			LastLogTerm:  2,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Equal([]p2p.Message{
		{
			PeerID: peer1ID,
			Msg: p2p.VoteResponse{
				MessageID:   messageID,
				Term:        2,
				VoteGranted: true,
			},
		},
	}, messages)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.EqualValues(2, s.CurrentTerm())

	expectedElectionTime = ts.Add(time.Hour)

	messageID = p2p.NewMessageID()
	role, messages, err = r.Apply(p2p.Message{
		PeerID: peer2ID,
		Msg: p2p.VoteRequest{
			MessageID:    messageID,
			Term:         3,
			NextLogIndex: 3,
			LastLogTerm:  2,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Equal([]p2p.Message{
		{
			PeerID: peer2ID,
			Msg: p2p.VoteResponse{
				MessageID:   messageID,
				Term:        3,
				VoteGranted: true,
			},
		},
	}, messages)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.EqualValues(3, s.CurrentTerm())
}

func TestFollowerApplyVoteRequestRejectedOnPastTerm(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(2))
	r, ts := newReactor(s)
	notExpectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.VoteRequest{
			MessageID:    messageID,
			Term:         1,
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
				Term:        2,
				VoteGranted: false,
			},
		},
	}, messages)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)

	requireT.EqualValues(2, s.CurrentTerm())

	granted, err := s.VoteFor(peer2ID)
	requireT.NoError(err)
	requireT.True(granted)
}

func TestFollowerApplyVoteRequestRejectedOnLowerLastLogTerm(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, success, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 1},
		{Term: 2},
	})
	requireT.NoError(err)
	requireT.True(success)
	requireT.NoError(s.SetCurrentTerm(2))
	r, ts := newReactor(s)
	notExpectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.VoteRequest{
			MessageID:    messageID,
			Term:         3,
			NextLogIndex: 3,
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
				VoteGranted: false,
			},
		},
	}, messages)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)

	requireT.EqualValues(3, s.CurrentTerm())

	granted, err := s.VoteFor(peer2ID)
	requireT.NoError(err)
	requireT.True(granted)
}

func TestFollowerApplyVoteRequestRejectedOnShorterLog(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, success, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 1},
		{Term: 2},
		{Term: 2},
	})
	requireT.NoError(err)
	requireT.True(success)
	requireT.NoError(s.SetCurrentTerm(2))
	r, ts := newReactor(s)
	notExpectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.VoteRequest{
			MessageID:    messageID,
			Term:         2,
			NextLogIndex: 3,
			LastLogTerm:  2,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Equal([]p2p.Message{
		{
			PeerID: peer1ID,
			Msg: p2p.VoteResponse{
				MessageID:   messageID,
				Term:        2,
				VoteGranted: false,
			},
		},
	}, messages)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)

	requireT.EqualValues(2, s.CurrentTerm())

	granted, err := s.VoteFor(peer2ID)
	requireT.NoError(err)
	requireT.True(granted)
}

func TestFollowerApplyVoteRequestRejectOtherCandidates(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	_, _, success, err := s.Append(0, 0, []state.LogItem{
		{Term: 1},
		{Term: 1},
		{Term: 2},
	})
	requireT.NoError(err)
	requireT.True(success)
	requireT.NoError(s.SetCurrentTerm(2))
	r, ts := newReactor(s)
	expectedElectionTime := ts.Add(time.Hour)

	messageID := p2p.NewMessageID()
	role, messages, err := r.Apply(p2p.Message{
		PeerID: peer1ID,
		Msg: p2p.VoteRequest{
			MessageID:    messageID,
			Term:         2,
			NextLogIndex: 3,
			LastLogTerm:  2,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Equal([]p2p.Message{
		{
			PeerID: peer1ID,
			Msg: p2p.VoteResponse{
				MessageID:   messageID,
				Term:        2,
				VoteGranted: true,
			},
		},
	}, messages)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.EqualValues(2, s.CurrentTerm())

	ts.Add(time.Hour)

	messageID = p2p.NewMessageID()
	role, messages, err = r.Apply(p2p.Message{
		PeerID: peer2ID,
		Msg: p2p.VoteRequest{
			MessageID:    messageID,
			Term:         2,
			NextLogIndex: 3,
			LastLogTerm:  2,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Equal([]p2p.Message{
		{
			PeerID: peer2ID,
			Msg: p2p.VoteResponse{
				MessageID:   messageID,
				Term:        2,
				VoteGranted: false,
			},
		},
	}, messages)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.EqualValues(2, s.CurrentTerm())
}

func TestFollowerApplyElectionTimeoutAfterElectionTime(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	r, ts := newReactor(s)

	electionTimeoutTime := ts.Add(time.Hour)
	expectedElectionTime := ts.Add(time.Hour)

	role, messages, err := r.Apply(p2p.Message{
		Msg: types.ElectionTimeout{
			Time: electionTimeoutTime,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, role)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.EqualValues(1, s.CurrentTerm())
	requireT.EqualValues(1, r.votedForMe)

	requireT.NotEmpty(messages)
	messageID := messages[0].Msg.(p2p.VoteRequest).MessageID
	requireT.Equal([]p2p.Message{
		{
			PeerID: peer1ID,
			Msg: p2p.VoteRequest{
				MessageID:    messageID,
				Term:         1,
				NextLogIndex: 0,
				LastLogTerm:  0,
			},
		},
		{
			PeerID: peer2ID,
			Msg: p2p.VoteRequest{
				MessageID:    messageID,
				Term:         1,
				NextLogIndex: 0,
				LastLogTerm:  0,
			},
		},
		{
			PeerID: peer3ID,
			Msg: p2p.VoteRequest{
				MessageID:    messageID,
				Term:         1,
				NextLogIndex: 0,
				LastLogTerm:  0,
			},
		},
		{
			PeerID: peer4ID,
			Msg: p2p.VoteRequest{
				MessageID:    messageID,
				Term:         1,
				NextLogIndex: 0,
				LastLogTerm:  0,
			},
		},
	}, messages)

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

	role, messages, err := r.Apply(p2p.Message{
		Msg: types.ElectionTimeout{
			Time: electionTimeoutTime,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)
	requireT.EqualValues(0, s.CurrentTerm())
	requireT.EqualValues(0, r.votedForMe)

	requireT.Empty(messages)

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

	role, messages, err := r.Apply(p2p.Message{
		Msg: types.HeartbeatTimeout{
			Time: heartbeatTimeoutTime,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.NotEqual(notExpectedHeartbeatTime, r.electionTime)
	requireT.Empty(messages)
}

func TestFollowerApplyPeerConnectedDoesNothing(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	r, _ := newReactor(s)

	role, messages, err := r.Apply(p2p.Message{
		Msg: types.ServerID(uuid.New()),
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Empty(messages)
}
