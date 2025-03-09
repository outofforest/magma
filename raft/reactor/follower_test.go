package reactor

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/raft/state"
	"github.com/outofforest/magma/raft/types"
	magmatypes "github.com/outofforest/magma/types"
)

const maxReadLogSize = 25

var (
	serverID = magmatypes.ServerID(uuid.New())
	peer1ID  = magmatypes.ServerID(uuid.New())
	peer2ID  = magmatypes.ServerID(uuid.New())
	peer3ID  = magmatypes.ServerID(uuid.New())
	peer4ID  = magmatypes.ServerID(uuid.New())

	config = magmatypes.Config{
		ServerID: serverID,
		Servers: []magmatypes.PeerConfig{
			{ID: serverID},
			{ID: peer1ID},
			{ID: peer2ID},
			{ID: peer3ID},
			{ID: peer4ID},
		},
		MaxLogSizePerMessage: maxReadLogSize,
		MaxLogSizeOnWire:     2 * maxReadLogSize,
	}
)

func newState() *state.State {
	return state.NewInMemory(1024 * 1024)
}

func newReactor(s *state.State) *Reactor {
	return New(config, s)
}

func TestFollowerInitialRole(t *testing.T) {
	requireT := require.New(t)
	r := newReactor(newState())

	requireT.Equal(types.RoleFollower, r.role)
	requireT.EqualValues(0, r.electionTick)
	requireT.EqualValues(0, r.ignoreElectionTick)
}

func TestFollowerSetup(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	r := newReactor(s)

	r.role = types.RoleCandidate
	r.leaderID = serverID
	r.votedForMe = 2
	r.electionTick = 1
	r.ignoreElectionTick = 0
	r.sync[peer1ID] = &syncProgress{
		NextIndex: 100,
		End:       100,
	}
	r.matchIndex[peer1ID] = 100

	r.lastLogTerm = 3
	r.nextLogIndex = 10
	r.commitInfo = types.CommitInfo{CommittedCount: 5}

	r.transitionToFollower()

	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)
	requireT.Zero(r.votedForMe)
	requireT.EqualValues(1, r.electionTick)
	requireT.EqualValues(2, r.ignoreElectionTick)
	requireT.Empty(r.sync)
	requireT.Empty(r.matchIndex)

	requireT.EqualValues(3, r.lastLogTerm)
	requireT.EqualValues(10, r.nextLogIndex)
	requireT.Equal(types.CommitInfo{CommittedCount: 5}, r.commitInfo)

	requireT.EqualValues(0, s.CurrentTerm())

	_, _, entries, err := s.Entries(0, maxReadLogSize)
	requireT.NoError(err)
	requireT.Nil(entries)
}

func TestFollowerAppendEntriesRequestAppendEntriesToEmptyLog(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         1,
		NextLogIndex: 0,
		NextLogTerm:  1,
		LastLogTerm:  0,
		Data:         []byte{0x01, 0x01, 0x02, 0x01, 0x00},
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(peer1ID, r.leaderID)

	requireT.EqualValues(1, s.CurrentTerm())
	_, _, entries, err := s.Entries(0, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x01, 0x01, 0x02, 0x01, 0x00}, entries)
}

func TestFollowerAppendEntriesRequestAppendEntriesToNonEmptyLog(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(1))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x03, 0x02, 0x00, 0x00,
	})
	requireT.NoError(err)

	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         1,
		NextLogIndex: 6,
		NextLogTerm:  1,
		LastLogTerm:  1,
		Data:         []byte{0x04, 0x03, 0x01, 0x02, 0x03},
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(peer1ID, r.leaderID)

	requireT.EqualValues(1, s.CurrentTerm())
	_, _, entries, err := s.Entries(0, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{
		0x01, 0x01, 0x03, 0x02, 0x00, 0x00, 0x04, 0x03, 0x01, 0x02, 0x03,
	}, entries)
}

func TestFollowerAppendEntriesRequestAppendEntriesToNonEmptyLogOnFutureTerm(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(3))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	})
	requireT.NoError(err)

	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         3,
		NextLogIndex: 11,
		NextLogTerm:  3,
		LastLogTerm:  2,
		Data:         []byte{0x01, 0x03, 0x03, 0x02, 0x01, 0x02},
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(peer1ID, r.leaderID)

	requireT.EqualValues(3, s.CurrentTerm())
	_, _, entries, err := s.Entries(0, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x03, 0x03, 0x02, 0x01, 0x02,
	}, entries)
}

func TestFollowerAppendEntriesRequestReplaceEntries(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(3))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x02, 0x01, 0x02,
		0x01, 0x03, 0x02, 0x01, 0x03,
	})
	requireT.NoError(err)

	r := newReactor(s)
	r.syncedCount = 5

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         4,
		NextLogIndex: 10,
		NextLogTerm:  4,
		LastLogTerm:  2,
		Data:         []byte{0x01, 0x04, 0x02, 0x01, 0x04},
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.EqualValues(5, r.syncedCount)

	requireT.EqualValues(4, s.CurrentTerm())
	_, _, entries, err := s.Entries(0, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x02, 0x01, 0x02,
		0x01, 0x04, 0x02, 0x01, 0x04,
	}, entries)
}

func TestFollowerAppendEntriesRequestReplaceEntriesAtSynced(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(3))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x02, 0x01, 0x02,
		0x01, 0x03, 0x02, 0x01, 0x03,
	})
	requireT.NoError(err)

	r := newReactor(s)
	r.syncedCount = 10

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         4,
		NextLogIndex: 10,
		NextLogTerm:  4,
		LastLogTerm:  2,
		Data:         []byte{0x01, 0x04, 0x02, 0x01, 0x04},
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.EqualValues(10, r.syncedCount)

	requireT.EqualValues(4, s.CurrentTerm())
	_, _, entries, err := s.Entries(0, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x02, 0x01, 0x02,
		0x01, 0x04, 0x02, 0x01, 0x04,
	}, entries)
}

func TestFollowerAppendEntriesRequestReplaceEntriesBelowSynced(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(3))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x02, 0x01, 0x02,
		0x01, 0x03, 0x02, 0x01, 0x03,
	})
	requireT.NoError(err)

	r := newReactor(s)
	r.syncedCount = 10

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         4,
		NextLogIndex: 5,
		NextLogTerm:  4,
		LastLogTerm:  1,
		Data:         []byte{0x01, 0x04, 0x02, 0x01, 0x04},
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.EqualValues(5, r.syncedCount)

	requireT.EqualValues(4, s.CurrentTerm())
	_, _, entries, err := s.Entries(0, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x04, 0x02, 0x01, 0x04,
	}, entries)
}

func TestFollowerAppendEntriesRequestDiscardEntriesOnTermMismatch(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	})
	requireT.NoError(err)

	r := newReactor(s)
	r.syncedCount = 11

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         4,
		NextLogIndex: 11,
		NextLogTerm:  4,
		LastLogTerm:  3,
		Data:         []byte{0x01, 0x04, 0x02, 0x01, 0x04},
		LeaderCommit: 5,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.EqualValues(5, r.leaderCommittedCount)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 5,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Messages: []any{
			&types.AppendEntriesResponse{
				Term:         4,
				NextLogIndex: 5,
				SyncLogIndex: 5,
			},
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.EqualValues(5, r.syncedCount)

	requireT.EqualValues(4, s.CurrentTerm())
	_, _, entries, err := s.Entries(0, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
	}, entries)
}

func TestFollowerAppendEntriesRequestDiscardEntriesOnTermMismatchTwice(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(3))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x03, 0x02, 0x02, 0x02,
		0x01, 0x03, 0x03, 0x02, 0x03, 0x03,
	})
	requireT.NoError(err)

	r := newReactor(s)

	// First time.

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         5,
		NextLogIndex: 17,
		NextLogTerm:  5,
		LastLogTerm:  4,
		Data:         []byte{0x01, 0x05},
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Messages: []any{
			&types.AppendEntriesResponse{
				Term:         5,
				NextLogIndex: 11,
			},
		},
	}, result)
	requireT.EqualValues(11, r.nextLogIndex)
	requireT.EqualValues(2, r.lastLogTerm)

	requireT.EqualValues(5, s.CurrentTerm())
	_, _, entries, err := s.Entries(0, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x03, 0x02, 0x02, 0x02,
	}, entries)

	// Second time.

	result, err = r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         6,
		NextLogIndex: 6,
		NextLogTerm:  6,
		LastLogTerm:  3,
		Data:         []byte{0x01, 0x06},
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Messages: []any{
			&types.AppendEntriesResponse{
				Term:         6,
				NextLogIndex: 5,
			},
		},
	}, result)
	requireT.EqualValues(5, r.nextLogIndex)
	requireT.EqualValues(1, r.lastLogTerm)

	requireT.EqualValues(6, s.CurrentTerm())
	_, _, entries, err = s.Entries(0, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
	}, entries)
}

func TestFollowerAppendEntriesRequestRejectIfNoPreviousEntry(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	})
	requireT.NoError(err)

	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         4,
		NextLogIndex: 1000,
		NextLogTerm:  4,
		LastLogTerm:  3,
		Data:         []byte{0x01, 0x04},
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Messages: []any{
			&types.AppendEntriesResponse{
				Term:         4,
				NextLogIndex: 11,
			},
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(peer1ID, r.leaderID)

	requireT.EqualValues(4, s.CurrentTerm())
	_, _, entries, err := s.Entries(0, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	}, entries)
}

func TestFollowerAppendEntriesRequestUpdateCurrentTermOnHeartbeat(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	})
	requireT.NoError(err)
	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         4,
		NextLogIndex: 11,
		NextLogTerm:  2,
		LastLogTerm:  2,
		Data:         nil,
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(peer1ID, r.leaderID)

	requireT.EqualValues(4, s.CurrentTerm())
	_, _, entries, err := s.Entries(0, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	}, entries)
}

func TestFollowerAppendEntriesRequestSendResponseIfLastLogTermIsLower(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	})
	requireT.NoError(err)
	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         4,
		NextLogIndex: 11,
		NextLogTerm:  3,
		LastLogTerm:  2,
		Data:         nil,
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Messages: []any{
			&types.AppendEntriesResponse{
				Term:         4,
				NextLogIndex: 11,
				SyncLogIndex: 0,
			},
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(peer1ID, r.leaderID)

	requireT.EqualValues(4, s.CurrentTerm())
	_, _, entries, err := s.Entries(0, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	}, entries)
}

func TestFollowerAppendEntriesRequestSendResponseIfNextLogIndexIsLower(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	})
	requireT.NoError(err)
	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         4,
		NextLogIndex: 12,
		NextLogTerm:  2,
		LastLogTerm:  2,
		Data:         nil,
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Messages: []any{
			&types.AppendEntriesResponse{
				Term:         4,
				NextLogIndex: 11,
				SyncLogIndex: 0,
			},
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(peer1ID, r.leaderID)

	requireT.EqualValues(4, s.CurrentTerm())
	_, _, entries, err := s.Entries(0, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	}, entries)
}

func TestFollowerAppendEntriesRequestDoNothingOnHeartbeat(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	})
	requireT.NoError(err)

	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         2,
		NextLogIndex: 11,
		NextLogTerm:  2,
		LastLogTerm:  2,
		Data:         nil,
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(peer1ID, r.leaderID)

	requireT.EqualValues(2, s.CurrentTerm())
	_, _, entries, err := s.Entries(0, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	}, entries)
}

func TestFollowerAppendEntriesRequestDoNothingOnLowerTerm(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(4))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	})
	requireT.NoError(err)
	r := newReactor(s)

	result, err := r.Apply(peer2ID, &types.AppendEntriesRequest{
		Term:         3,
		NextLogIndex: 11,
		NextLogTerm:  3,
		LastLogTerm:  2,
		Data:         []byte{0x01, 0x03},
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer2ID,
		},
		Messages: []any{
			&types.AppendEntriesResponse{
				Term:         4,
				NextLogIndex: 11,
			},
		},
	}, result)
	requireT.Zero(r.ignoreElectionTick)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)

	requireT.EqualValues(4, s.CurrentTerm())
	_, _, entries, err := s.Entries(0, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	}, entries)
}

func TestFollowerAppendEntriesRequestSetCommittedCountToLeaderCommit(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(1))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x04, 0x03, 0x00, 0x00, 0x00, 0x02, 0x01, 0x00,
	})
	requireT.NoError(err)

	r := newReactor(s)
	r.commitInfo = types.CommitInfo{CommittedCount: 0}
	r.syncedCount = 10

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         1,
		NextLogIndex: 10,
		NextLogTerm:  1,
		LastLogTerm:  1,
		Data:         []byte{0x02, 0x01, 0x01, 0x03, 0x02, 0x02, 0x02},
		LeaderCommit: 7,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.EqualValues(7, r.leaderCommittedCount)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 7,
		},
	}, result)

	requireT.EqualValues(1, s.CurrentTerm())
	_, _, entries, err := s.Entries(0, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{
		0x01, 0x01, 0x04, 0x03, 0x00, 0x00, 0x00, 0x02, 0x01, 0x00, 0x02, 0x01, 0x01, 0x03, 0x02, 0x02, 0x02,
	}, entries)
}

func TestFollowerAppendEntriesRequestSetCommittedCountToLeaderCommitOnHeartbeat(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(1))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x04, 0x03, 0x00, 0x00, 0x00, 0x02, 0x01, 0x01, 0x02, 0x02, 0x02,
	})
	requireT.NoError(err)

	r := newReactor(s)
	r.commitInfo = types.CommitInfo{CommittedCount: 1}
	r.syncedCount = 10

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         1,
		NextLogIndex: 13,
		NextLogTerm:  1,
		LastLogTerm:  1,
		Data:         nil,
		LeaderCommit: 7,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.EqualValues(7, r.leaderCommittedCount)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 7,
		},
	}, result)

	requireT.EqualValues(1, s.CurrentTerm())
	_, _, entries, err := s.Entries(0, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{
		0x01, 0x01, 0x04, 0x03, 0x00, 0x00, 0x00, 0x02, 0x01, 0x01, 0x02, 0x02, 0x02,
	}, entries)
}

func TestFollowerAppendEntriesRequestSetCommittedCountToSyncedLength(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(1))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x04, 0x03, 0x00, 0x00, 0x00, 0x03, 0x02, 0x00, 0x00,
	})
	requireT.NoError(err)

	r := newReactor(s)
	r.commitInfo = types.CommitInfo{CommittedCount: 1}
	r.syncedCount = 7

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         1,
		NextLogIndex: 11,
		NextLogTerm:  1,
		LastLogTerm:  1,
		Data:         []byte{0x02, 0x01, 0x00},
		LeaderCommit: 100,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.EqualValues(100, r.leaderCommittedCount)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 7,
		},
	}, result)

	requireT.EqualValues(1, s.CurrentTerm())
	_, _, entries, err := s.Entries(0, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{
		0x01, 0x01, 0x04, 0x03, 0x00, 0x00, 0x00, 0x03, 0x02, 0x00, 0x00, 0x02, 0x01, 0x00,
	}, entries)
}

func TestFollowerAppendEntriesRequestSetCommittedCountToSyncedLengthOnHeartbeat(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(1))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x04, 0x03, 0x00, 0x00, 0x00, 0x03, 0x02, 0x00, 0x00,
	})
	requireT.NoError(err)

	r := newReactor(s)
	r.commitInfo = types.CommitInfo{CommittedCount: 1}
	r.syncedCount = 7

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         1,
		NextLogIndex: 11,
		NextLogTerm:  1,
		LastLogTerm:  1,
		Data:         nil,
		LeaderCommit: 100,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.EqualValues(100, r.leaderCommittedCount)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 7,
		},
	}, result)

	requireT.EqualValues(1, s.CurrentTerm())
	_, _, entries, err := s.Entries(0, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{
		0x01, 0x01, 0x04, 0x03, 0x00, 0x00, 0x00, 0x03, 0x02, 0x00, 0x00,
	}, entries)
}

func TestFollowerAppendEntriesRequestDoNotSetCommittedCountToStaleSyncedLength(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x04, 0x03, 0x00, 0x00, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	})
	requireT.NoError(err)

	r := newReactor(s)
	r.commitInfo = types.CommitInfo{CommittedCount: 7}
	r.syncedCount = 7

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         3,
		NextLogIndex: 13,
		NextLogTerm:  3,
		LastLogTerm:  3,
		Data:         nil,
		LeaderCommit: 13,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.EqualValues(13, r.leaderCommittedCount)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 7,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Messages: []any{
			&types.AppendEntriesResponse{
				Term:         3,
				NextLogIndex: 7,
				SyncLogIndex: 7,
			},
		},
	}, result)

	requireT.EqualValues(3, s.CurrentTerm())
	_, _, entries, err := s.Entries(0, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{
		0x01, 0x01, 0x04, 0x03, 0x00, 0x00, 0x00,
	}, entries)
}

func TestFollowerAppendEntriesRequestDoNotSetCommittedCountToStaleCommit(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(1))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x04, 0x03, 0x00, 0x00, 0x00, 0x02, 0x01, 0x00,
	})
	requireT.NoError(err)

	r := newReactor(s)
	r.commitInfo = types.CommitInfo{CommittedCount: 10}
	r.syncedCount = 10

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         1,
		NextLogIndex: 3,
		NextLogTerm:  1,
		LastLogTerm:  1,
		Data:         nil,
		LeaderCommit: 7,
	})
	requireT.Error(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Zero(r.leaderCommittedCount)
	requireT.Equal(Result{}, result)

	requireT.EqualValues(1, s.CurrentTerm())
	_, _, entries, err := s.Entries(0, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{
		0x01, 0x01, 0x04, 0x03, 0x00, 0x00, 0x00, 0x02, 0x01, 0x00,
	}, entries)
}

func TestFollowerAppendEntriesRequestErrorIfBelowCommit(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(1))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x04, 0x03, 0x00, 0x00, 0x00,
	})
	requireT.NoError(err)

	r := newReactor(s)
	r.commitInfo = types.CommitInfo{CommittedCount: 3}

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         2,
		NextLogIndex: 2,
		NextLogTerm:  2,
		LastLogTerm:  1,
		Data:         nil,
		LeaderCommit: 2,
	})
	requireT.Error(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Zero(r.leaderCommittedCount)
	requireT.Equal(Result{}, result)
}

func TestFollowerApplyVoteRequestGrantedOnEmptyLog(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(1))
	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.VoteRequest{
		Term:         1,
		NextLogIndex: 0,
		LastLogTerm:  0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
		Channel: ChannelP2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Messages: []any{
			&types.VoteResponse{
				Term:        1,
				VoteGranted: true,
			},
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)

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
	s := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x02, 0x02, 0x01, 0x00,
	})
	requireT.NoError(err)
	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.VoteRequest{
		Term:         2,
		NextLogIndex: 11,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
		Channel: ChannelP2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Messages: []any{
			&types.VoteResponse{
				Term:        2,
				VoteGranted: true,
			},
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)

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
	s := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	})
	requireT.NoError(err)
	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.VoteRequest{
		Term:         2,
		NextLogIndex: 15,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
		Channel: ChannelP2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Messages: []any{
			&types.VoteResponse{
				Term:        2,
				VoteGranted: true,
			},
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)

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
	s := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.VoteRequest{
		Term:         3,
		NextLogIndex: 0,
		LastLogTerm:  0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
		Channel: ChannelP2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Messages: []any{
			&types.VoteResponse{
				Term:        3,
				VoteGranted: true,
			},
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)

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
	s := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x02, 0x02, 0x01, 0x00,
	})
	requireT.NoError(err)
	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.VoteRequest{
		Term:         2,
		NextLogIndex: 11,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
		Channel: ChannelP2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Messages: []any{
			&types.VoteResponse{
				Term:        2,
				VoteGranted: true,
			},
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.EqualValues(2, s.CurrentTerm())

	result, err = r.Apply(peer1ID, &types.VoteRequest{
		Term:         2,
		NextLogIndex: 11,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
		Channel: ChannelP2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Messages: []any{
			&types.VoteResponse{
				Term:        2,
				VoteGranted: true,
			},
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)
	requireT.EqualValues(2, s.CurrentTerm())
}

func TestFollowerApplyVoteRequestGrantVoteToOtherCandidateInNextTerm(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x02, 0x02, 0x01, 0x00,
	})
	requireT.NoError(err)
	requireT.NoError(err)
	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.VoteRequest{
		Term:         2,
		NextLogIndex: 11,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
		Channel: ChannelP2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Messages: []any{
			&types.VoteResponse{
				Term:        2,
				VoteGranted: true,
			},
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.EqualValues(2, s.CurrentTerm())

	result, err = r.Apply(peer2ID, &types.VoteRequest{
		Term:         3,
		NextLogIndex: 11,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
		Channel: ChannelP2P,
		Recipients: []magmatypes.ServerID{
			peer2ID,
		},
		Messages: []any{
			&types.VoteResponse{
				Term:        3,
				VoteGranted: true,
			},
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)
	requireT.EqualValues(3, s.CurrentTerm())
}

func TestFollowerApplyVoteRequestRejectedOnPastTerm(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.VoteRequest{
		Term:         1,
		NextLogIndex: 0,
		LastLogTerm:  0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
		Channel: ChannelP2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Messages: []any{
			&types.VoteResponse{
				Term:        2,
				VoteGranted: false,
			},
		},
	}, result)
	requireT.Zero(r.ignoreElectionTick)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)

	requireT.EqualValues(2, s.CurrentTerm())

	granted, err := s.VoteFor(peer2ID)
	requireT.NoError(err)
	requireT.True(granted)
}

func TestFollowerApplyVoteRequestRejectedOnLowerLastLogTerm(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x02, 0x02, 0x01, 0x00,
	})
	requireT.NoError(err)
	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.VoteRequest{
		Term:         3,
		NextLogIndex: 11,
		LastLogTerm:  1,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
		Channel: ChannelP2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Messages: []any{
			&types.VoteResponse{
				Term:        3,
				VoteGranted: false,
			},
		},
	}, result)
	requireT.Zero(r.ignoreElectionTick)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)

	requireT.EqualValues(3, s.CurrentTerm())

	granted, err := s.VoteFor(peer2ID)
	requireT.NoError(err)
	requireT.True(granted)
}

func TestFollowerApplyVoteRequestRejectedOnShorterLog(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	})
	requireT.NoError(err)
	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.VoteRequest{
		Term:         2,
		NextLogIndex: 11,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
		Channel: ChannelP2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Messages: []any{
			&types.VoteResponse{
				Term:        2,
				VoteGranted: false,
			},
		},
	}, result)
	requireT.Zero(r.ignoreElectionTick)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)

	requireT.EqualValues(2, s.CurrentTerm())

	granted, err := s.VoteFor(peer2ID)
	requireT.NoError(err)
	requireT.True(granted)
}

func TestFollowerApplyVoteRequestRejectOtherCandidates(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x02, 0x02, 0x01, 0x00,
	})
	requireT.NoError(err)
	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.VoteRequest{
		Term:         2,
		NextLogIndex: 11,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
		Channel: ChannelP2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Messages: []any{
			&types.VoteResponse{
				Term:        2,
				VoteGranted: true,
			},
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.EqualValues(2, s.CurrentTerm())

	result, err = r.Apply(peer2ID, &types.VoteRequest{
		Term:         2,
		NextLogIndex: 11,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
		Channel: ChannelP2P,
		Recipients: []magmatypes.ServerID{
			peer2ID,
		},
		Messages: []any{
			&types.VoteResponse{
				Term:        2,
				VoteGranted: false,
			},
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)
	requireT.EqualValues(2, s.CurrentTerm())
}

func TestFollowerApplyElectionTimeoutAfterElectionTime(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	r := newReactor(s)

	result, err := r.Apply(magmatypes.ZeroServerID, types.ElectionTick(1))
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.EqualValues(2, r.ignoreElectionTick)
	requireT.EqualValues(1, r.electionTick)
	requireT.EqualValues(1, s.CurrentTerm())
	requireT.EqualValues(1, r.votedForMe)
	requireT.Equal(Result{
		Role:     types.RoleCandidate,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
		Channel: ChannelP2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
			peer2ID,
			peer3ID,
			peer4ID,
		},
		Messages: []any{
			&types.VoteRequest{
				Term:         1,
				NextLogIndex: 0,
				LastLogTerm:  0,
			},
		},
	}, result)

	granted, err := s.VoteFor(peer1ID)
	requireT.NoError(err)
	requireT.False(granted)

	granted, err = s.VoteFor(serverID)
	requireT.NoError(err)
	requireT.True(granted)
}

func TestFollowerApplyElectionTimeoutBeforeElectionTime(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	r := newReactor(s)

	r.ignoreElectionTick = 2

	result, err := r.Apply(magmatypes.ZeroServerID, types.ElectionTick(1))
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.EqualValues(2, r.ignoreElectionTick)
	requireT.EqualValues(1, r.electionTick)
	requireT.EqualValues(0, s.CurrentTerm())
	requireT.EqualValues(0, r.votedForMe)
	requireT.Equal(Result{
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
	}, result)

	granted, err := s.VoteFor(peer1ID)
	requireT.NoError(err)
	requireT.True(granted)
}

func TestFollowerApplyHeartbeatTimeoutDoesNothing(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	r := newReactor(s)

	result, err := r.Apply(magmatypes.ZeroServerID, types.HeartbeatTick(1))
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Zero(r.ignoreHeartbeatTick)
	requireT.EqualValues(1, r.heartbeatTick)
	requireT.Equal(Result{}, result)
}

func TestFollowerApplySyncTickSyncedBelowCommitted(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(4))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x02, 0x01, 0x02,
		0x01, 0x03, 0x02, 0x01, 0x03,
		0x01, 0x04, 0x02, 0x01, 0x04,
	})
	requireT.NoError(err)
	r := newReactor(s)

	r.commitInfo = types.CommitInfo{
		CommittedCount: 25,
	}
	r.leaderCommittedCount = 25

	result, err := r.Apply(magmatypes.ZeroServerID, types.SyncTick{})
	requireT.Error(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{}, result)
}

func TestFollowerApplySyncTickCommitToLeader(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(4))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x02, 0x01, 0x02,
		0x01, 0x03, 0x02, 0x01, 0x03,
		0x01, 0x04, 0x02, 0x01, 0x04,
	})
	requireT.NoError(err)
	r := newReactor(s)

	r.commitInfo = types.CommitInfo{
		CommittedCount: 5,
	}
	r.leaderCommittedCount = 10

	result, err := r.Apply(magmatypes.ZeroServerID, types.SyncTick{})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 10,
		},
	}, result)
}

func TestFollowerApplySyncTickCommitToSynced(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(4))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x02, 0x01, 0x02,
		0x01, 0x03, 0x02, 0x01, 0x03,
		0x01, 0x04, 0x02, 0x01, 0x04,
	})
	requireT.NoError(err)
	r := newReactor(s)

	r.commitInfo = types.CommitInfo{
		CommittedCount: 5,
	}
	r.leaderCommittedCount = 100

	result, err := r.Apply(magmatypes.ZeroServerID, types.SyncTick{})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 20,
		},
	}, result)
}

func TestFollowerApplySyncTickSendResponse(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(4))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x02, 0x01, 0x02,
		0x01, 0x03, 0x02, 0x01, 0x03,
		0x01, 0x04, 0x02, 0x01, 0x04,
	})
	requireT.NoError(err)
	r := newReactor(s)

	r.commitInfo = types.CommitInfo{
		CommittedCount: 5,
	}
	r.leaderCommittedCount = 10
	r.leaderID = peer1ID

	result, err := r.Apply(magmatypes.ZeroServerID, types.SyncTick{})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 10,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Messages: []any{
			&types.AppendEntriesResponse{
				Term:         4,
				NextLogIndex: 20,
				SyncLogIndex: 20,
			},
		},
	}, result)
}

func TestFollowerApplyPeerConnectedDoesNothing(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	r := newReactor(s)

	result, err := r.Apply(magmatypes.ServerID(uuid.New()), nil)
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{}, result)
}

func TestFollowerApplyClientRequestIgnoreIfNotLeader(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(1))
	r := newReactor(s)

	result, err := r.Apply(magmatypes.ZeroServerID, &types.ClientRequest{
		Data: []byte{0x01},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
	}, result)
	requireT.Zero(r.ignoreHeartbeatTick)
	requireT.Zero(r.nextLogIndex)
	requireT.Empty(r.nextLogIndex)
	requireT.Empty(r.matchIndex)
}
