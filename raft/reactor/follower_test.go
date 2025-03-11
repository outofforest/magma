package reactor

import (
	"bytes"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/raft/state"
	"github.com/outofforest/magma/raft/types"
	magmatypes "github.com/outofforest/magma/types"
)

const (
	maxReadLogSize = 25
	logSize        = 1024 * 1024
)

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
	}
)

func newState() (*state.State, []byte) {
	return state.NewInMemory(logSize)
}

func newReactor(s *state.State) *Reactor {
	return New(config, s)
}

func TestFollowerInitialRole(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState()
	r := newReactor(s)

	requireT.Equal(types.RoleFollower, r.role)
	requireT.EqualValues(0, r.electionTick)
	requireT.EqualValues(0, r.ignoreElectionTick)
}

func TestFollowerSetup(t *testing.T) {
	requireT := require.New(t)
	s, log := newState()
	r := newReactor(s)

	r.role = types.RoleCandidate
	r.leaderID = serverID
	r.votedForMe = 2
	r.electionTick = 1
	r.ignoreElectionTick = 0
	r.sync[peer1ID] = &syncProgress{
		NextIndex: 100,
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

	requireT.Equal(bytes.Repeat([]byte{0x00}, logSize), log)
}

func TestFollowerAppendTxAppendToEmptyLog(t *testing.T) {
	requireT := require.New(t)
	s, log := newState()
	requireT.NoError(s.SetCurrentTerm(1))
	r := newReactor(s)
	r.leaderID = peer1ID

	tx := []byte{0x01, 0x01, 0x02, 0x01, 0x00}
	result, err := r.Apply(peer1ID, tx)
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

	logEqual(requireT, tx, log)
}

func TestFollowerAppendTxAppendToNonEmptyLog(t *testing.T) {
	requireT := require.New(t)
	s, log := newState()
	requireT.NoError(s.SetCurrentTerm(1))
	_, _, err := s.Append([]byte{
		0x01, 0x01, 0x03, 0x02, 0x00, 0x00,
	}, true)
	requireT.NoError(err)

	r := newReactor(s)
	r.leaderID = peer1ID

	result, err := r.Apply(peer1ID, []byte{0x04, 0x03, 0x01, 0x02, 0x03})
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
	logEqual(requireT, []byte{
		0x01, 0x01, 0x03, 0x02, 0x00, 0x00, 0x04, 0x03, 0x01, 0x02, 0x03,
	}, log)
}

func TestFollowerAppendEntriesRequestOnFutureTerm(t *testing.T) {
	requireT := require.New(t)
	s, log := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append([]byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	}, true)
	requireT.NoError(err)

	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         3,
		NextLogIndex: 11,
		LastLogTerm:  2,
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
		Message: &types.AppendEntriesResponse{
			Term:         3,
			NextLogIndex: 11,
			SyncLogIndex: 0,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(peer1ID, r.leaderID)

	requireT.EqualValues(3, s.CurrentTerm())
	logEqual(requireT, []byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	}, log)
}

func TestFollowerAppendEntriesRequestDiscardEntries(t *testing.T) {
	requireT := require.New(t)
	s, log := newState()
	requireT.NoError(s.SetCurrentTerm(3))
	_, _, err := s.Append([]byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x02, 0x01, 0x02,
		0x01, 0x03, 0x02, 0x01, 0x03,
	}, true)
	requireT.NoError(err)

	r := newReactor(s)
	r.syncedCount = 5

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         4,
		NextLogIndex: 10,
		LastLogTerm:  2,
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
		Message: &types.AppendEntriesResponse{
			Term:         4,
			NextLogIndex: 10,
			SyncLogIndex: 5,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.EqualValues(5, r.syncedCount)

	requireT.EqualValues(4, s.CurrentTerm())
	logEqual(requireT, []byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x02, 0x01, 0x02,
	}, log)
}

func TestFollowerAppendEntriesRequestDiscardAtSynced(t *testing.T) {
	requireT := require.New(t)
	s, log := newState()
	requireT.NoError(s.SetCurrentTerm(3))
	_, _, err := s.Append([]byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x02, 0x01, 0x02,
		0x01, 0x03, 0x02, 0x01, 0x03,
	}, true)
	requireT.NoError(err)

	r := newReactor(s)
	r.syncedCount = 10

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         4,
		NextLogIndex: 10,
		LastLogTerm:  2,
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
		Message: &types.AppendEntriesResponse{
			Term:         4,
			NextLogIndex: 10,
			SyncLogIndex: 10,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.EqualValues(10, r.syncedCount)

	requireT.EqualValues(4, s.CurrentTerm())
	logEqual(requireT, []byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x02, 0x01, 0x02,
	}, log)
}

func TestFollowerAppendEntriesRequestDiscardOnTermMismatch(t *testing.T) {
	requireT := require.New(t)
	s, log := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append([]byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	}, true)
	requireT.NoError(err)

	r := newReactor(s)
	r.syncedCount = 11

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         4,
		NextLogIndex: 11,
		LastLogTerm:  3,
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
		Message: &types.AppendEntriesResponse{
			Term:         4,
			NextLogIndex: 5,
			SyncLogIndex: 5,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.EqualValues(5, r.syncedCount)

	requireT.EqualValues(4, s.CurrentTerm())
	logEqual(requireT, []byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
	}, log)
}

func TestFollowerAppendEntriesRequestDiscardOnTermMismatchTwice(t *testing.T) {
	requireT := require.New(t)
	s, log := newState()
	requireT.NoError(s.SetCurrentTerm(3))
	_, _, err := s.Append([]byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x03, 0x02, 0x02, 0x02,
		0x01, 0x03, 0x03, 0x02, 0x03, 0x03,
	}, true)
	requireT.NoError(err)

	r := newReactor(s)

	// First time.

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         5,
		NextLogIndex: 17,
		LastLogTerm:  4,
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
		Message: &types.AppendEntriesResponse{
			Term:         5,
			NextLogIndex: 11,
		},
	}, result)
	requireT.EqualValues(11, r.nextLogIndex)
	requireT.EqualValues(2, r.lastLogTerm)

	requireT.EqualValues(5, s.CurrentTerm())
	logEqual(requireT, []byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x03, 0x02, 0x02, 0x02,
	}, log)

	// Second time.

	result, err = r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         6,
		NextLogIndex: 6,
		LastLogTerm:  3,
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
		Message: &types.AppendEntriesResponse{
			Term:         6,
			NextLogIndex: 5,
		},
	}, result)
	requireT.EqualValues(5, r.nextLogIndex)
	requireT.EqualValues(1, r.lastLogTerm)

	requireT.EqualValues(6, s.CurrentTerm())
	logEqual(requireT, []byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
	}, log)
}

func TestFollowerAppendEntriesRequestRejectIfNoPreviousEntry(t *testing.T) {
	requireT := require.New(t)
	s, log := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append([]byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	}, true)
	requireT.NoError(err)

	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         4,
		NextLogIndex: 1000,
		LastLogTerm:  3,
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
		Message: &types.AppendEntriesResponse{
			Term:         4,
			NextLogIndex: 11,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(peer1ID, r.leaderID)

	requireT.EqualValues(4, s.CurrentTerm())
	logEqual(requireT, []byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	}, log)
}

func TestFollowerAppendEntriesRequestSendResponseIfLastLogTermIsLower(t *testing.T) {
	requireT := require.New(t)
	s, log := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append([]byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         4,
		NextLogIndex: 11,
		LastLogTerm:  2,
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
		Message: &types.AppendEntriesResponse{
			Term:         4,
			NextLogIndex: 11,
			SyncLogIndex: 0,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(peer1ID, r.leaderID)

	requireT.EqualValues(4, s.CurrentTerm())
	logEqual(requireT, []byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	}, log)
}

func TestFollowerAppendEntriesRequestSendResponseIfNextLogIndexIsLower(t *testing.T) {
	requireT := require.New(t)
	s, log := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append([]byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         4,
		NextLogIndex: 12,
		LastLogTerm:  2,
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
		Message: &types.AppendEntriesResponse{
			Term:         4,
			NextLogIndex: 11,
			SyncLogIndex: 0,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(peer1ID, r.leaderID)

	requireT.EqualValues(4, s.CurrentTerm())
	logEqual(requireT, []byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	}, log)
}

func TestFollowerAppendEntriesRequestDoNothingOnLowerTerm(t *testing.T) {
	requireT := require.New(t)
	s, log := newState()
	requireT.NoError(s.SetCurrentTerm(4))
	_, _, err := s.Append([]byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)

	result, err := r.Apply(peer2ID, &types.AppendEntriesRequest{
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
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer2ID,
		},
		Message: &types.AppendEntriesResponse{
			Term:         4,
			NextLogIndex: 11,
		},
	}, result)
	requireT.Zero(r.ignoreElectionTick)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)

	requireT.EqualValues(4, s.CurrentTerm())
	logEqual(requireT, []byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	}, log)
}

func TestFollowerApplyVoteRequestGrantedOnEmptyLog(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState()
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
		Message: &types.VoteResponse{
			Term:        1,
			VoteGranted: true,
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
	s, _ := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append([]byte{
		0x01, 0x01, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x02, 0x02, 0x01, 0x00,
	}, true)
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
		Message: &types.VoteResponse{
			Term:        2,
			VoteGranted: true,
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
	s, _ := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append([]byte{
		0x01, 0x01, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	}, true)
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
		Message: &types.VoteResponse{
			Term:        2,
			VoteGranted: true,
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
	s, _ := newState()
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
		Message: &types.VoteResponse{
			Term:        3,
			VoteGranted: true,
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
	s, _ := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append([]byte{
		0x01, 0x01, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x02, 0x02, 0x01, 0x00,
	}, true)
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
		Message: &types.VoteResponse{
			Term:        2,
			VoteGranted: true,
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
		Message: &types.VoteResponse{
			Term:        2,
			VoteGranted: true,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)
	requireT.EqualValues(2, s.CurrentTerm())
}

func TestFollowerApplyVoteRequestGrantVoteToOtherCandidateInNextTerm(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append([]byte{
		0x01, 0x01, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x02, 0x02, 0x01, 0x00,
	}, true)
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
		Message: &types.VoteResponse{
			Term:        2,
			VoteGranted: true,
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
		Message: &types.VoteResponse{
			Term:        3,
			VoteGranted: true,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)
	requireT.EqualValues(3, s.CurrentTerm())
}

func TestFollowerApplyVoteRequestRejectedOnPastTerm(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState()
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
		Message: &types.VoteResponse{
			Term:        2,
			VoteGranted: false,
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
	s, _ := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append([]byte{
		0x01, 0x01, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x02, 0x02, 0x01, 0x00,
	}, true)
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
		Message: &types.VoteResponse{
			Term:        3,
			VoteGranted: false,
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
	s, _ := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append([]byte{
		0x01, 0x01, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	}, true)
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
		Message: &types.VoteResponse{
			Term:        2,
			VoteGranted: false,
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
	s, _ := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append([]byte{
		0x01, 0x01, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x02, 0x02, 0x01, 0x00,
	}, true)
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
		Message: &types.VoteResponse{
			Term:        2,
			VoteGranted: true,
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
		Message: &types.VoteResponse{
			Term:        2,
			VoteGranted: false,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)
	requireT.EqualValues(2, s.CurrentTerm())
}

func TestFollowerApplyElectionTimeoutAfterElectionTime(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState()
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
		Message: &types.VoteRequest{
			Term:         1,
			NextLogIndex: 0,
			LastLogTerm:  0,
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
	s, _ := newState()
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

func TestFollowerApplyPeerConnectedDoesNothing(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState()
	r := newReactor(s)

	result, err := r.Apply(magmatypes.ServerID(uuid.New()), nil)
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{}, result)
}

func TestFollowerApplyClientRequestIgnoreIfNotLeader(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState()
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

func logEqual(requireT *require.Assertions, expected, actual []byte) {
	requireT.Equal(expected, actual[:len(expected)])
}
