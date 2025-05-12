package reactor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/raft/types"
	magmatypes "github.com/outofforest/magma/types"
)

func TestLeaderSetup(t *testing.T) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(3))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)

	r.role = types.RoleCandidate
	r.leaderID = peer1ID
	r.votedForMe = 10
	r.heartbeatTick = 1
	r.ignoreHeartbeatTick = 0
	r.indexTermStarted = 12
	r.nextIndex[peer1ID] = 100
	r.matchIndex[peer1ID] = 100

	requireT.EqualValues(2, r.lastLogTerm)
	r.commitInfo = types.CommitInfo{
		NextLogIndex:   43,
		CommittedCount: 2,
		HotEndIndex:    0,
	}

	result, err := r.transitionToLeader()
	requireT.NoError(err)

	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(serverID, r.leaderID)
	requireT.Equal(10, r.votedForMe)
	requireT.EqualValues(1, r.heartbeatTick)
	requireT.EqualValues(2, r.ignoreHeartbeatTick)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   53,
			CommittedCount: 2,
			HotEndIndex:    53,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
			peer2ID,
			peer3ID,
			peer4ID,
			passivePeerID,
		},
		Message: &types.LogSyncRequest{
			Term:         3,
			NextLogIndex: 53,
			LastLogTerm:  3,
		},
	}, result)
	requireT.EqualValues(43, r.indexTermStarted)
	requireT.Equal(map[magmatypes.ServerID]magmatypes.Index{
		peer1ID: 53,
		peer2ID: 53,
		peer3ID: 53,
		peer4ID: 53,
	}, r.nextIndex)
	requireT.Equal(map[magmatypes.ServerID]magmatypes.Index{
		peer1ID: 0,
		peer2ID: 0,
		peer3ID: 0,
		peer4ID: 0,
	}, r.matchIndex)

	requireT.EqualValues(3, r.lastLogTerm)

	requireT.EqualValues(3, s.CurrentTerm())

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
		txb(0x03),
	))
}

func TestLeaderApplyLogSyncRequestTransitionToFollowerOnFutureTerm(t *testing.T) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(3))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x01, 0x00),
	), true, true)
	requireT.NoError(err)

	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)
	requireT.EqualValues(3, s.CurrentTerm())

	result, err := r.Apply(peer1ID, &types.LogSyncRequest{
		Term:         4,
		NextLogIndex: 42,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   42,
			CommittedCount: 0,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Message: &types.LogSyncResponse{
			Term:         4,
			NextLogIndex: 42,
			SyncLogIndex: 0,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(peer1ID, r.leaderID)

	requireT.EqualValues(4, s.CurrentTerm())

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x01, 0x00),
	))
}

func TestLeaderApplyLogSyncRequestErrorIfThereIsAnotherLeader(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(3))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x01, 0x00),
	), true, true)
	requireT.NoError(err)

	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)
	requireT.EqualValues(3, s.CurrentTerm())

	result, err := r.Apply(peer1ID, &types.LogSyncRequest{
		Term:         3,
		NextLogIndex: 52,
		LastLogTerm:  3,
	})
	requireT.Error(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{}, result)
}

func TestLeaderApplyLogSyncResponseTransitionToFollowerOnFutureTerm(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(1))
	r := newReactor(s)
	_, err := r.transitionToLeader()
	requireT.NoError(err)

	result, err := r.Apply(peer1ID, &types.LogSyncResponse{
		Term:         3,
		NextLogIndex: 10,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Zero(r.votedForMe)
	requireT.Equal(Result{
		CommitInfo: types.CommitInfo{
			NextLogIndex:   10,
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)

	requireT.EqualValues(3, s.CurrentTerm())
}

func TestLeaderApplyLogACKTransitionToFollowerOnFutureTerm(t *testing.T) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(1))
	r := newReactor(s)
	_, err := r.transitionToLeader()
	requireT.NoError(err)

	result, err := r.Apply(peer1ID, &types.LogACK{
		Term:         2,
		NextLogIndex: 31,
		SyncLogIndex: 31,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   10,
			CommittedCount: 0,
		},
	}, result)

	txb := newTxBuilder()
	logEqual(requireT, dir, txb(0x01))
}

func TestLeaderApplyLogACKErrorIfReportedIndexIsGreater(t *testing.T) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(1))
	r := newReactor(s)
	_, err := r.transitionToLeader()
	requireT.NoError(err)

	result, err := r.Apply(peer1ID, &types.LogACK{
		Term:         1,
		NextLogIndex: 31,
		SyncLogIndex: 31,
	})
	requireT.Error(err)
	requireT.Equal(Result{}, result)

	txb := newTxBuilder()
	logEqual(requireT, dir, txb(0x01))
}

func TestLeaderApplyLogACKErrorIfSyncIndexIsGreater(t *testing.T) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(1))
	r := newReactor(s)
	_, err := r.transitionToLeader()
	requireT.NoError(err)

	result, err := r.Apply(peer1ID, &types.LogACK{
		Term:         1,
		NextLogIndex: 9,
		SyncLogIndex: 10,
	})
	requireT.Error(err)
	requireT.Equal(Result{}, result)

	txb := newTxBuilder()
	logEqual(requireT, dir, txb(0x01))
}

func TestLeaderApplyLogACKUpdateNextIndex(t *testing.T) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(2))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x00),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	result, err := r.Apply(peer1ID, &types.LogACK{
		Term:         2,
		NextLogIndex: 31,
		SyncLogIndex: 0,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   31,
			CommittedCount: 0,
			HotEndIndex:    31,
		},
	}, result)
	requireT.EqualValues(31, r.nextIndex[peer1ID])
	requireT.Zero(r.matchIndex[peer1ID])

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02),
	))
}

func TestLeaderApplyLogACKUpdateSyncIndex(t *testing.T) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(2))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x00),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	result, err := r.Apply(peer1ID, &types.LogACK{
		Term:         2,
		NextLogIndex: 31,
		SyncLogIndex: 10,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   31,
			CommittedCount: 0,
			HotEndIndex:    31,
		},
	}, result)
	requireT.EqualValues(31, r.nextIndex[peer1ID])
	requireT.EqualValues(10, r.matchIndex[peer1ID])

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02),
	))
}

func TestLeaderApplyLogACKDoNothingIfNextIndexIsLower(t *testing.T) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(2))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x00),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.nextIndex[peer1ID] = 21

	result, err := r.Apply(peer1ID, &types.LogACK{
		Term:         2,
		NextLogIndex: 10,
		SyncLogIndex: 10,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   31,
			CommittedCount: 0,
			HotEndIndex:    31,
		},
	}, result)
	requireT.EqualValues(21, r.nextIndex[peer1ID])
	requireT.Zero(r.matchIndex[peer1ID])

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02),
	))
}

func TestLeaderApplyLogACKDoNothingIfSyncIndexIsLower(t *testing.T) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(2))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x00),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.matchIndex[peer1ID] = 21

	result, err := r.Apply(peer1ID, &types.LogACK{
		Term:         2,
		NextLogIndex: 31,
		SyncLogIndex: 10,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   31,
			CommittedCount: 0,
			HotEndIndex:    31,
		},
	}, result)
	requireT.EqualValues(31, r.nextIndex[peer1ID])
	requireT.EqualValues(21, r.matchIndex[peer1ID])

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02),
	))
}

func TestLeaderApplyLogACKCommitNotUpdatedIfBelowCurrentTerm(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(2))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x00),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.matchIndex[serverID] = 31

	result, err := r.Apply(peer1ID, &types.LogACK{
		Term:         2,
		NextLogIndex: 31,
		SyncLogIndex: 31,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   31,
			CommittedCount: 0,
			HotEndIndex:    31,
		},
	}, result)
	requireT.EqualValues(31, r.nextIndex[peer1ID])
	requireT.EqualValues(31, r.matchIndex[peer1ID])
	requireT.Equal(types.CommitInfo{
		NextLogIndex:   31,
		CommittedCount: 0,
		HotEndIndex:    31,
	}, r.commitInfo)

	result, err = r.Apply(peer2ID, &types.LogACK{
		Term:         2,
		NextLogIndex: 31,
		SyncLogIndex: 21,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   31,
			CommittedCount: 0,
			HotEndIndex:    31,
		},
	}, result)
	requireT.EqualValues(31, r.nextIndex[peer1ID])
	requireT.EqualValues(31, r.matchIndex[peer1ID])
	requireT.EqualValues(31, r.nextIndex[peer2ID])
	requireT.EqualValues(21, r.matchIndex[peer2ID])
	requireT.Equal(types.CommitInfo{
		NextLogIndex:   31,
		CommittedCount: 0,
		HotEndIndex:    31,
	}, r.commitInfo)
}

func TestLeaderApplyLogACKCommitNotUpdatedIfBelowCurrentCommit(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(2))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x00),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	_, err = r.Apply(magmatypes.ZeroServerID, &types.ClientRequest{
		Data: []byte{0x02, 0x01, 0x00},
	})
	requireT.NoError(err)

	r.matchIndex[serverID] = 42
	r.commitInfo.CommittedCount = 42

	result, err := r.Apply(peer1ID, &types.LogACK{
		Term:         2,
		NextLogIndex: 31,
		SyncLogIndex: 31,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   42,
			CommittedCount: 42,
			HotEndIndex:    42,
		},
	}, result)
	requireT.EqualValues(31, r.nextIndex[peer1ID])
	requireT.EqualValues(31, r.matchIndex[peer1ID])
	requireT.Equal(types.CommitInfo{
		NextLogIndex:   42,
		CommittedCount: 42,
		HotEndIndex:    42,
	}, r.commitInfo)

	result, err = r.Apply(peer2ID, &types.LogACK{
		Term:         2,
		NextLogIndex: 31,
		SyncLogIndex: 21,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   42,
			CommittedCount: 42,
			HotEndIndex:    42,
		},
	}, result)
	requireT.EqualValues(31, r.nextIndex[peer1ID])
	requireT.EqualValues(31, r.matchIndex[peer1ID])
	requireT.EqualValues(31, r.nextIndex[peer2ID])
	requireT.EqualValues(21, r.matchIndex[peer2ID])
	requireT.Equal(types.CommitInfo{
		NextLogIndex:   42,
		CommittedCount: 42,
		HotEndIndex:    42,
	}, r.commitInfo)
}

func TestLeaderApplyLogACKCommitNotUpdatedIfPeerIsPassive(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(2))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x00),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	_, err = r.Apply(magmatypes.ZeroServerID, &types.ClientRequest{
		Data: []byte{0x02, 0x01, 0x00},
	})
	requireT.NoError(err)

	r.matchIndex[serverID] = 42

	result, err := r.Apply(peer1ID, &types.LogACK{
		Term:         2,
		NextLogIndex: 31,
		SyncLogIndex: 31,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   42,
			CommittedCount: 0,
			HotEndIndex:    42,
		},
	}, result)
	requireT.EqualValues(31, r.nextIndex[peer1ID])
	requireT.Equal(map[magmatypes.ServerID]magmatypes.Index{
		serverID: 42,
		peer1ID:  31,
		peer2ID:  0,
		peer3ID:  0,
		peer4ID:  0,
	}, r.matchIndex)
	requireT.Equal(types.CommitInfo{
		NextLogIndex:   42,
		CommittedCount: 0,
		HotEndIndex:    42,
	}, r.commitInfo)

	result, err = r.Apply(passivePeerID, &types.LogACK{
		Term:         2,
		NextLogIndex: 42,
		SyncLogIndex: 42,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   42,
			CommittedCount: 0,
			HotEndIndex:    42,
		},
	}, result)
	requireT.EqualValues(31, r.nextIndex[peer1ID])
	requireT.EqualValues(42, r.nextIndex[passivePeerID])
	requireT.Equal(map[magmatypes.ServerID]magmatypes.Index{
		serverID: 42,
		peer1ID:  31,
		peer2ID:  0,
		peer3ID:  0,
		peer4ID:  0,
	}, r.matchIndex)
	requireT.Equal(types.CommitInfo{
		NextLogIndex:   42,
		CommittedCount: 0,
		HotEndIndex:    42,
	}, r.commitInfo)
}

func TestLeaderApplyLogACKUpdateLeaderCommitToCommonPoint(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(2))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x00),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	_, err = r.Apply(magmatypes.ZeroServerID, &types.ClientRequest{
		Data: []byte{0x02, 0x01, 0x00},
	})
	requireT.NoError(err)

	r.matchIndex[serverID] = 42

	result, err := r.Apply(peer1ID, &types.LogACK{
		Term:         2,
		NextLogIndex: 31,
		SyncLogIndex: 31,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   42,
			CommittedCount: 0,
			HotEndIndex:    42,
		},
	}, result)
	requireT.EqualValues(31, r.nextIndex[peer1ID])
	requireT.Equal(map[magmatypes.ServerID]magmatypes.Index{
		serverID: 42,
		peer1ID:  31,
		peer2ID:  0,
		peer3ID:  0,
		peer4ID:  0,
	}, r.matchIndex)
	requireT.Equal(types.CommitInfo{
		NextLogIndex:   42,
		CommittedCount: 0,
		HotEndIndex:    42,
	}, r.commitInfo)

	result, err = r.Apply(peer2ID, &types.LogACK{
		Term:         2,
		NextLogIndex: 42,
		SyncLogIndex: 42,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   42,
			CommittedCount: 31,
			HotEndIndex:    42,
		},
	}, result)
	requireT.EqualValues(31, r.nextIndex[peer1ID])
	requireT.EqualValues(42, r.nextIndex[peer2ID])
	requireT.Equal(map[magmatypes.ServerID]magmatypes.Index{
		serverID: 42,
		peer1ID:  31,
		peer2ID:  42,
		peer3ID:  0,
		peer4ID:  0,
	}, r.matchIndex)
	requireT.Equal(types.CommitInfo{
		NextLogIndex:   42,
		CommittedCount: 31,
		HotEndIndex:    42,
	}, r.commitInfo)
}

func TestLeaderApplyVoteRequestTransitionToFollowerOnFutureTerm(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(1))
	r := newReactor(s)
	_, err := r.transitionToLeader()
	requireT.NoError(err)
	requireT.EqualValues(1, s.CurrentTerm())

	result, err := r.Apply(peer1ID, &types.VoteRequest{
		Term:         3,
		NextLogIndex: 10,
		LastLogTerm:  1,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   10,
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

func TestLeaderApplyLogSyncResponseErrorIfReplicatedMore(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(5))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x01),
		txb(0x02), txb(0x01, 0x02),
		txb(0x03), txb(0x01, 0x03),
		txb(0x04), txb(0x01, 0x04),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.nextIndex[peer1ID] = 0

	result, err := r.Apply(peer1ID, &types.LogSyncResponse{
		Term:         5,
		NextLogIndex: 95,
	})
	requireT.Error(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{}, result)
	requireT.Zero(r.nextIndex[peer1ID])
	requireT.EqualValues(0, r.matchIndex[peer1ID])
	requireT.Equal(serverID, r.leaderID)
}

func TestLeaderApplyLogSyncResponseErrorIfSyncIsAheadLog(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(2))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x03, 0x01, 0x02, 0x03),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	result, err := r.Apply(peer1ID, &types.LogSyncResponse{
		Term:         2,
		NextLogIndex: 23,
		SyncLogIndex: 24,
	})
	requireT.Error(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{}, result)
}

func TestLeaderApplyLogSyncResponseIgnorePastTerm(t *testing.T) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(5))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x01),
		txb(0x02), txb(0x01, 0x02),
		txb(0x03), txb(0x01, 0x03),
		txb(0x04), txb(0x01, 0x04),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.nextIndex[peer1ID] = 0

	result, err := r.Apply(peer1ID, &types.LogSyncResponse{
		Term:         4,
		NextLogIndex: 84,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   94,
			CommittedCount: 0,
			HotEndIndex:    94,
		},
	}, result)
	requireT.Zero(r.nextIndex[peer1ID])
	requireT.EqualValues(0, r.matchIndex[peer1ID])
	requireT.Equal(serverID, r.leaderID)

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x01),
		txb(0x02), txb(0x01, 0x02),
		txb(0x03), txb(0x01, 0x03),
		txb(0x04), txb(0x01, 0x04),
		txb(0x05),
	))
}

func TestLeaderApplyLogSyncResponseCommonPointFound(t *testing.T) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(5))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x01),
		txb(0x02), txb(0x01, 0x02),
		txb(0x03), txb(0x01, 0x03),
		txb(0x04), txb(0x01, 0x04),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.nextIndex[peer1ID] = 42

	result, err := r.Apply(peer1ID, &types.LogSyncResponse{
		Term:         5,
		NextLogIndex: 42,
		SyncLogIndex: 30,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   94,
			CommittedCount: 0,
			HotEndIndex:    94,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Message: &StartTransfer{
			NextLogIndex: 42,
		},
	}, result)
	requireT.EqualValues(42, r.nextIndex[peer1ID])
	requireT.Equal(map[magmatypes.ServerID]magmatypes.Index{
		peer1ID: 30,
		peer2ID: 0,
		peer3ID: 0,
		peer4ID: 0,
	}, r.matchIndex)
	requireT.Equal(serverID, r.leaderID)

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x01),
		txb(0x02), txb(0x01, 0x02),
		txb(0x03), txb(0x01, 0x03),
		txb(0x04), txb(0x01, 0x04),
		txb(0x05),
	))
}

func TestLeaderApplyLogSyncResponseCommonPointFoundWithPassivePeer(t *testing.T) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(5))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x01),
		txb(0x02), txb(0x01, 0x02),
		txb(0x03), txb(0x01, 0x03),
		txb(0x04), txb(0x01, 0x04),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.nextIndex[passivePeerID] = 42

	result, err := r.Apply(passivePeerID, &types.LogSyncResponse{
		Term:         5,
		NextLogIndex: 42,
		SyncLogIndex: 30,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   94,
			CommittedCount: 0,
			HotEndIndex:    94,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			passivePeerID,
		},
		Message: &StartTransfer{
			NextLogIndex: 42,
		},
	}, result)
	requireT.EqualValues(42, r.nextIndex[passivePeerID])
	requireT.Equal(map[magmatypes.ServerID]magmatypes.Index{
		peer1ID: 0,
		peer2ID: 0,
		peer3ID: 0,
		peer4ID: 0,
	}, r.matchIndex)
	requireT.Equal(serverID, r.leaderID)

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x01),
		txb(0x02), txb(0x01, 0x02),
		txb(0x03), txb(0x01, 0x03),
		txb(0x04), txb(0x01, 0x04),
		txb(0x05),
	))
}

func TestLeaderApplyLogSyncResponseCommonPointNotFound(t *testing.T) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(5))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x01),
		txb(0x02), txb(0x01, 0x02),
		txb(0x03), txb(0x01, 0x03),
		txb(0x04), txb(0x01, 0x04),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.nextIndex[peer1ID] = 94

	result, err := r.Apply(peer1ID, &types.LogSyncResponse{
		Term:         5,
		NextLogIndex: 42,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   94,
			CommittedCount: 0,
			HotEndIndex:    94,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Message: &types.LogSyncRequest{
			Term:         5,
			NextLogIndex: 42,
			LastLogTerm:  2,
		},
	}, result)
	requireT.EqualValues(42, r.nextIndex[peer1ID])
	requireT.EqualValues(0, r.matchIndex[peer1ID])
	requireT.Equal(serverID, r.leaderID)

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x01),
		txb(0x02), txb(0x01, 0x02),
		txb(0x03), txb(0x01, 0x03),
		txb(0x04), txb(0x01, 0x04),
		txb(0x05),
	))
}

func TestLeaderApplyHeartbeatTimeoutAfterHeartbeatTime(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(5))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x01),
		txb(0x02), txb(0x01, 0x02),
		txb(0x03), txb(0x01, 0x03),
		txb(0x04), txb(0x01, 0x04),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.nextIndex = map[magmatypes.ServerID]magmatypes.Index{
		peer1ID: 94,
		peer2ID: 94,
		peer3ID: 94,
		peer4ID: 94,
	}

	result, err := r.Apply(magmatypes.ZeroServerID, types.HeartbeatTick(20))
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   94,
			CommittedCount: 0,
			HotEndIndex:    94,
		},
		Channel: ChannelP2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
			peer2ID,
			peer3ID,
			peer4ID,
		},
		Message: &types.Heartbeat{
			Term:         5,
			LeaderCommit: 0,
		},
		Force: true,
	}, result)
	requireT.EqualValues(20, r.heartbeatTick)
}

func TestLeaderApplyHeartbeatTimeoutBeforeHeartbeatTime(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(5))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x01),
		txb(0x02), txb(0x01, 0x02),
		txb(0x03), txb(0x01, 0x03),
		txb(0x04), txb(0x01, 0x04),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.nextIndex = map[magmatypes.ServerID]magmatypes.Index{
		peer1ID: 94,
		peer2ID: 94,
		peer3ID: 94,
		peer4ID: 94,
	}
	r.ignoreHeartbeatTick = 20

	result, err := r.Apply(magmatypes.ZeroServerID, types.HeartbeatTick(20))
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   94,
			CommittedCount: 0,
			HotEndIndex:    94,
		},
		Force: true,
	}, result)
	requireT.EqualValues(20, r.heartbeatTick)
}

func TestLeaderApplyHeartbeatTimeoutCommit(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(5))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x01),
		txb(0x02), txb(0x01, 0x02),
		txb(0x03), txb(0x01, 0x03),
		txb(0x04), txb(0x01, 0x04),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.nextIndex = map[magmatypes.ServerID]magmatypes.Index{
		peer1ID: 94,
		peer2ID: 94,
		peer3ID: 94,
		peer4ID: 94,
	}
	r.matchIndex = map[magmatypes.ServerID]magmatypes.Index{
		peer1ID: 94,
		peer2ID: 94,
	}

	result, err := r.Apply(magmatypes.ZeroServerID, types.HeartbeatTick(20))
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   94,
			CommittedCount: 94,
			HotEndIndex:    94,
		},
		Channel: ChannelP2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
			peer2ID,
			peer3ID,
			peer4ID,
		},
		Message: &types.Heartbeat{
			Term:         5,
			LeaderCommit: 94,
		},
		Force: true,
	}, result)
	requireT.EqualValues(20, r.heartbeatTick)
}

func TestLeaderApplyClientRequestIgnoreEmptyData(t *testing.T) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(4))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
		txb(0x03), txb(0x02, 0x00, 0x00),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.nextIndex = map[magmatypes.ServerID]magmatypes.Index{
		peer1ID: 75,
		peer2ID: 75,
		peer3ID: 75,
		peer4ID: 75,
	}

	result, err := r.Apply(magmatypes.ZeroServerID, &types.ClientRequest{
		Data: nil,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   75,
			CommittedCount: 0,
			HotEndIndex:    75,
		},
	}, result)
	requireT.EqualValues(4, r.lastLogTerm)

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
		txb(0x03), txb(0x02, 0x00, 0x00),
		txb(0x04),
	))
}

func TestLeaderApplyClientRequestNoTermMarkAllowed(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(4))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
		txb(0x03), txb(0x02, 0x00, 0x00),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.nextIndex = map[magmatypes.ServerID]magmatypes.Index{
		peer1ID: 75,
		peer2ID: 75,
		peer3ID: 75,
		peer4ID: 75,
	}

	result, err := r.Apply(magmatypes.ZeroServerID, &types.ClientRequest{
		Data: []byte{0x01, 0x05},
	})
	requireT.Error(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{}, result)
}

func TestLeaderApplyClientRequestAppend(t *testing.T) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(4))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
		txb(0x03), txb(0x02, 0x00, 0x00),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.nextIndex = map[magmatypes.ServerID]magmatypes.Index{
		peer1ID: 75,
		peer2ID: 75,
		peer3ID: 75,
		peer4ID: 75,
	}

	result, err := r.Apply(magmatypes.ZeroServerID, &types.ClientRequest{
		Data: []byte{0x02, 0x01, 0x00},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   86,
			CommittedCount: 0,
			HotEndIndex:    86,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreHeartbeatTick)
	requireT.Equal(map[magmatypes.ServerID]magmatypes.Index{
		peer1ID: 75,
		peer2ID: 75,
		peer3ID: 75,
		peer4ID: 75,
	}, r.nextIndex)
	requireT.Equal(map[magmatypes.ServerID]magmatypes.Index{
		peer1ID: 0,
		peer2ID: 0,
		peer3ID: 0,
		peer4ID: 0,
	}, r.matchIndex)
	requireT.EqualValues(4, r.lastLogTerm)

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
		txb(0x03), txb(0x02, 0x00, 0x00),
		txb(0x04), txb(0x01, 0x00),
	))
}

func TestLeaderApplyClientRequestAppendMany(t *testing.T) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(4))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
		txb(0x03), txb(0x02, 0x00, 0x00),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.nextIndex = map[magmatypes.ServerID]magmatypes.Index{
		peer1ID: 75,
		peer2ID: 75,
		peer3ID: 75,
		peer4ID: 75,
	}

	result, err := r.Apply(magmatypes.ZeroServerID, &types.ClientRequest{
		Data: []byte{0x03, 0x02, 0x02, 0x03},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   87,
			CommittedCount: 0,
			HotEndIndex:    87,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreHeartbeatTick)
	requireT.Equal(map[magmatypes.ServerID]magmatypes.Index{
		peer1ID: 75,
		peer2ID: 75,
		peer3ID: 75,
		peer4ID: 75,
	}, r.nextIndex)
	requireT.Equal(map[magmatypes.ServerID]magmatypes.Index{
		peer1ID: 0,
		peer2ID: 0,
		peer3ID: 0,
		peer4ID: 0,
	}, r.matchIndex)
	requireT.EqualValues(4, r.lastLogTerm)

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
		txb(0x03), txb(0x02, 0x00, 0x00),
		txb(0x04), txb(0x02, 0x02, 0x03),
	))
}

func TestLeaderApplyActivePeerConnected(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(5))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x01, 0x00),
		txb(0x03), txb(0x01, 0x00),
		txb(0x04), txb(0x01, 0x00),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.nextIndex = map[magmatypes.ServerID]magmatypes.Index{
		peer1ID: 21,
		peer2ID: 42,
		peer3ID: 63,
		peer4ID: 84,
	}
	r.matchIndex = map[magmatypes.ServerID]magmatypes.Index{
		peer1ID: 21,
		peer2ID: 42,
		peer3ID: 63,
		peer4ID: 84,
	}

	result, err := r.Apply(peer1ID, nil)
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(map[magmatypes.ServerID]magmatypes.Index{
		peer1ID: 94,
		peer2ID: 42,
		peer3ID: 63,
		peer4ID: 84,
	}, r.nextIndex)
	requireT.Equal(map[magmatypes.ServerID]magmatypes.Index{
		peer1ID: 0,
		peer2ID: 42,
		peer3ID: 63,
		peer4ID: 84,
	}, r.matchIndex)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   94,
			CommittedCount: 0,
			HotEndIndex:    94,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Message: &types.LogSyncRequest{
			Term:         5,
			NextLogIndex: 94,
			LastLogTerm:  5,
		},
	}, result)
	requireT.Equal(serverID, r.leaderID)
}

func TestLeaderApplyPassivePeerConnected(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(5))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x01, 0x00),
		txb(0x03), txb(0x01, 0x00),
		txb(0x04), txb(0x01, 0x00),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.nextIndex = map[magmatypes.ServerID]magmatypes.Index{
		peer1ID:       21,
		peer2ID:       42,
		peer3ID:       63,
		peer4ID:       84,
		passivePeerID: 10,
	}
	r.matchIndex = map[magmatypes.ServerID]magmatypes.Index{
		peer1ID: 21,
		peer2ID: 42,
		peer3ID: 63,
		peer4ID: 84,
	}

	result, err := r.Apply(passivePeerID, nil)
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(map[magmatypes.ServerID]magmatypes.Index{
		peer1ID:       21,
		peer2ID:       42,
		peer3ID:       63,
		peer4ID:       84,
		passivePeerID: 94,
	}, r.nextIndex)
	requireT.Equal(map[magmatypes.ServerID]magmatypes.Index{
		peer1ID: 21,
		peer2ID: 42,
		peer3ID: 63,
		peer4ID: 84,
	}, r.matchIndex)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   94,
			CommittedCount: 0,
			HotEndIndex:    94,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			passivePeerID,
		},
		Message: &types.LogSyncRequest{
			Term:         5,
			NextLogIndex: 94,
			LastLogTerm:  5,
		},
	}, result)
	requireT.Equal(serverID, r.leaderID)
}

func TestLeaderApplyHeartbeatErrorIfThereIsAnotherLeader(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(5))

	r := newReactor(s)
	_, err := r.transitionToLeader()
	requireT.NoError(err)

	_, err = r.Apply(peer1ID, &types.Heartbeat{
		Term:         5,
		LeaderCommit: 10,
	})
	requireT.Error(err)
}

func TestLeaderApplyHeartbeatChangeToFollowerOnFutureTerm(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(5))

	r := newReactor(s)
	_, err := r.transitionToLeader()
	requireT.NoError(err)

	result, err := r.Apply(peer1ID, &types.Heartbeat{
		Term:         6,
		LeaderCommit: 20,
	})
	requireT.NoError(err)
	requireT.EqualValues(6, r.state.CurrentTerm())
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   10,
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
}
