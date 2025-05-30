package reactor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/raft/types"
	magmatypes "github.com/outofforest/magma/types"
)

func TestCandidateSetup(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(1))
	r := newReactor(serverID, s)

	r.role = types.RoleLeader
	r.leaderID = serverID
	r.votedForMe = 10
	r.electionTick = 1
	r.ignoreElectionTick = 0
	r.nextIndex[peer1ID] = 100
	r.matchIndex[peer1ID] = 100

	r.lastTerm = 3
	r.commitInfo = types.CommitInfo{
		NextIndex:      10,
		CommittedCount: 5,
		HotEndIndex:    100,
	}
	result, err := r.transitionToCandidate()
	requireT.NoError(err)

	requireT.Equal(types.RoleCandidate, r.role)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)
	requireT.Equal(1, r.votedForMe)
	requireT.EqualValues(1, r.electionTick)
	requireT.EqualValues(2, r.ignoreElectionTick)
	requireT.Empty(r.nextIndex)
	requireT.Empty(r.matchIndex)
	requireT.Equal(Result{
		Role:     types.RoleCandidate,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextIndex:      10,
			CommittedCount: 5,
			HotEndIndex:    0,
		},
		Channel: ChannelP2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
			peer2ID,
			peer3ID,
			peer4ID,
		},
		Message: &types.VoteRequest{
			Term:      2,
			NextIndex: 10,
			LastTerm:  3,
		},
	}, result)

	requireT.EqualValues(3, r.lastTerm)
	requireT.Equal(types.CommitInfo{
		NextIndex:      10,
		CommittedCount: 5,
		HotEndIndex:    0,
	}, r.commitInfo)

	requireT.EqualValues(2, s.CurrentTerm())

	granted, err := s.VoteFor(peer1ID)
	requireT.NoError(err)
	requireT.False(granted)

	granted, err = s.VoteFor(serverID)
	requireT.NoError(err)
	requireT.True(granted)
}

func TestCandidateApplyLogSyncRequestTransitionToFollowerOnFutureTerm(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(2))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
	), true, true)
	requireT.NoError(err)

	r := newReactor(serverID, s)
	_, err = r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(3, s.CurrentTerm())

	result, err := r.Apply(peer1ID, &types.LogSyncRequest{
		Term:      4,
		NextIndex: 43,
		LastTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			NextIndex:      43,
			CommittedCount: 0,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Message: &types.LogSyncResponse{
			Term:      4,
			NextIndex: 43,
			SyncIndex: 0,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(peer1ID, r.leaderID)

	requireT.EqualValues(4, s.CurrentTerm())

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
	))
}

func TestCandidateApplyVoteRequestTransitionToFollowerOnFutureTerm(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(1))
	r := newReactor(serverID, s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	result, err := r.Apply(peer1ID, &types.VoteRequest{
		Term:      3,
		NextIndex: 0,
		LastTerm:  0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextIndex:      0,
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

func TestCandidateApplyVoteResponseTransitionToFollowerOnFutureTerm(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(1))
	r := newReactor(serverID, s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	result, err := r.Apply(peer1ID, &types.VoteResponse{
		Term:        3,
		VoteGranted: true,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Zero(r.votedForMe)
	requireT.Equal(Result{
		CommitInfo: types.CommitInfo{
			NextIndex:      0,
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)

	requireT.EqualValues(3, s.CurrentTerm())

	granted, err := s.VoteFor(peer2ID)
	requireT.NoError(err)
	requireT.True(granted)
}

func TestCandidateApplyVoteResponseIgnoreVoteFromPastTerm(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(1))
	r := newReactor(serverID, s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	result, err := r.Apply(peer1ID, &types.VoteResponse{
		Term:        1,
		VoteGranted: true,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.Equal(1, r.votedForMe)
	requireT.Equal(Result{
		Role:     types.RoleCandidate,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextIndex:      0,
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)

	requireT.EqualValues(2, s.CurrentTerm())

	granted, err := s.VoteFor(peer1ID)
	requireT.NoError(err)
	requireT.False(granted)
}

func TestCandidateApplyVoteResponseNotGranted(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(1))
	r := newReactor(serverID, s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	result, err := r.Apply(peer1ID, &types.VoteResponse{
		Term:        2,
		VoteGranted: false,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.Equal(1, r.votedForMe)
	requireT.Equal(Result{
		Role:     types.RoleCandidate,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextIndex:      0,
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)

	requireT.EqualValues(2, s.CurrentTerm())
}

func TestCandidateApplyVoteResponseGranted(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(1))
	r := newReactor(serverID, s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	result, err := r.Apply(peer1ID, &types.VoteResponse{
		Term:        2,
		VoteGranted: true,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.Equal(2, r.votedForMe)
	requireT.Equal(Result{
		Role:     types.RoleCandidate,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextIndex:      0,
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)

	requireT.EqualValues(2, s.CurrentTerm())
}

func TestCandidateApplyVoteResponseGrantedInNextTerm(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(1))
	r := newReactor(serverID, s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	result, err := r.Apply(peer1ID, &types.VoteResponse{
		Term:        2,
		VoteGranted: true,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.Equal(2, r.votedForMe)
	requireT.Equal(Result{
		Role:     types.RoleCandidate,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextIndex:      0,
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.EqualValues(2, s.CurrentTerm())

	_, err = r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(3, s.CurrentTerm())

	result, err = r.Apply(peer1ID, &types.VoteResponse{
		Term:        3,
		VoteGranted: true,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.Equal(2, r.votedForMe)
	requireT.Equal(Result{
		Role:     types.RoleCandidate,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)

	requireT.EqualValues(3, s.CurrentTerm())
}

func TestCandidateApplyVoteResponseGrantedFromMajority(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(1))
	r := newReactor(serverID, s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	result, err := r.Apply(peer1ID, &types.VoteResponse{
		Term:        2,
		VoteGranted: true,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.Equal(2, r.votedForMe)
	requireT.Equal(Result{
		Role:     types.RoleCandidate,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextIndex:      0,
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)
	requireT.EqualValues(2, s.CurrentTerm())

	result, err = r.Apply(peer2ID, &types.VoteResponse{
		Term:        2,
		VoteGranted: true,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(3, r.votedForMe)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextIndex:      10,
			CommittedCount: 0,
			HotEndIndex:    10,
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
			Term:      2,
			NextIndex: 10,
			LastTerm:  2,
		},
	}, result)
	requireT.Equal(map[magmatypes.ServerID]magmatypes.Index{
		peer1ID:       10,
		peer2ID:       10,
		peer3ID:       10,
		peer4ID:       10,
		passivePeerID: 10,
	}, r.nextIndex)
	requireT.Equal(map[magmatypes.ServerID]magmatypes.Index{
		peer1ID: 0,
		peer2ID: 0,
		peer3ID: 0,
		peer4ID: 0,
	}, r.matchIndex)
	requireT.EqualValues(2, r.lastTerm)
	requireT.Equal(types.CommitInfo{
		NextIndex:      10,
		CommittedCount: 0,
		HotEndIndex:    10,
	}, r.commitInfo)

	requireT.EqualValues(2, s.CurrentTerm())

	txb := newTxBuilder()
	logEqual(requireT, dir, txb(0x02))
}

func TestCandidateApplyHeartbeatTickDoesNothing(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	r := newReactor(serverID, s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(1, s.CurrentTerm())

	result, err := r.Apply(magmatypes.ZeroServerID, types.HeartbeatTick(1))
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.Equal(Result{
		Role:     types.RoleCandidate,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextIndex:      0,
			CommittedCount: 0,
		},
		Force: true,
	}, result)
}

func TestCandidateApplyPeerConnectedDoesNothing(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	r := newReactor(serverID, s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)
	requireT.EqualValues(1, s.CurrentTerm())

	result, err := r.Apply("PeerID", nil)
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.Equal(Result{
		Role:     types.RoleCandidate,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextIndex:      0,
			CommittedCount: 0,
		},
	}, result)
}
