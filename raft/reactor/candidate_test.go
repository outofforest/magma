package reactor

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/raft/types"
	magmatypes "github.com/outofforest/magma/types"
)

func TestCandidateSetup(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(1))
	r := newReactor(s)

	r.role = types.RoleLeader
	r.leaderID = serverID
	r.votedForMe = 10
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
	result, err := r.transitionToCandidate()
	requireT.NoError(err)

	requireT.Equal(types.RoleCandidate, r.role)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)
	requireT.EqualValues(1, r.votedForMe)
	requireT.EqualValues(1, r.electionTick)
	requireT.EqualValues(2, r.ignoreElectionTick)
	requireT.Empty(r.sync)
	requireT.Empty(r.matchIndex)
	requireT.Equal(Result{
		Role:     types.RoleCandidate,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 5,
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
				Term:         2,
				NextLogIndex: 10,
				LastLogTerm:  3,
			},
		},
	}, result)

	requireT.EqualValues(3, r.lastLogTerm)
	requireT.EqualValues(10, r.nextLogIndex)

	requireT.EqualValues(2, s.CurrentTerm())

	granted, err := s.VoteFor(peer1ID)
	requireT.NoError(err)
	requireT.False(granted)

	granted, err = s.VoteFor(serverID)
	requireT.NoError(err)
	requireT.True(granted)

	_, _, entries, err := s.Entries(0, maxReadLogSize)
	requireT.NoError(err)
	requireT.Empty(entries)
}

func TestCandidateApplyAppendEntriesRequestTransitionToFollowerOnFutureTerm(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	}, true)
	requireT.NoError(err)

	r := newReactor(s)
	_, err = r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(3, s.CurrentTerm())

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         4,
		NextLogIndex: 11,
		NextLogTerm:  3,
		LastLogTerm:  2,
		Data:         []byte{0x01, 0x04, 0x02, 0x01, 0x00},
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
		0x01, 0x04, 0x02, 0x01, 0x00,
	}, entries)
}

func TestCandidateApplyVoteRequestTransitionToFollowerOnFutureTerm(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(1))
	r := newReactor(s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

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

func TestCandidateApplyVoteResponseTransitionToFollowerOnFutureTerm(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(1))
	r := newReactor(s)
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
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(1))
	r := newReactor(s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	result, err := r.Apply(peer1ID, &types.VoteResponse{
		Term:        1,
		VoteGranted: true,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.EqualValues(1, r.votedForMe)
	requireT.Equal(Result{
		Role:     types.RoleCandidate,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
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
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(1))
	r := newReactor(s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	result, err := r.Apply(peer1ID, &types.VoteResponse{
		Term:        2,
		VoteGranted: false,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.EqualValues(1, r.votedForMe)
	requireT.Equal(Result{
		Role:     types.RoleCandidate,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)

	requireT.EqualValues(2, s.CurrentTerm())
}

func TestCandidateApplyVoteResponseGranted(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(1))
	r := newReactor(s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	result, err := r.Apply(peer1ID, &types.VoteResponse{
		Term:        2,
		VoteGranted: true,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.EqualValues(2, r.votedForMe)
	requireT.Equal(Result{
		Role:     types.RoleCandidate,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)

	requireT.EqualValues(2, s.CurrentTerm())
}

func TestCandidateApplyVoteResponseGrantedInNextTerm(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(1))
	r := newReactor(s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	result, err := r.Apply(peer1ID, &types.VoteResponse{
		Term:        2,
		VoteGranted: true,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.EqualValues(2, r.votedForMe)
	requireT.Equal(Result{
		Role:     types.RoleCandidate,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
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
	requireT.EqualValues(2, r.votedForMe)
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
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(1))
	r := newReactor(s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	result, err := r.Apply(peer1ID, &types.VoteResponse{
		Term:        2,
		VoteGranted: true,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.EqualValues(2, r.votedForMe)
	requireT.Equal(Result{
		Role:     types.RoleCandidate,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
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
	requireT.EqualValues(3, r.votedForMe)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
			peer2ID,
			peer3ID,
			peer4ID,
		},
		Messages: []any{
			&types.AppendEntriesRequest{
				Term:         2,
				NextLogIndex: 0,
				NextLogTerm:  2,
				LastLogTerm:  0,
				Data:         []byte{0x01, 0x02},
				LeaderCommit: 0,
			},
		},
	}, result)
	requireT.Equal(map[magmatypes.ServerID]*syncProgress{
		peer1ID: {
			NextIndex: 0,
			End:       0,
		},
		peer2ID: {
			NextIndex: 0,
			End:       0,
		},
		peer3ID: {
			NextIndex: 0,
			End:       0,
		},
		peer4ID: {
			NextIndex: 0,
			End:       0,
		},
	}, r.sync)
	requireT.Empty(r.matchIndex)
	requireT.EqualValues(1, r.ignoreHeartbeatTick)
	requireT.EqualValues(2, r.lastLogTerm)
	requireT.EqualValues(2, r.nextLogIndex)

	requireT.EqualValues(2, s.CurrentTerm())
}

func TestCandidateApplyHeartbeatTimeoutDoesNothing(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	r := newReactor(s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(1, s.CurrentTerm())

	result, err := r.Apply(magmatypes.ZeroServerID, types.HeartbeatTick(1))
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.EqualValues(1, r.heartbeatTick)
	requireT.Equal(Result{
		Role:     types.RoleCandidate,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
	}, result)
}

func TestCandidateApplyPeerConnectedDoesNothing(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	r := newReactor(s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)
	requireT.EqualValues(1, s.CurrentTerm())

	result, err := r.Apply(magmatypes.ServerID(uuid.New()), nil)
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.Equal(Result{
		Role:     types.RoleCandidate,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
	}, result)
}
