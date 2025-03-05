package reactor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/raft/types"
	magmatypes "github.com/outofforest/magma/types"
)

func TestLeaderSetup(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	_, _, err := s.Append(0, 0, 1, []byte{0x00})
	requireT.NoError(err)
	_, _, err = s.Append(1, 1, 2, []byte{0x00, 0x00})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(3))
	r, ts := newReactor(s)

	r.role = types.RoleCandidate
	r.leaderID = peer1ID
	r.votedForMe = 10
	r.indexTermStarted = 12
	r.sync[peer1ID] = syncProgress{
		NextIndex: 100,
		End:       100,
	}
	r.matchIndex[peer1ID] = 100

	requireT.EqualValues(2, r.lastLogTerm)
	requireT.EqualValues(3, r.nextLogIndex)
	r.commitInfo = types.CommitInfo{NextLogIndex: 2}

	expectedHeartbeatTime := ts.Add(time.Hour)
	result, err := r.transitionToLeader()
	requireT.NoError(err)

	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(serverID, r.leaderID)
	requireT.EqualValues(10, r.votedForMe)
	requireT.Equal(expectedHeartbeatTime, r.heartBeatTime)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex: 2,
		},
		Recipients: []magmatypes.ServerID{
			peer1ID,
			peer2ID,
			peer3ID,
			peer4ID,
		},
		Messages: []any{
			&types.AppendEntriesRequest{
				Term:         3,
				NextLogIndex: 3,
				NextLogTerm:  3,
				LastLogTerm:  2,
				Data:         []byte{0x00},
				LeaderCommit: 2,
			},
		},
	}, result)
	requireT.EqualValues(3, r.indexTermStarted)
	requireT.Equal(map[magmatypes.ServerID]syncProgress{
		peer1ID: {
			NextIndex: 3,
			End:       0,
		},
		peer2ID: {
			NextIndex: 3,
			End:       0,
		},
		peer3ID: {
			NextIndex: 3,
			End:       0,
		},
		peer4ID: {
			NextIndex: 3,
			End:       0,
		},
	}, r.sync)
	requireT.Equal(map[magmatypes.ServerID]types.Index{
		serverID: 4,
	}, r.matchIndex)

	requireT.EqualValues(3, r.lastLogTerm)
	requireT.EqualValues(4, r.nextLogIndex)

	requireT.EqualValues(3, s.CurrentTerm())

	_, _, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00}, entries)
	_, _, entries, err = s.Entries(1)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00, 0x00}, entries)
	_, _, entries, err = s.Entries(3)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00}, entries)
}

func TestLeaderApplyAppendEntriesRequestTransitionToFollowerOnFutureTerm(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append(0, 0, 1, []byte{0x00})
	requireT.NoError(err)
	_, _, err = s.Append(1, 1, 2, []byte{0x00})
	requireT.NoError(err)

	r, ts := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	expectedElectionTime := ts.Add(time.Hour)

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         4,
		NextLogIndex: 3,
		NextLogTerm:  3,
		LastLogTerm:  2,
		Data:         []byte{0x01, 0x02},
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			NextLogIndex: 0,
		},
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Messages: []any{
			&types.AppendEntriesResponse{
				Term:         4,
				NextLogIndex: 5,
			},
		},
	}, result)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Equal(peer1ID, r.leaderID)

	requireT.EqualValues(4, s.CurrentTerm())
	_, _, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00}, entries)
	_, _, entries, err = s.Entries(1)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00, 0x00}, entries)
	_, _, entries, err = s.Entries(3)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x01, 0x02}, entries)
}

func TestLeaderApplyAppendEntriesRequestErrorIfThereIsAnotherLeader(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append(0, 0, 1, []byte{0x00})
	requireT.NoError(err)
	_, _, err = s.Append(1, 1, 2, []byte{0x00})
	requireT.NoError(err)

	r, _ := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         2,
		NextLogIndex: 3,
		NextLogTerm:  3,
		LastLogTerm:  2,
		Data:         []byte{0x01, 0x02},
		LeaderCommit: 0,
	})
	requireT.Error(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{}, result)
}

func TestLeaderApplyAppendEntriesResponseTransitionToFollowerOnFutureTerm(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(1))
	r, ts := newReactor(s)
	_, err := r.transitionToLeader()
	requireT.NoError(err)

	expectedElectionTime := ts.Add(time.Hour)

	result, err := r.Apply(peer1ID, &types.AppendEntriesResponse{
		Term:         3,
		NextLogIndex: 2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Zero(r.votedForMe)
	requireT.Equal(Result{
		CommitInfo: types.CommitInfo{
			NextLogIndex: 0,
		},
	}, result)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)

	requireT.EqualValues(3, s.CurrentTerm())
}

func TestLeaderApplyVoteRequestTransitionToFollowerOnFutureTerm(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(1))
	r, ts := newReactor(s)
	_, err := r.transitionToLeader()
	requireT.NoError(err)
	requireT.EqualValues(1, s.CurrentTerm())

	expectedElectionTime := ts.Add(time.Hour)

	result, err := r.Apply(peer1ID, &types.VoteRequest{
		Term:         3,
		NextLogIndex: 1,
		LastLogTerm:  1,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextLogIndex: 0,
		},
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
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)

	requireT.EqualValues(3, s.CurrentTerm())

	granted, err := s.VoteFor(peer2ID)
	requireT.NoError(err)
	requireT.False(granted)

	granted, err = s.VoteFor(peer1ID)
	requireT.NoError(err)
	requireT.True(granted)
}

func TestLeaderApplyAppendEntriesResponseSendRemainingLogs(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	_, _, err := s.Append(0, 0, 1, []byte{0x01})
	requireT.NoError(err)
	_, _, err = s.Append(1, 1, 2, []byte{0x02})
	requireT.NoError(err)
	_, _, err = s.Append(2, 2, 3, []byte{0x03})
	requireT.NoError(err)
	_, _, err = s.Append(3, 3, 4, []byte{0x04})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(5))
	r, _ := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.sync[peer1ID] = syncProgress{
		NextIndex: 0,
		End:       0,
	}

	result, err := r.Apply(peer1ID, &types.AppendEntriesResponse{
		Term:         5,
		NextLogIndex: 2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex: 0,
		},
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Messages: []any{
			&types.AppendEntriesRequest{
				Term:         5,
				NextLogIndex: 2,
				NextLogTerm:  3,
				LastLogTerm:  2,
				Data:         []byte{0x03},
			},
		},
	}, result)
	requireT.EqualValues(syncProgress{
		NextIndex: 2,
		End:       3,
	}, r.sync[peer1ID])
	requireT.EqualValues(2, r.matchIndex[peer1ID])
	requireT.Equal(serverID, r.leaderID)
}

func TestLeaderApplyAppendEntriesResponseSendEarlierLogs(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	_, _, err := s.Append(0, 0, 1, []byte{0x01})
	requireT.NoError(err)
	_, _, err = s.Append(1, 1, 2, []byte{0x02})
	requireT.NoError(err)
	_, _, err = s.Append(2, 2, 3, []byte{0x03})
	requireT.NoError(err)
	_, _, err = s.Append(3, 3, 4, []byte{0x04})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(5))
	r, _ := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	result, err := r.Apply(peer1ID, &types.AppendEntriesResponse{
		Term:         5,
		NextLogIndex: 2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex: 0,
		},
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Messages: []any{
			&types.AppendEntriesRequest{
				Term:         5,
				NextLogIndex: 2,
				NextLogTerm:  3,
				LastLogTerm:  2,
				Data:         []byte{0x03},
			},
		},
	}, result)
	requireT.EqualValues(syncProgress{
		NextIndex: 2,
		End:       0,
	}, r.sync[peer1ID])
	requireT.EqualValues(0, r.matchIndex[peer1ID])
	requireT.Equal(serverID, r.leaderID)
}

func TestLeaderApplyAppendEntriesResponseNothingMoreToSend(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	_, _, err := s.Append(0, 0, 1, []byte{0x01})
	requireT.NoError(err)
	_, _, err = s.Append(1, 1, 2, []byte{0x02})
	requireT.NoError(err)
	_, _, err = s.Append(2, 2, 3, []byte{0x03})
	requireT.NoError(err)
	_, _, err = s.Append(3, 3, 4, []byte{0x04})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(5))
	r, _ := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.sync[peer1ID] = syncProgress{
		NextIndex: 0,
		End:       0,
	}

	result, err := r.Apply(peer1ID, &types.AppendEntriesResponse{
		Term:         5,
		NextLogIndex: 5,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex: 0,
		},
	}, result)
	requireT.EqualValues(syncProgress{
		NextIndex: 5,
		End:       5,
	}, r.sync[peer1ID])
	requireT.EqualValues(5, r.matchIndex[peer1ID])
	requireT.Equal(serverID, r.leaderID)
}

func TestLeaderApplyAppendEntriesResponseErrorIfReplicatedMore(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	_, _, err := s.Append(0, 0, 1, []byte{0x01})
	requireT.NoError(err)
	_, _, err = s.Append(1, 1, 2, []byte{0x02})
	requireT.NoError(err)
	_, _, err = s.Append(2, 2, 3, []byte{0x03})
	requireT.NoError(err)
	_, _, err = s.Append(3, 3, 4, []byte{0x04})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(5))
	r, _ := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.sync[peer1ID] = syncProgress{
		NextIndex: 0,
		End:       0,
	}

	result, err := r.Apply(peer1ID, &types.AppendEntriesResponse{
		Term:         5,
		NextLogIndex: 6,
	})
	requireT.Error(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{}, result)
	requireT.EqualValues(syncProgress{
		NextIndex: 0,
		End:       0,
	}, r.sync[peer1ID])
	requireT.EqualValues(0, r.matchIndex[peer1ID])
	requireT.Equal(serverID, r.leaderID)
}

func TestLeaderApplyAppendEntriesResponseIgnorePastTerm(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	_, _, err := s.Append(0, 0, 1, []byte{0x01})
	requireT.NoError(err)
	_, _, err = s.Append(1, 1, 2, []byte{0x02})
	requireT.NoError(err)
	_, _, err = s.Append(2, 2, 3, []byte{0x03})
	requireT.NoError(err)
	_, _, err = s.Append(3, 3, 4, []byte{0x04})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(5))
	r, _ := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.sync[peer1ID] = syncProgress{
		NextIndex: 0,
		End:       0,
	}

	result, err := r.Apply(peer1ID, &types.AppendEntriesResponse{
		Term:         4,
		NextLogIndex: 5,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex: 0,
		},
	}, result)
	requireT.EqualValues(syncProgress{
		NextIndex: 0,
		End:       0,
	}, r.sync[peer1ID])
	requireT.EqualValues(0, r.matchIndex[peer1ID])
	requireT.Equal(serverID, r.leaderID)
}

func TestLeaderApplyAppendEntriesResponseCommitToLast(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	_, _, err := s.Append(0, 0, 1, []byte{0x01})
	requireT.NoError(err)
	_, _, err = s.Append(1, 1, 2, []byte{0x02})
	requireT.NoError(err)
	_, _, err = s.Append(2, 2, 3, []byte{0x03})
	requireT.NoError(err)
	_, _, err = s.Append(3, 3, 4, []byte{0x04})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(5))
	r, _ := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.sync[peer1ID] = syncProgress{
		NextIndex: 0,
		End:       0,
	}

	r.commitInfo = types.CommitInfo{NextLogIndex: 0}
	r.matchIndex[serverID] = 5

	result, err := r.Apply(peer1ID, &types.AppendEntriesResponse{
		Term:         5,
		NextLogIndex: 5,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex: 0,
		},
	}, result)
	requireT.EqualValues(5, r.matchIndex[peer1ID])

	result, err = r.Apply(peer2ID, &types.AppendEntriesResponse{
		Term:         5,
		NextLogIndex: 5,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex: 5,
		},
	}, result)
	requireT.EqualValues(5, r.matchIndex[peer2ID])
}

func TestLeaderApplyAppendEntriesResponseCommitToPrevious(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	_, _, err := s.Append(0, 0, 1, []byte{0x01})
	requireT.NoError(err)
	_, _, err = s.Append(1, 1, 2, []byte{0x02})
	requireT.NoError(err)
	_, _, err = s.Append(2, 2, 3, []byte{0x03})
	requireT.NoError(err)
	_, _, err = s.Append(3, 3, 4, []byte{0x04})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(5))
	r, _ := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)
	_, _, err = s.Append(0, 0, 5, []byte{0x00})
	requireT.NoError(err)

	r.sync[peer1ID] = syncProgress{
		NextIndex: 0,
		End:       0,
	}

	r.commitInfo = types.CommitInfo{NextLogIndex: 0}
	r.matchIndex[serverID] = 5

	result, err := r.Apply(peer1ID, &types.AppendEntriesResponse{
		Term:         5,
		NextLogIndex: 5,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex: 0,
		},
	}, result)
	requireT.EqualValues(5, r.matchIndex[peer1ID])

	result, err = r.Apply(peer2ID, &types.AppendEntriesResponse{
		Term:         5,
		NextLogIndex: 5,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex: 5,
		},
	}, result)
	requireT.EqualValues(5, r.matchIndex[peer2ID])
}

func TestLeaderApplyAppendEntriesResponseCommitToCommonHeight(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	_, _, err := s.Append(0, 0, 1, []byte{0x01})
	requireT.NoError(err)
	_, _, err = s.Append(1, 1, 2, []byte{0x02})
	requireT.NoError(err)
	_, _, err = s.Append(2, 2, 3, []byte{0x03})
	requireT.NoError(err)
	_, _, err = s.Append(3, 3, 4, []byte{0x04})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(5))
	r, _ := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)
	r.lastLogTerm, r.nextLogIndex, err = s.Append(5, 5, 5, []byte{0x00, 0x00})
	requireT.NoError(err)

	r.sync[peer1ID] = syncProgress{
		NextIndex: 0,
		End:       0,
	}

	r.commitInfo = types.CommitInfo{NextLogIndex: 0}
	r.matchIndex[serverID] = 5

	result, err := r.Apply(peer1ID, &types.AppendEntriesResponse{
		Term:         5,
		NextLogIndex: 6,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex: 0,
		},
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Messages: []any{
			&types.AppendEntriesRequest{
				Term:         5,
				NextLogIndex: 6,
				NextLogTerm:  5,
				LastLogTerm:  5,
				Data:         []byte{0x00},
			},
		},
	}, result)
	requireT.EqualValues(6, r.matchIndex[peer1ID])

	result, err = r.Apply(peer2ID, &types.AppendEntriesResponse{
		Term:         5,
		NextLogIndex: 7,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex: 5,
		},
	}, result)
	requireT.EqualValues(7, r.matchIndex[peer2ID])
}

func TestLeaderApplyAppendEntriesResponseNoCommitToOldTerm(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	_, _, err := s.Append(0, 0, 1, []byte{0x01})
	requireT.NoError(err)
	_, _, err = s.Append(1, 1, 2, []byte{0x02})
	requireT.NoError(err)
	_, _, err = s.Append(2, 2, 3, []byte{0x03})
	requireT.NoError(err)
	_, _, err = s.Append(3, 3, 4, []byte{0x04})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(5))
	r, _ := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.sync = map[magmatypes.ServerID]syncProgress{
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
	}

	r.commitInfo = types.CommitInfo{NextLogIndex: 0}
	r.matchIndex[serverID] = 5

	result, err := r.Apply(peer1ID, &types.AppendEntriesResponse{
		Term:         5,
		NextLogIndex: 5,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex: 0,
		},
	}, result)
	requireT.EqualValues(5, r.matchIndex[peer1ID])

	result, err = r.Apply(peer2ID, &types.AppendEntriesResponse{
		Term:         5,
		NextLogIndex: 4,
	})
	requireT.NoError(err)
	requireT.EqualValues(4, r.matchIndex[peer2ID])
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex: 0,
		},
		Recipients: []magmatypes.ServerID{
			peer2ID,
		},
		Messages: []any{
			&types.AppendEntriesRequest{
				Term:         5,
				NextLogIndex: 4,
				NextLogTerm:  5,
				LastLogTerm:  4,
				Data:         []byte{0x00},
				LeaderCommit: 0,
			},
		},
	}, result)
}

func TestLeaderApplyAppendEntriesResponseNoCommitBelowPreviousOne(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	_, _, err := s.Append(0, 0, 1, []byte{0x01})
	requireT.NoError(err)
	_, _, err = s.Append(1, 1, 2, []byte{0x02})
	requireT.NoError(err)
	_, _, err = s.Append(2, 2, 3, []byte{0x03})
	requireT.NoError(err)
	_, _, err = s.Append(3, 3, 4, []byte{0x04})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(5))
	r, _ := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)
	r.lastLogTerm, r.nextLogIndex, err = s.Append(5, 5, 5, []byte{0x00, 0x00})
	requireT.NoError(err)

	r.sync = map[magmatypes.ServerID]syncProgress{
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
	}

	r.commitInfo = types.CommitInfo{NextLogIndex: 7}
	r.matchIndex[serverID] = 7

	result, err := r.Apply(peer1ID, &types.AppendEntriesResponse{
		Term:         5,
		NextLogIndex: 5,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex: 7,
		},
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Messages: []any{
			&types.AppendEntriesRequest{
				Term:         5,
				NextLogIndex: 5,
				NextLogTerm:  5,
				LastLogTerm:  5,
				Data:         []byte{0x00, 0x00},
				LeaderCommit: 7,
			},
		},
	}, result)
	requireT.EqualValues(5, r.matchIndex[peer1ID])

	result, err = r.Apply(peer2ID, &types.AppendEntriesResponse{
		Term:         5,
		NextLogIndex: 6,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex: 7,
		},
		Recipients: []magmatypes.ServerID{
			peer2ID,
		},
		Messages: []any{
			&types.AppendEntriesRequest{
				Term:         5,
				NextLogIndex: 6,
				NextLogTerm:  5,
				LastLogTerm:  5,
				Data:         []byte{0x00},
				LeaderCommit: 7,
			},
		},
	}, result)
	requireT.EqualValues(6, r.matchIndex[peer2ID])
}

func TestLeaderApplyHeartbeatTimeoutAfterHeartbeatTime(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	_, _, err := s.Append(0, 0, 1, []byte{0x01})
	requireT.NoError(err)
	_, _, err = s.Append(1, 1, 2, []byte{0x02})
	requireT.NoError(err)
	_, _, err = s.Append(2, 2, 3, []byte{0x03})
	requireT.NoError(err)
	_, _, err = s.Append(3, 3, 4, []byte{0x04})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(5))
	r, ts := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.sync = map[magmatypes.ServerID]syncProgress{
		peer1ID: {
			NextIndex: 5,
			End:       5,
		},
		peer2ID: {
			NextIndex: 5,
			End:       5,
		},
		peer3ID: {
			NextIndex: 5,
			End:       5,
		},
		peer4ID: {
			NextIndex: 5,
			End:       5,
		},
	}

	r.heartBeatTime = ts.Add(time.Hour)
	heartbeatTimeoutTime := ts.Add(time.Hour)
	expectedHeartbeatTime := ts.Add(time.Hour)

	result, err := r.Apply(magmatypes.ZeroServerID, types.HeartbeatTimeout(heartbeatTimeoutTime))
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex: 0,
		},
		Recipients: []magmatypes.ServerID{
			peer1ID,
			peer2ID,
			peer3ID,
			peer4ID,
		},
		Messages: []any{
			&types.AppendEntriesRequest{
				Term:         5,
				NextLogIndex: 5,
				NextLogTerm:  5,
				LastLogTerm:  5,
				Data:         nil,
			},
		},
	}, result)
	requireT.Equal(expectedHeartbeatTime, r.heartBeatTime)
}

func TestLeaderApplyHeartbeatTimeoutBeforeHeartbeatTime(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	_, _, err := s.Append(0, 0, 1, []byte{0x01})
	requireT.NoError(err)
	_, _, err = s.Append(1, 1, 2, []byte{0x02})
	requireT.NoError(err)
	_, _, err = s.Append(2, 2, 3, []byte{0x03})
	requireT.NoError(err)
	_, _, err = s.Append(3, 3, 4, []byte{0x04})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(5))
	r, ts := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.sync = map[magmatypes.ServerID]syncProgress{
		peer1ID: {
			NextIndex: 5,
			End:       5,
		},
		peer2ID: {
			NextIndex: 5,
			End:       5,
		},
		peer3ID: {
			NextIndex: 5,
			End:       5,
		},
		peer4ID: {
			NextIndex: 5,
			End:       5,
		},
	}

	heartbeatTimeoutTime := ts.Add(time.Hour)
	r.heartBeatTime = ts.Add(time.Hour)
	notExpectedHeartbeatTime := ts.Add(time.Hour)

	result, err := r.Apply(magmatypes.ZeroServerID, types.HeartbeatTimeout(heartbeatTimeoutTime))
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex: 0,
		},
	}, result)
	requireT.NotEqual(notExpectedHeartbeatTime, r.heartBeatTime)
}

func TestLeaderApplyClientRequestAppendAndBroadcast(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	_, _, err := s.Append(0, 0, 1, []byte{0x00})
	requireT.NoError(err)
	_, _, err = s.Append(1, 1, 2, []byte{0x00, 0x00})
	requireT.NoError(err)
	_, _, err = s.Append(3, 2, 3, []byte{0x00, 0x00})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(4))
	r, ts := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.sync = map[magmatypes.ServerID]syncProgress{
		peer1ID: {
			NextIndex: 6,
			End:       6,
		},
		peer2ID: {
			NextIndex: 6,
			End:       6,
		},
		peer3ID: {
			NextIndex: 6,
			End:       6,
		},
		peer4ID: {
			NextIndex: 6,
			End:       6,
		},
	}

	expectedHeartbeatTime := ts.Add(time.Hour)

	result, err := r.Apply(magmatypes.ZeroServerID, &types.ClientRequest{
		Data: []byte{0x01},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex: 0,
		},
		Recipients: []magmatypes.ServerID{
			peer1ID,
			peer2ID,
			peer3ID,
			peer4ID,
		},
		Messages: []any{
			&types.AppendEntriesRequest{
				Term:         4,
				NextLogIndex: 6,
				NextLogTerm:  4,
				LastLogTerm:  4,
				Data:         []byte{0x01, 0x01},
			},
		},
	}, result)
	requireT.Equal(expectedHeartbeatTime, r.heartBeatTime)
	requireT.Equal(map[magmatypes.ServerID]syncProgress{
		peer1ID: {
			NextIndex: 6,
			End:       8,
		},
		peer2ID: {
			NextIndex: 6,
			End:       8,
		},
		peer3ID: {
			NextIndex: 6,
			End:       8,
		},
		peer4ID: {
			NextIndex: 6,
			End:       8,
		},
	}, r.sync)
	requireT.Equal(map[magmatypes.ServerID]types.Index{
		serverID: 8,
	}, r.matchIndex)
	requireT.EqualValues(4, r.lastLogTerm)
	requireT.EqualValues(8, r.nextLogIndex)

	_, _, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00}, entries)
	_, _, entries, err = s.Entries(1)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00, 0x00}, entries)
	_, _, entries, err = s.Entries(3)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00, 0x00}, entries)
	_, _, entries, err = s.Entries(5)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00, 0x01, 0x01}, entries)
}

func TestLeaderApplyClientRequestAppendManyAndBroadcast(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	_, _, err := s.Append(0, 0, 1, []byte{0x00})
	requireT.NoError(err)
	_, _, err = s.Append(1, 1, 2, []byte{0x00, 0x00})
	requireT.NoError(err)
	_, _, err = s.Append(3, 2, 3, []byte{0x00, 0x00})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(4))
	r, ts := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.sync = map[magmatypes.ServerID]syncProgress{
		peer1ID: {
			NextIndex: 6,
			End:       6,
		},
		peer2ID: {
			NextIndex: 6,
			End:       6,
		},
		peer3ID: {
			NextIndex: 6,
			End:       6,
		},
		peer4ID: {
			NextIndex: 6,
			End:       6,
		},
	}

	expectedHeartbeatTime := ts.Add(time.Hour)

	result, err := r.Apply(magmatypes.ZeroServerID, &types.ClientRequest{
		Data: []byte{0x01, 0x02, 0x03},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex: 0,
		},
		Recipients: []magmatypes.ServerID{
			peer1ID,
			peer2ID,
			peer3ID,
			peer4ID,
		},
		Messages: []any{
			&types.AppendEntriesRequest{
				Term:         4,
				NextLogIndex: 6,
				NextLogTerm:  4,
				LastLogTerm:  4,
				Data:         []byte{0x03, 0x01, 0x02, 0x03},
			},
		},
	}, result)
	requireT.Equal(expectedHeartbeatTime, r.heartBeatTime)
	requireT.Equal(map[magmatypes.ServerID]syncProgress{
		peer1ID: {
			NextIndex: 6,
			End:       10,
		},
		peer2ID: {
			NextIndex: 6,
			End:       10,
		},
		peer3ID: {
			NextIndex: 6,
			End:       10,
		},
		peer4ID: {
			NextIndex: 6,
			End:       10,
		},
	}, r.sync)
	requireT.Equal(map[magmatypes.ServerID]types.Index{
		serverID: 10,
	}, r.matchIndex)
	requireT.EqualValues(4, r.lastLogTerm)
	requireT.EqualValues(10, r.nextLogIndex)

	_, _, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00}, entries)
	_, _, entries, err = s.Entries(1)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00, 0x00}, entries)
	_, _, entries, err = s.Entries(3)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00, 0x00}, entries)
	_, _, entries, err = s.Entries(5)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00, 0x03, 0x01, 0x02, 0x03}, entries)
}

func TestLeaderApplyPeerConnected(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	_, _, err := s.Append(0, 0, 1, []byte{0x00})
	requireT.NoError(err)
	_, _, err = s.Append(1, 1, 2, []byte{0x00})
	requireT.NoError(err)
	_, _, err = s.Append(2, 2, 3, []byte{0x00})
	requireT.NoError(err)
	_, _, err = s.Append(3, 3, 4, []byte{0x00})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(5))
	r, _ := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.sync = map[magmatypes.ServerID]syncProgress{
		peer1ID: {
			NextIndex: 1,
			End:       0,
		},
		peer2ID: {
			NextIndex: 2,
			End:       0,
		},
		peer3ID: {
			NextIndex: 3,
			End:       0,
		},
		peer4ID: {
			NextIndex: 4,
			End:       0,
		},
	}
	r.matchIndex = map[magmatypes.ServerID]types.Index{
		peer1ID: 1,
		peer2ID: 2,
		peer3ID: 3,
		peer4ID: 4,
	}

	result, err := r.Apply(peer1ID, nil)
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(map[magmatypes.ServerID]syncProgress{
		peer1ID: {
			NextIndex: 5,
			End:       0,
		},
		peer2ID: {
			NextIndex: 2,
			End:       0,
		},
		peer3ID: {
			NextIndex: 3,
			End:       0,
		},
		peer4ID: {
			NextIndex: 4,
			End:       0,
		},
	}, r.sync)
	requireT.Equal(map[magmatypes.ServerID]types.Index{
		peer2ID: 2,
		peer3ID: 3,
		peer4ID: 4,
	}, r.matchIndex)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex: 0,
		},
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Messages: []any{
			&types.AppendEntriesRequest{
				Term:         5,
				NextLogIndex: 5,
				NextLogTerm:  5,
				LastLogTerm:  5,
				Data:         nil,
			},
		},
	}, result)
	requireT.Equal(serverID, r.leaderID)
}
