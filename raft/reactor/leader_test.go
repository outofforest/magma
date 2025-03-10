package reactor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/raft/types"
	magmatypes "github.com/outofforest/magma/types"
)

func TestLeaderSetup(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(3))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)

	r.role = types.RoleCandidate
	r.leaderID = peer1ID
	r.votedForMe = 10
	r.heartbeatTick = 1
	r.ignoreHeartbeatTick = 0
	r.indexTermStarted = 12
	r.sync[peer1ID] = &syncProgress{
		NextIndex: 100,
		End:       100,
	}
	r.matchIndex[peer1ID] = 100

	requireT.EqualValues(2, r.lastLogTerm)
	requireT.EqualValues(11, r.nextLogIndex)
	r.commitInfo = types.CommitInfo{CommittedCount: 2}

	result, err := r.transitionToLeader()
	requireT.NoError(err)

	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(serverID, r.leaderID)
	requireT.EqualValues(10, r.votedForMe)
	requireT.EqualValues(1, r.heartbeatTick)
	requireT.EqualValues(2, r.ignoreHeartbeatTick)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 2,
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
				Term:         3,
				NextLogIndex: 11,
				NextLogTerm:  3,
				LastLogTerm:  2,
				Data:         []byte{0x01, 0x03},
				LeaderCommit: 2,
			},
		},
	}, result)
	requireT.EqualValues(11, r.indexTermStarted)
	requireT.Equal(map[magmatypes.ServerID]*syncProgress{
		peer1ID: {
			NextIndex: 11,
			End:       0,
		},
		peer2ID: {
			NextIndex: 11,
			End:       0,
		},
		peer3ID: {
			NextIndex: 11,
			End:       0,
		},
		peer4ID: {
			NextIndex: 11,
			End:       0,
		},
	}, r.sync)
	requireT.Empty(r.matchIndex)

	requireT.EqualValues(3, r.lastLogTerm)
	requireT.EqualValues(13, r.nextLogIndex)

	requireT.EqualValues(3, s.CurrentTerm())

	_, _, entries, err := s.Entries(0, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x03,
	}, entries)
}

func TestLeaderApplyAppendEntriesRequestTransitionToFollowerOnFutureTerm(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(3))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x02, 0x01, 0x00,
	}, true)
	requireT.NoError(err)

	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)
	requireT.EqualValues(3, s.CurrentTerm())

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         4,
		NextLogIndex: 10,
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
		0x01, 0x02, 0x02, 0x01, 0x00,
		0x01, 0x04, 0x02, 0x01, 0x00,
	}, entries)
}

func TestLeaderApplyAppendEntriesRequestErrorIfThereIsAnotherLeader(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(3))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x02, 0x01, 0x00,
	}, true)
	requireT.NoError(err)

	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)
	requireT.EqualValues(3, s.CurrentTerm())

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         3,
		NextLogIndex: 12,
		NextLogTerm:  3,
		LastLogTerm:  3,
		Data:         []byte{0x02, 0x01, 0x00},
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
	r := newReactor(s)
	_, err := r.transitionToLeader()
	requireT.NoError(err)

	result, err := r.Apply(peer1ID, &types.AppendEntriesResponse{
		Term:         3,
		NextLogIndex: 2,
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
}

func TestLeaderApplyVoteRequestTransitionToFollowerOnFutureTerm(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(1))
	r := newReactor(s)
	_, err := r.transitionToLeader()
	requireT.NoError(err)
	requireT.EqualValues(1, s.CurrentTerm())

	result, err := r.Apply(peer1ID, &types.VoteRequest{
		Term:         3,
		NextLogIndex: 2,
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

func TestLeaderApplyAppendEntriesResponseSendRemainingLogs(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(5))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x02, 0x01, 0x02,
		0x01, 0x03, 0x02, 0x01, 0x03,
		0x01, 0x04, 0x02, 0x01, 0x04,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.sync[peer1ID] = &syncProgress{
		NextIndex: 0,
		End:       0,
	}

	result, err := r.Apply(peer1ID, &types.AppendEntriesResponse{
		Term:         5,
		NextLogIndex: 10,
		SyncLogIndex: 5,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Messages: []any{
			&types.AppendEntriesRequest{
				Term:         5,
				NextLogIndex: 10,
				NextLogTerm:  3,
				LastLogTerm:  2,
				Data: []byte{
					0x01, 0x03, 0x02, 0x01, 0x03,
					0x01, 0x04, 0x02, 0x01, 0x04,
					0x01, 0x05,
				},
			},
		},
	}, result)
	requireT.EqualValues(&syncProgress{
		NextIndex: 10,
		End:       22,
	}, r.sync[peer1ID])
	requireT.EqualValues(5, r.matchIndex[peer1ID])
	requireT.Equal(serverID, r.leaderID)
}

func TestLeaderApplyAppendEntriesResponseSplitLog(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01,
		0x0b, 0x0a, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
		0x0b, 0x0a, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.sync[peer1ID] = &syncProgress{
		NextIndex: 0,
		End:       0,
	}

	result, err := r.Apply(peer1ID, &types.AppendEntriesResponse{
		Term:         2,
		NextLogIndex: 2,
		SyncLogIndex: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Messages: []any{
			&types.AppendEntriesRequest{
				Term:         2,
				NextLogIndex: 2,
				NextLogTerm:  1,
				LastLogTerm:  1,
				Data: []byte{
					0x0b, 0x0a, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
					0x0b, 0x0a, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12,
				},
			},
			&types.AppendEntriesRequest{
				Term:         2,
				NextLogIndex: 26,
				NextLogTerm:  2,
				LastLogTerm:  1,
				Data:         []byte{0x01, 0x02},
			},
		},
	}, result)
	requireT.EqualValues(&syncProgress{
		NextIndex: 2,
		End:       28,
	}, r.sync[peer1ID])
}

func TestLeaderApplyAppendEntriesResponseSendRemainingWireCapacity(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01,
		0x0d, 0x0c, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
		0x0d, 0x0c, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
		0x0d, 0x0c, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
		0x0d, 0x0c, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.sync[peer1ID] = &syncProgress{
		NextIndex: 0,
		End:       2,
	}

	result, err := r.Apply(peer1ID, &types.AppendEntriesResponse{
		Term:         2,
		NextLogIndex: 2,
		SyncLogIndex: 1,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Messages: []any{
			&types.AppendEntriesRequest{
				Term:         2,
				NextLogIndex: 2,
				NextLogTerm:  1,
				LastLogTerm:  1,
				Data:         []byte{0x0d, 0x0c, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c},
			},
			&types.AppendEntriesRequest{
				Term:         2,
				NextLogIndex: 16,
				NextLogTerm:  1,
				LastLogTerm:  1,
				Data:         []byte{0x0d, 0x0c, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c},
			},
			&types.AppendEntriesRequest{
				Term:         2,
				NextLogIndex: 30,
				NextLogTerm:  1,
				LastLogTerm:  1,
				Data:         []byte{0x0d, 0x0c, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c},
			},
		},
	}, result)
	requireT.EqualValues(&syncProgress{
		NextIndex: 2,
		End:       44,
	}, r.sync[peer1ID])
}

func TestLeaderApplyAppendEntriesResponseSendRemainingRest(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01,
		0x0d, 0x0c, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
		0x0d, 0x0c, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
		0x09, 0x08, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.sync[peer1ID] = &syncProgress{
		NextIndex: 2,
		End:       16,
	}

	result, err := r.Apply(peer1ID, &types.AppendEntriesResponse{
		Term:         2,
		NextLogIndex: 2,
		SyncLogIndex: 1,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Messages: []any{
			&types.AppendEntriesRequest{
				Term:         2,
				NextLogIndex: 16,
				NextLogTerm:  1,
				LastLogTerm:  1,
				Data: []byte{
					0x0d, 0x0c, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
					0x09, 0x08, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
				},
			},
			&types.AppendEntriesRequest{
				Term:         2,
				NextLogIndex: 40,
				NextLogTerm:  2,
				LastLogTerm:  1,
				Data:         []byte{0x01, 0x02},
			},
		},
	}, result)
	requireT.EqualValues(&syncProgress{
		NextIndex: 2,
		End:       42,
	}, r.sync[peer1ID])
}

func TestLeaderApplyAppendEntriesResponseCantSendMore(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01,
		0x0d, 0x0c, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
		0x0d, 0x0c, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
		0x0d, 0x0c, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
		0x0d, 0x0c, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.sync[peer1ID] = &syncProgress{
		NextIndex: 0,
		End:       44,
	}

	result, err := r.Apply(peer1ID, &types.AppendEntriesResponse{
		Term:         2,
		NextLogIndex: 2,
		SyncLogIndex: 1,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(&syncProgress{
		NextIndex: 2,
		End:       44,
	}, r.sync[peer1ID])
}

func TestLeaderApplyAppendEntriesResponseSendEarlierLogs(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(5))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x02, 0x01, 0x02,
		0x01, 0x03, 0x02, 0x01, 0x03,
		0x01, 0x04, 0x02, 0x01, 0x04,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	result, err := r.Apply(peer1ID, &types.AppendEntriesResponse{
		Term:         5,
		NextLogIndex: 10,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Messages: []any{
			&types.AppendEntriesRequest{
				Term:         5,
				NextLogIndex: 10,
				NextLogTerm:  3,
				LastLogTerm:  2,
				Data:         nil,
			},
		},
	}, result)
	requireT.EqualValues(&syncProgress{
		NextIndex: 10,
		End:       0,
	}, r.sync[peer1ID])
	requireT.EqualValues(0, r.matchIndex[peer1ID])
	requireT.Equal(serverID, r.leaderID)
}

func TestLeaderApplyAppendEntriesResponseNothingMoreToSend(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(5))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x02, 0x01, 0x02,
		0x01, 0x03, 0x02, 0x01, 0x03,
		0x01, 0x04, 0x02, 0x01, 0x04,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.sync[peer1ID] = &syncProgress{
		NextIndex: 0,
		End:       0,
	}

	result, err := r.Apply(peer1ID, &types.AppendEntriesResponse{
		Term:         5,
		NextLogIndex: 22,
		SyncLogIndex: 3,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(&syncProgress{
		NextIndex: 22,
		End:       22,
	}, r.sync[peer1ID])
	requireT.EqualValues(3, r.matchIndex[peer1ID])
	requireT.Equal(serverID, r.leaderID)
}

func TestLeaderApplyAppendEntriesResponseErrorIfReplicatedMore(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(5))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x02, 0x01, 0x02,
		0x01, 0x03, 0x02, 0x01, 0x03,
		0x01, 0x04, 0x02, 0x01, 0x04,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.sync[peer1ID] = &syncProgress{
		NextIndex: 0,
		End:       0,
	}

	result, err := r.Apply(peer1ID, &types.AppendEntriesResponse{
		Term:         5,
		NextLogIndex: 23,
	})
	requireT.Error(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{}, result)
	requireT.EqualValues(&syncProgress{
		NextIndex: 0,
		End:       0,
	}, r.sync[peer1ID])
	requireT.EqualValues(0, r.matchIndex[peer1ID])
	requireT.Equal(serverID, r.leaderID)
}

func TestLeaderApplyAppendEntriesResponseErrorIfSyncIsAheadLog(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x04, 0x03, 0x01, 0x02, 0x03,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	result, err := r.Apply(peer1ID, &types.AppendEntriesResponse{
		Term:         2,
		NextLogIndex: 7,
		SyncLogIndex: 8,
	})
	requireT.Error(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{}, result)
}

func TestLeaderApplyAppendEntriesResponseIgnorePastTerm(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(5))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x02, 0x01, 0x02,
		0x01, 0x03, 0x02, 0x01, 0x03,
		0x01, 0x04, 0x02, 0x01, 0x04,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.sync[peer1ID] = &syncProgress{
		NextIndex: 0,
		End:       0,
	}

	result, err := r.Apply(peer1ID, &types.AppendEntriesResponse{
		Term:         4,
		NextLogIndex: 20,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(&syncProgress{
		NextIndex: 0,
		End:       0,
	}, r.sync[peer1ID])
	requireT.EqualValues(0, r.matchIndex[peer1ID])
	requireT.Equal(serverID, r.leaderID)
}

func TestLeaderApplyAppendEntriesResponseCommitToLast(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(5))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x02, 0x01, 0x02,
		0x01, 0x03, 0x02, 0x01, 0x03,
		0x01, 0x04, 0x02, 0x01, 0x04,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.sync[peer1ID] = &syncProgress{
		NextIndex: 0,
		End:       0,
	}

	r.commitInfo = types.CommitInfo{CommittedCount: 0}
	r.matchIndex[serverID] = 22

	result, err := r.Apply(peer1ID, &types.AppendEntriesResponse{
		Term:         5,
		NextLogIndex: 22,
		SyncLogIndex: 22,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(22, r.matchIndex[peer1ID])

	result, err = r.Apply(peer2ID, &types.AppendEntriesResponse{
		Term:         5,
		NextLogIndex: 22,
		SyncLogIndex: 22,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 22,
		},
	}, result)
	requireT.EqualValues(22, r.matchIndex[peer2ID])
}

func TestLeaderApplyAppendEntriesResponseCommitToPrevious(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(5))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x02, 0x01, 0x02,
		0x01, 0x03, 0x02, 0x01, 0x03,
		0x01, 0x04, 0x02, 0x01, 0x04,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)
	_, _, err = s.Append(22, 5, []byte{
		0x02, 0x01, 0x00,
	}, true)
	requireT.NoError(err)

	r.sync[peer1ID] = &syncProgress{
		NextIndex: 0,
		End:       0,
	}

	r.commitInfo = types.CommitInfo{CommittedCount: 0}
	r.matchIndex[serverID] = 22

	result, err := r.Apply(peer1ID, &types.AppendEntriesResponse{
		Term:         5,
		NextLogIndex: 22,
		SyncLogIndex: 22,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(22, r.matchIndex[peer1ID])

	result, err = r.Apply(peer2ID, &types.AppendEntriesResponse{
		Term:         5,
		NextLogIndex: 22,
		SyncLogIndex: 22,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 22,
		},
	}, result)
	requireT.EqualValues(22, r.matchIndex[peer2ID])
}

func TestLeaderApplyAppendEntriesResponseCommitToCommonHeight(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(5))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x02, 0x01, 0x02,
		0x01, 0x03, 0x02, 0x01, 0x03,
		0x01, 0x04, 0x02, 0x01, 0x04,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)
	r.lastLogTerm, r.nextLogIndex, err = s.Append(22, 5, []byte{
		0x03, 0x02, 0x00, 0x00,
		0x03, 0x02, 0x00, 0x00,
		0x03, 0x02, 0x00, 0x00,
	}, true)
	requireT.NoError(err)

	r.sync[peer1ID] = &syncProgress{
		NextIndex: 0,
		End:       0,
	}

	r.commitInfo = types.CommitInfo{CommittedCount: 0}
	r.matchIndex[serverID] = 34

	result, err := r.Apply(peer1ID, &types.AppendEntriesResponse{
		Term:         5,
		NextLogIndex: 34,
		SyncLogIndex: 26,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(26, r.matchIndex[peer1ID])

	result, err = r.Apply(peer2ID, &types.AppendEntriesResponse{
		Term:         5,
		NextLogIndex: 34,
		SyncLogIndex: 22,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 22,
		},
	}, result)
	requireT.EqualValues(22, r.matchIndex[peer2ID])
}

func TestLeaderApplyAppendEntriesResponseNoCommitToOldTerm(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(5))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x02, 0x01, 0x02,
		0x01, 0x03, 0x02, 0x01, 0x03,
		0x01, 0x04, 0x02, 0x01, 0x04,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.sync = map[magmatypes.ServerID]*syncProgress{
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

	r.commitInfo = types.CommitInfo{CommittedCount: 0}
	r.matchIndex[serverID] = 22

	result, err := r.Apply(peer1ID, &types.AppendEntriesResponse{
		Term:         5,
		NextLogIndex: 22,
		SyncLogIndex: 22,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(22, r.matchIndex[peer1ID])

	result, err = r.Apply(peer2ID, &types.AppendEntriesResponse{
		Term:         5,
		NextLogIndex: 22,
		SyncLogIndex: 20,
	})
	requireT.NoError(err)
	requireT.EqualValues(20, r.matchIndex[peer2ID])
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
	}, result)
}

func TestLeaderApplyAppendEntriesResponseNoCommitBelowPreviousOne(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(5))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x02, 0x01, 0x02,
		0x01, 0x03, 0x02, 0x01, 0x03,
		0x01, 0x04, 0x02, 0x01, 0x04,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)
	r.lastLogTerm, r.nextLogIndex, err = s.Append(22, 5, []byte{
		0x03, 0x02, 0x00, 0x00,
		0x03, 0x02, 0x00, 0x00,
	}, true)
	requireT.NoError(err)

	r.sync = map[magmatypes.ServerID]*syncProgress{
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

	r.commitInfo = types.CommitInfo{CommittedCount: 30}
	r.matchIndex[serverID] = 30

	result, err := r.Apply(peer1ID, &types.AppendEntriesResponse{
		Term:         5,
		NextLogIndex: 30,
		SyncLogIndex: 26,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 30,
		},
	}, result)
	requireT.EqualValues(26, r.matchIndex[peer1ID])

	result, err = r.Apply(peer2ID, &types.AppendEntriesResponse{
		Term:         5,
		NextLogIndex: 30,
		SyncLogIndex: 26,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 30,
		},
	}, result)
	requireT.EqualValues(26, r.matchIndex[peer2ID])
}

func TestLeaderApplyHeartbeatTimeoutAfterHeartbeatTime(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(5))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x02, 0x01, 0x02,
		0x01, 0x03, 0x02, 0x01, 0x03,
		0x01, 0x04, 0x02, 0x01, 0x04,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.sync = map[magmatypes.ServerID]*syncProgress{
		peer1ID: {
			NextIndex: 22,
			End:       22,
		},
		peer2ID: {
			NextIndex: 22,
			End:       22,
		},
		peer3ID: {
			NextIndex: 22,
			End:       22,
		},
		peer4ID: {
			NextIndex: 22,
			End:       22,
		},
	}

	result, err := r.Apply(magmatypes.ZeroServerID, types.HeartbeatTick(2))
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
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
				Term:         5,
				NextLogIndex: 22,
				NextLogTerm:  5,
				LastLogTerm:  5,
				Data:         nil,
			},
		},
	}, result)
	requireT.EqualValues(2, r.heartbeatTick)
}

func TestLeaderApplyHeartbeatTimeoutBeforeHeartbeatTime(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(5))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x02, 0x01, 0x02,
		0x01, 0x03, 0x02, 0x01, 0x03,
		0x01, 0x04, 0x02, 0x01, 0x04,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.sync = map[magmatypes.ServerID]*syncProgress{
		peer1ID: {
			NextIndex: 22,
			End:       22,
		},
		peer2ID: {
			NextIndex: 22,
			End:       22,
		},
		peer3ID: {
			NextIndex: 22,
			End:       22,
		},
		peer4ID: {
			NextIndex: 22,
			End:       22,
		},
	}

	result, err := r.Apply(magmatypes.ZeroServerID, types.HeartbeatTick(1))
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(1, r.heartbeatTick)
}

func TestLeaderApplySyncTickSyncedBelowCommitted(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(5))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x02, 0x01, 0x02,
		0x01, 0x03, 0x02, 0x01, 0x03,
		0x01, 0x04, 0x02, 0x01, 0x04,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.commitInfo = types.CommitInfo{
		CommittedCount: 50,
	}
	r.nextLogIndex = 3
	r.matchIndex = map[magmatypes.ServerID]types.Index{
		peer1ID: 50,
		peer2ID: 50,
		peer3ID: 50,
	}

	result, err := r.Apply(magmatypes.ZeroServerID, types.SyncTick{})
	requireT.Error(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(map[magmatypes.ServerID]types.Index{
		peer1ID: 50,
		peer2ID: 50,
		peer3ID: 50,
	}, r.matchIndex)
	requireT.Equal(Result{}, result)
}

func TestLeaderApplySyncTickOnMinority(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(5))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x02, 0x01, 0x02,
		0x01, 0x03, 0x02, 0x01, 0x03,
		0x01, 0x04, 0x02, 0x01, 0x04,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)
	r.lastLogTerm, r.nextLogIndex, err = s.Append(22, 5, []byte{
		0x04, 0x03, 0x05, 0x06, 0x07,
	}, true)
	requireT.NoError(err)

	r.commitInfo = types.CommitInfo{
		CommittedCount: 5,
	}
	r.nextLogIndex = 27
	r.matchIndex = map[magmatypes.ServerID]types.Index{
		peer1ID: 22,
	}

	result, err := r.Apply(magmatypes.ZeroServerID, types.SyncTick{})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(map[magmatypes.ServerID]types.Index{
		serverID: 27,
		peer1ID:  22,
	}, r.matchIndex)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 5,
		},
	}, result)
}

func TestLeaderApplySyncTickOnMajority(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(5))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x02, 0x01, 0x02,
		0x01, 0x03, 0x02, 0x01, 0x03,
		0x01, 0x04, 0x02, 0x01, 0x04,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)
	r.lastLogTerm, r.nextLogIndex, err = s.Append(22, 5, []byte{
		0x04, 0x03, 0x05, 0x06, 0x07,
	}, true)
	requireT.NoError(err)

	r.commitInfo = types.CommitInfo{
		CommittedCount: 5,
	}
	r.nextLogIndex = 22
	r.matchIndex = map[magmatypes.ServerID]types.Index{
		peer1ID: 27,
		peer2ID: 22,
	}

	result, err := r.Apply(magmatypes.ZeroServerID, types.SyncTick{})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(map[magmatypes.ServerID]types.Index{
		serverID: 27,
		peer1ID:  27,
		peer2ID:  22,
	}, r.matchIndex)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 22,
		},
	}, result)
}

func TestLeaderApplySyncTickOnMajoritySendHeartbeat(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(5))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x02, 0x01, 0x02,
		0x01, 0x03, 0x02, 0x01, 0x03,
		0x01, 0x04, 0x02, 0x01, 0x04,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)
	r.lastLogTerm, r.nextLogIndex, err = s.Append(22, 5, []byte{
		0x04, 0x03, 0x05, 0x06, 0x07,
	}, true)
	requireT.NoError(err)

	r.commitInfo = types.CommitInfo{
		CommittedCount: 1,
	}
	r.matchIndex = map[magmatypes.ServerID]types.Index{
		peer1ID: 27,
		peer2ID: 22,
	}
	r.sync[peer1ID] = &syncProgress{
		NextIndex: r.nextLogIndex,
		End:       r.nextLogIndex,
	}
	r.sync[peer3ID] = &syncProgress{
		NextIndex: r.nextLogIndex,
		End:       r.nextLogIndex,
	}

	result, err := r.Apply(magmatypes.ZeroServerID, types.SyncTick{})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(map[magmatypes.ServerID]types.Index{
		serverID: 27,
		peer1ID:  27,
		peer2ID:  22,
	}, r.matchIndex)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 22,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
			peer3ID,
		},
		Messages: []any{
			&types.AppendEntriesRequest{
				Term:         5,
				NextLogIndex: 27,
				NextLogTerm:  5,
				LastLogTerm:  5,
				Data:         nil,
				LeaderCommit: 22,
			},
		},
	}, result)
}

func TestLeaderApplySyncTickCommitAtFirstTermIndex(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(5))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x02, 0x01, 0x02,
		0x01, 0x03, 0x02, 0x01, 0x03,
		0x01, 0x04, 0x02, 0x01, 0x04,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)
	r.lastLogTerm, r.nextLogIndex, err = s.Append(22, 5, []byte{
		0x04, 0x03, 0x05, 0x06, 0x07,
	}, true)
	requireT.NoError(err)

	r.commitInfo = types.CommitInfo{
		CommittedCount: 1,
	}
	r.matchIndex = map[magmatypes.ServerID]types.Index{
		peer1ID: 22,
		peer2ID: 27,
	}

	result, err := r.Apply(magmatypes.ZeroServerID, types.SyncTick{})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(map[magmatypes.ServerID]types.Index{
		serverID: 27,
		peer1ID:  22,
		peer2ID:  27,
	}, r.matchIndex)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 22,
		},
	}, result)
}

func TestLeaderApplySyncTickCommitBelowCurrentOne(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(5))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x01,
		0x01, 0x02, 0x02, 0x01, 0x02,
		0x01, 0x03, 0x02, 0x01, 0x03,
		0x01, 0x04, 0x02, 0x01, 0x04,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)
	r.lastLogTerm, r.nextLogIndex, err = s.Append(22, 5, []byte{
		0x04, 0x03, 0x05, 0x06, 0x07,
	}, true)
	requireT.NoError(err)

	r.commitInfo = types.CommitInfo{
		CommittedCount: 27,
	}
	r.matchIndex = map[magmatypes.ServerID]types.Index{
		peer1ID: 22,
		peer2ID: 22,
	}

	result, err := r.Apply(magmatypes.ZeroServerID, types.SyncTick{})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(map[magmatypes.ServerID]types.Index{
		serverID: 27,
		peer1ID:  22,
		peer2ID:  22,
	}, r.matchIndex)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 27,
		},
	}, result)
}

func TestLeaderApplyClientRequestIgnoreEmptyData(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(4))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x03, 0x03, 0x02, 0x00, 0x00,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.sync = map[magmatypes.ServerID]*syncProgress{
		peer1ID: {
			NextIndex: 19,
			End:       19,
		},
		peer2ID: {
			NextIndex: 19,
			End:       19,
		},
		peer3ID: {
			NextIndex: 19,
			End:       19,
		},
		peer4ID: {
			NextIndex: 19,
			End:       19,
		},
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
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(4, r.lastLogTerm)
	requireT.EqualValues(19, r.nextLogIndex)

	_, _, entries, err := s.Entries(0, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x03, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x04,
	}, entries)
}

func TestLeaderApplyClientRequestNoTermMarkAllowed(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(4))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x03, 0x03, 0x02, 0x00, 0x00,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.sync = map[magmatypes.ServerID]*syncProgress{
		peer1ID: {
			NextIndex: 19,
			End:       19,
		},
		peer2ID: {
			NextIndex: 19,
			End:       19,
		},
		peer3ID: {
			NextIndex: 19,
			End:       19,
		},
		peer4ID: {
			NextIndex: 19,
			End:       19,
		},
	}

	result, err := r.Apply(magmatypes.ZeroServerID, &types.ClientRequest{
		Data: []byte{0x01, 0x05},
	})
	requireT.Error(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{}, result)
}

func TestLeaderApplyClientRequestAppendAndBroadcast(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(4))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x03, 0x03, 0x02, 0x00, 0x00,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.sync = map[magmatypes.ServerID]*syncProgress{
		peer1ID: {
			NextIndex: 19,
			End:       19,
		},
		peer2ID: {
			NextIndex: 19,
			End:       19,
		},
		peer3ID: {
			NextIndex: 19,
			End:       19,
		},
		peer4ID: {
			NextIndex: 19,
			End:       19,
		},
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
				Term:         4,
				NextLogIndex: 19,
				NextLogTerm:  4,
				LastLogTerm:  4,
				Data:         []byte{0x02, 0x01, 0x00},
			},
		},
	}, result)
	requireT.EqualValues(1, r.ignoreHeartbeatTick)
	requireT.Equal(map[magmatypes.ServerID]*syncProgress{
		peer1ID: {
			NextIndex: 19,
			End:       22,
		},
		peer2ID: {
			NextIndex: 19,
			End:       22,
		},
		peer3ID: {
			NextIndex: 19,
			End:       22,
		},
		peer4ID: {
			NextIndex: 19,
			End:       22,
		},
	}, r.sync)
	requireT.Empty(r.matchIndex)
	requireT.EqualValues(4, r.lastLogTerm)
	requireT.EqualValues(22, r.nextLogIndex)

	_, _, entries, err := s.Entries(0, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x03, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x04, 0x02, 0x01, 0x00,
	}, entries)
}

func TestLeaderApplyClientRequestAppendManyAndBroadcast(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(4))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x03, 0x03, 0x02, 0x00, 0x00,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.sync = map[magmatypes.ServerID]*syncProgress{
		peer1ID: {
			NextIndex: 19,
			End:       19,
		},
		peer2ID: {
			NextIndex: 19,
			End:       19,
		},
		peer3ID: {
			NextIndex: 19,
			End:       19,
		},
		peer4ID: {
			NextIndex: 19,
			End:       19,
		},
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
				Term:         4,
				NextLogIndex: 19,
				NextLogTerm:  4,
				LastLogTerm:  4,
				Data:         []byte{0x03, 0x02, 0x02, 0x03},
			},
		},
	}, result)
	requireT.EqualValues(1, r.ignoreHeartbeatTick)
	requireT.Equal(map[magmatypes.ServerID]*syncProgress{
		peer1ID: {
			NextIndex: 19,
			End:       23,
		},
		peer2ID: {
			NextIndex: 19,
			End:       23,
		},
		peer3ID: {
			NextIndex: 19,
			End:       23,
		},
		peer4ID: {
			NextIndex: 19,
			End:       23,
		},
	}, r.sync)
	requireT.Empty(r.matchIndex)
	requireT.EqualValues(4, r.lastLogTerm)
	requireT.EqualValues(23, r.nextLogIndex)

	_, _, entries, err := s.Entries(0, maxReadLogSize)
	requireT.NoError(err)
	requireT.EqualValues([]byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x03, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x04, 0x03, 0x02, 0x02, 0x03,
	}, entries)
}

func TestLeaderApplyPeerConnected(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(5))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x02, 0x01, 0x00,
		0x01, 0x03, 0x02, 0x01, 0x00,
		0x01, 0x04, 0x02, 0x01, 0x00,
	}, true)
	requireT.NoError(err)
	r := newReactor(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	r.sync = map[magmatypes.ServerID]*syncProgress{
		peer1ID: {
			NextIndex: 5,
			End:       0,
		},
		peer2ID: {
			NextIndex: 10,
			End:       0,
		},
		peer3ID: {
			NextIndex: 15,
			End:       0,
		},
		peer4ID: {
			NextIndex: 20,
			End:       0,
		},
	}
	r.matchIndex = map[magmatypes.ServerID]types.Index{
		peer1ID: 5,
		peer2ID: 10,
		peer3ID: 15,
		peer4ID: 20,
	}

	result, err := r.Apply(peer1ID, nil)
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(map[magmatypes.ServerID]*syncProgress{
		peer1ID: {
			NextIndex: 22,
			End:       0,
		},
		peer2ID: {
			NextIndex: 10,
			End:       0,
		},
		peer3ID: {
			NextIndex: 15,
			End:       0,
		},
		peer4ID: {
			NextIndex: 20,
			End:       0,
		},
	}, r.sync)
	requireT.Equal(map[magmatypes.ServerID]types.Index{
		peer2ID: 10,
		peer3ID: 15,
		peer4ID: 20,
	}, r.matchIndex)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Messages: []any{
			&types.AppendEntriesRequest{
				Term:         5,
				NextLogIndex: 22,
				NextLogTerm:  5,
				LastLogTerm:  5,
				Data:         nil,
			},
		},
	}, result)
	requireT.Equal(serverID, r.leaderID)
}
