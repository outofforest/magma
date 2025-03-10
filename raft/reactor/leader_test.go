package reactor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/raft/types"
	magmatypes "github.com/outofforest/magma/types"
)

func TestLeaderSetup(t *testing.T) {
	requireT := require.New(t)
	s, log := newState()
	requireT.NoError(s.SetCurrentTerm(3))
	_, _, err := s.Append([]byte{
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
		Message: &types.AppendEntriesRequest{
			Term:         3,
			NextLogIndex: 13,
			LastLogTerm:  3,
		},
	}, result)
	requireT.EqualValues(11, r.indexTermStarted)
	requireT.Equal(map[magmatypes.ServerID]*syncProgress{
		peer1ID: {
			NextIndex: 11,
		},
		peer2ID: {
			NextIndex: 11,
		},
		peer3ID: {
			NextIndex: 11,
		},
		peer4ID: {
			NextIndex: 11,
		},
	}, r.sync)
	requireT.Empty(r.matchIndex)

	requireT.EqualValues(3, r.lastLogTerm)
	requireT.EqualValues(13, r.nextLogIndex)

	requireT.EqualValues(3, s.CurrentTerm())

	logEqual(requireT, []byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x03,
	}, log)
}

func TestLeaderApplyAppendEntriesRequestTransitionToFollowerOnFutureTerm(t *testing.T) {
	requireT := require.New(t)
	s, log := newState()
	requireT.NoError(s.SetCurrentTerm(3))
	_, _, err := s.Append([]byte{
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
			SyncLogIndex: 0,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(peer1ID, r.leaderID)

	requireT.EqualValues(4, s.CurrentTerm())
	logEqual(requireT, []byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x02, 0x01, 0x00,
	}, log)
}

func TestLeaderApplyAppendEntriesRequestErrorIfThereIsAnotherLeader(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState()
	requireT.NoError(s.SetCurrentTerm(3))
	_, _, err := s.Append([]byte{
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
		LastLogTerm:  3,
	})
	requireT.Error(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{}, result)
}

func TestLeaderApplyAppendEntriesResponseTransitionToFollowerOnFutureTerm(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState()
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
	s, _ := newState()
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

func TestLeaderApplyAppendEntriesResponseErrorIfReplicatedMore(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState()
	requireT.NoError(s.SetCurrentTerm(5))
	_, _, err := s.Append([]byte{
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
	}, r.sync[peer1ID])
	requireT.EqualValues(0, r.matchIndex[peer1ID])
	requireT.Equal(serverID, r.leaderID)
}

func TestLeaderApplyAppendEntriesResponseErrorIfSyncIsAheadLog(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append([]byte{
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
	s, _ := newState()
	requireT.NoError(s.SetCurrentTerm(5))
	_, _, err := s.Append([]byte{
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
	}, r.sync[peer1ID])
	requireT.EqualValues(0, r.matchIndex[peer1ID])
	requireT.Equal(serverID, r.leaderID)
}

func TestLeaderApplyHeartbeatTimeoutAfterHeartbeatTime(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState()
	requireT.NoError(s.SetCurrentTerm(5))
	_, _, err := s.Append([]byte{
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
		},
		peer2ID: {
			NextIndex: 22,
		},
		peer3ID: {
			NextIndex: 22,
		},
		peer4ID: {
			NextIndex: 22,
		},
	}

	result, err := r.Apply(magmatypes.ZeroServerID, types.HeartbeatTick(20))
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
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
		Message: &types.Heartbeat{
			Term:         5,
			LeaderCommit: 0,
		},
	}, result)
	requireT.EqualValues(20, r.heartbeatTick)
}

func TestLeaderApplyHeartbeatTimeoutBeforeHeartbeatTime(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState()
	requireT.NoError(s.SetCurrentTerm(5))
	_, _, err := s.Append([]byte{
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
		},
		peer2ID: {
			NextIndex: 22,
		},
		peer3ID: {
			NextIndex: 22,
		},
		peer4ID: {
			NextIndex: 22,
		},
	}
	r.ignoreHeartbeatTick = 20

	result, err := r.Apply(magmatypes.ZeroServerID, types.HeartbeatTick(20))
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(20, r.heartbeatTick)
}

func TestLeaderApplyClientRequestIgnoreEmptyData(t *testing.T) {
	requireT := require.New(t)
	s, log := newState()
	requireT.NoError(s.SetCurrentTerm(4))
	_, _, err := s.Append([]byte{
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
		},
		peer2ID: {
			NextIndex: 19,
		},
		peer3ID: {
			NextIndex: 19,
		},
		peer4ID: {
			NextIndex: 19,
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
	requireT.EqualValues(19, r.nextLogIndex)
	logEqual(requireT, []byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x03, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x04,
	}, log)
}

func TestLeaderApplyClientRequestNoTermMarkAllowed(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState()
	requireT.NoError(s.SetCurrentTerm(4))
	_, _, err := s.Append([]byte{
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
		},
		peer2ID: {
			NextIndex: 19,
		},
		peer3ID: {
			NextIndex: 19,
		},
		peer4ID: {
			NextIndex: 19,
		},
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
	s, log := newState()
	requireT.NoError(s.SetCurrentTerm(4))
	_, _, err := s.Append([]byte{
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
		},
		peer2ID: {
			NextIndex: 19,
		},
		peer3ID: {
			NextIndex: 19,
		},
		peer4ID: {
			NextIndex: 19,
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
	}, result)
	requireT.EqualValues(1, r.ignoreHeartbeatTick)
	requireT.Equal(map[magmatypes.ServerID]*syncProgress{
		peer1ID: {
			NextIndex: 19,
		},
		peer2ID: {
			NextIndex: 19,
		},
		peer3ID: {
			NextIndex: 19,
		},
		peer4ID: {
			NextIndex: 19,
		},
	}, r.sync)
	requireT.Empty(r.matchIndex)
	requireT.EqualValues(4, r.lastLogTerm)
	requireT.EqualValues(22, r.nextLogIndex)

	logEqual(requireT, []byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x03, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x04, 0x02, 0x01, 0x00,
	}, log)
}

func TestLeaderApplyClientRequestAppendMany(t *testing.T) {
	requireT := require.New(t)
	s, log := newState()
	requireT.NoError(s.SetCurrentTerm(4))
	_, _, err := s.Append([]byte{
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
		},
		peer2ID: {
			NextIndex: 19,
		},
		peer3ID: {
			NextIndex: 19,
		},
		peer4ID: {
			NextIndex: 19,
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
	}, result)
	requireT.EqualValues(1, r.ignoreHeartbeatTick)
	requireT.Equal(map[magmatypes.ServerID]*syncProgress{
		peer1ID: {
			NextIndex: 19,
		},
		peer2ID: {
			NextIndex: 19,
		},
		peer3ID: {
			NextIndex: 19,
		},
		peer4ID: {
			NextIndex: 19,
		},
	}, r.sync)
	requireT.Empty(r.matchIndex)
	requireT.EqualValues(4, r.lastLogTerm)
	requireT.EqualValues(23, r.nextLogIndex)

	logEqual(requireT, []byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x03, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x04, 0x03, 0x02, 0x02, 0x03,
	}, log)
}

func TestLeaderApplyPeerConnected(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState()
	requireT.NoError(s.SetCurrentTerm(5))
	_, _, err := s.Append([]byte{
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
		},
		peer2ID: {
			NextIndex: 10,
		},
		peer3ID: {
			NextIndex: 15,
		},
		peer4ID: {
			NextIndex: 20,
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
		},
		peer2ID: {
			NextIndex: 10,
		},
		peer3ID: {
			NextIndex: 15,
		},
		peer4ID: {
			NextIndex: 20,
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
		Message: &types.AppendEntriesRequest{
			Term:         5,
			NextLogIndex: 22,
			LastLogTerm:  5,
		},
	}, result)
	requireT.Equal(serverID, r.leaderID)
}
