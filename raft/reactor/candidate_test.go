package reactor

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/raft/state"
	"github.com/outofforest/magma/raft/types"
	magmatypes "github.com/outofforest/magma/types"
)

func TestCandidateSetup(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(1))
	r, ts := newReactor(s)

	r.role = types.RoleLeader
	r.leaderID = serverID
	r.votedForMe = 10
	r.nextIndex[peer1ID] = 100
	r.matchIndex[peer1ID] = 100

	r.lastLogTerm = 3
	r.nextLogIndex = 10
	r.committedCount = 5

	expectedElectionTime := ts.Add(time.Hour)
	msg, err := r.transitionToCandidate()
	requireT.NoError(err)

	requireT.Equal(types.RoleCandidate, r.role)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)
	requireT.EqualValues(1, r.votedForMe)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Empty(r.nextIndex)
	requireT.Empty(r.matchIndex)
	requireT.Equal(&types.VoteRequest{
		Term:         2,
		NextLogIndex: 10,
		LastLogTerm:  3,
	}, msg)

	requireT.EqualValues(3, r.lastLogTerm)
	requireT.EqualValues(10, r.nextLogIndex)
	requireT.EqualValues(5, r.committedCount)

	requireT.EqualValues(2, s.CurrentTerm())

	granted, err := s.VoteFor(peer1ID)
	requireT.NoError(err)
	requireT.False(granted)

	granted, err = s.VoteFor(serverID)
	requireT.NoError(err)
	requireT.True(granted)

	_, _, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.Empty(entries)
}

func TestCandidateApplyAppendEntriesRequestTransitionToFollowerOnFutureTerm(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append(0, 0, 1, []byte{0x00})
	requireT.NoError(err)
	_, _, err = s.Append(1, 1, 2, []byte{0x00, 0x00})
	requireT.NoError(err)

	r, ts := newReactor(s)
	_, err = r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(3, s.CurrentTerm())

	expectedElectionTime := ts.Add(time.Hour)

	msg, err := r.ApplyAppendEntriesRequest(peer1ID, &types.AppendEntriesRequest{
		Term:         4,
		NextLogIndex: 3,
		NextLogTerm:  3,
		LastLogTerm:  2,
		Data:         []byte{0x01, 0x02},
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(&types.AppendEntriesResponse{
		Term:         4,
		NextLogIndex: 5,
	}, msg)
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

func TestCandidateApplyVoteRequestTransitionToFollowerOnFutureTerm(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(1))
	r, ts := newReactor(s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	expectedElectionTime := ts.Add(time.Hour)

	msg, err := r.ApplyVoteRequest(peer1ID, &types.VoteRequest{
		Term:         3,
		NextLogIndex: 0,
		LastLogTerm:  0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(&types.VoteResponse{
		Term:        3,
		VoteGranted: true,
	}, msg)
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

func TestCandidateApplyVoteResponseTransitionToFollowerOnFutureTerm(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(1))
	r, ts := newReactor(s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	expectedElectionTime := ts.Add(time.Hour)

	msg, err := r.ApplyVoteResponse(peer1ID, &types.VoteResponse{
		Term:        3,
		VoteGranted: true,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Zero(r.votedForMe)
	requireT.Nil(msg)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)

	requireT.EqualValues(3, s.CurrentTerm())

	granted, err := s.VoteFor(peer2ID)
	requireT.NoError(err)
	requireT.True(granted)
}

func TestCandidateApplyVoteResponseIgnoreVoteFromPastTerm(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(1))
	r, ts := newReactor(s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	notExpectedElectionTime := ts.Add(time.Hour)

	msg, err := r.ApplyVoteResponse(peer1ID, &types.VoteResponse{
		Term:        1,
		VoteGranted: true,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.EqualValues(1, r.votedForMe)
	requireT.Nil(msg)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)

	requireT.EqualValues(2, s.CurrentTerm())

	granted, err := s.VoteFor(peer1ID)
	requireT.NoError(err)
	requireT.False(granted)
}

func TestCandidateApplyVoteResponseNotGranted(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(1))
	r, ts := newReactor(s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	notExpectedElectionTime := ts.Add(time.Hour)

	msg, err := r.ApplyVoteResponse(peer1ID, &types.VoteResponse{
		Term:        2,
		VoteGranted: false,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.EqualValues(1, r.votedForMe)
	requireT.Nil(msg)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)

	requireT.EqualValues(2, s.CurrentTerm())
}

func TestCandidateApplyVoteResponseGranted(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(1))
	r, ts := newReactor(s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	notExpectedElectionTime := ts.Add(time.Hour)

	msg, err := r.ApplyVoteResponse(peer1ID, &types.VoteResponse{
		Term:        2,
		VoteGranted: true,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.EqualValues(2, r.votedForMe)
	requireT.Nil(msg)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)

	requireT.EqualValues(2, s.CurrentTerm())
}

func TestCandidateApplyVoteResponseGrantedInNextTerm(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(1))
	r, ts := newReactor(s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	notExpectedElectionTime := ts.Add(time.Hour)

	msg, err := r.ApplyVoteResponse(peer1ID, &types.VoteResponse{
		Term:        2,
		VoteGranted: true,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.EqualValues(2, r.votedForMe)
	requireT.Nil(msg)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)
	requireT.EqualValues(2, s.CurrentTerm())

	expectedElectionTime := ts.Add(time.Hour)

	_, err = r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(3, s.CurrentTerm())

	ts.Add(time.Hour)

	msg, err = r.ApplyVoteResponse(peer1ID, &types.VoteResponse{
		Term:        3,
		VoteGranted: true,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.EqualValues(2, r.votedForMe)
	requireT.Empty(msg)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)

	requireT.EqualValues(3, s.CurrentTerm())
}

func TestCandidateApplyVoteResponseGrantedFromMajority(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	requireT.NoError(s.SetCurrentTerm(1))
	r, ts := newReactor(s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(2, s.CurrentTerm())

	notExpectedElectionTime := ts.Add(time.Hour)

	msg, err := r.ApplyVoteResponse(peer1ID, &types.VoteResponse{
		Term:        2,
		VoteGranted: true,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.EqualValues(2, r.votedForMe)
	requireT.Empty(msg)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)
	requireT.EqualValues(2, s.CurrentTerm())

	expectedHeartbeatTime := ts.Add(time.Hour)

	msg, err = r.ApplyVoteResponse(peer2ID, &types.VoteResponse{
		Term:        2,
		VoteGranted: true,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.EqualValues(3, r.votedForMe)
	requireT.Equal(&types.AppendEntriesRequest{
		Term:         2,
		NextLogIndex: 0,
		NextLogTerm:  2,
		LastLogTerm:  0,
		Data:         []byte{0x00},
		LeaderCommit: 0,
	}, msg)
	requireT.Empty(r.nextIndex)
	requireT.Equal(map[magmatypes.ServerID]types.Index{
		serverID: 1,
	}, r.matchIndex)
	requireT.Equal(expectedHeartbeatTime, r.heartBeatTime)
	requireT.EqualValues(2, r.lastLogTerm)
	requireT.EqualValues(1, r.nextLogIndex)

	requireT.EqualValues(2, s.CurrentTerm())
}

func TestCandidateApplyHeartbeatTimeoutDoesNothing(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	r, ts := newReactor(s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.EqualValues(1, s.CurrentTerm())

	heartbeatTimeoutTime := ts.Add(time.Hour)
	notExpectedHeartbeatTime := ts.Add(time.Hour)

	msg, err := r.ApplyHeartbeatTimeout(heartbeatTimeoutTime)
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.NotEqual(notExpectedHeartbeatTime, r.electionTime)
	requireT.Nil(msg)
}

func TestCandidateApplyPeerConnectedDoesNothing(t *testing.T) {
	requireT := require.New(t)
	s := &state.State{}
	r, _ := newReactor(s)
	_, err := r.transitionToCandidate()
	requireT.NoError(err)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)
	requireT.EqualValues(1, s.CurrentTerm())

	msg, err := r.ApplyPeerConnected(magmatypes.ServerID(uuid.New()))
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.Nil(msg)
}
