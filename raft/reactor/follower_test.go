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

var (
	serverID = magmatypes.ServerID(uuid.New())
	peer1ID  = magmatypes.ServerID(uuid.New())
	peer2ID  = magmatypes.ServerID(uuid.New())
	peer3ID  = magmatypes.ServerID(uuid.New())
	peer4ID  = magmatypes.ServerID(uuid.New())

	peers = []magmatypes.ServerID{peer1ID, peer2ID, peer3ID, peer4ID}
)

func newState() *state.State {
	return state.NewInMemory(1024 * 1024)
}

func newReactor(s *state.State) (*Reactor, TimeAdvancer) {
	timeSource := &TestTimeSource{}
	return New(serverID, len(peers)/2+1, s, timeSource), timeSource
}

func TestFollowerInitialRole(t *testing.T) {
	requireT := require.New(t)
	r, ts := newReactor(newState())
	expectedElectionTime := ts.Add(0)

	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(expectedElectionTime, r.electionTime)
}

func TestFollowerSetup(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	r, ts := newReactor(s)

	r.role = types.RoleCandidate
	r.votedForMe = 2
	r.nextIndex[peer1ID] = 100
	r.matchIndex[peer1ID] = 100

	r.lastLogTerm = 3
	r.nextLogIndex = 10
	r.commitInfo = types.CommitInfo{NextLogIndex: 5}

	expectedElectionTime := ts.Add(time.Hour)
	r.transitionToFollower()

	requireT.Equal(types.RoleFollower, r.role)
	requireT.Zero(r.votedForMe)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Empty(r.nextIndex)
	requireT.Empty(r.matchIndex)

	requireT.EqualValues(3, r.lastLogTerm)
	requireT.EqualValues(10, r.nextLogIndex)
	requireT.Equal(types.CommitInfo{NextLogIndex: 5}, r.commitInfo)

	requireT.EqualValues(0, s.CurrentTerm())

	_, _, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.Nil(entries)
}

func TestFollowerAppendEntriesRequestAppendEntriesToEmptyLog(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	r, ts := newReactor(s)
	expectedElectionTime := ts.Add(time.Hour)

	msg, commitInfo, err := r.ApplyAppendEntriesRequest(peer1ID, &types.AppendEntriesRequest{
		Term:         1,
		NextLogIndex: 0,
		NextLogTerm:  1,
		LastLogTerm:  0,
		Data:         []byte{0x01},
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(&types.AppendEntriesResponse{
		Term:         1,
		NextLogIndex: 1,
	}, msg)
	requireT.Equal(types.CommitInfo{NextLogIndex: 0}, commitInfo)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Equal(peer1ID, r.leaderID)

	requireT.EqualValues(1, s.CurrentTerm())
	_, _, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x01}, entries)
}

func TestFollowerAppendEntriesRequestAppendEntriesToNonEmptyLog(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(1))
	_, _, err := s.Append(0, 0, 1, []byte{0x00, 0x00})
	requireT.NoError(err)

	r, ts := newReactor(s)
	expectedElectionTime := ts.Add(time.Hour)

	msg, commitInfo, err := r.ApplyAppendEntriesRequest(peer1ID, &types.AppendEntriesRequest{
		Term:         1,
		NextLogIndex: 2,
		NextLogTerm:  1,
		LastLogTerm:  1,
		Data:         []byte{0x01, 0x02, 0x03},
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(&types.AppendEntriesResponse{
		Term:         1,
		NextLogIndex: 5,
	}, msg)
	requireT.Equal(types.CommitInfo{NextLogIndex: 0}, commitInfo)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Equal(peer1ID, r.leaderID)

	requireT.EqualValues(1, s.CurrentTerm())
	_, _, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]byte{
		0x00, 0x00,
		0x01,
		0x02,
		0x03,
	}, entries)
}

func TestFollowerAppendEntriesRequestAppendEntriesToNonEmptyLogOnFutureTerm(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(3))
	_, _, err := s.Append(0, 0, 1, []byte{0x00})
	requireT.NoError(err)
	_, _, err = s.Append(1, 1, 2, []byte{0x00, 0x00})
	requireT.NoError(err)

	r, ts := newReactor(s)
	expectedElectionTime := ts.Add(time.Hour)

	msg, commitInfo, err := r.ApplyAppendEntriesRequest(peer1ID, &types.AppendEntriesRequest{
		Term:         3,
		NextLogIndex: 3,
		NextLogTerm:  3,
		LastLogTerm:  2,
		Data:         []byte{0x01, 0x02},
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(&types.AppendEntriesResponse{
		Term:         3,
		NextLogIndex: 5,
	}, msg)
	requireT.Equal(types.CommitInfo{NextLogIndex: 0}, commitInfo)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Equal(peer1ID, r.leaderID)

	requireT.EqualValues(3, s.CurrentTerm())
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

func TestFollowerAppendEntriesRequestReplaceEntries(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append(0, 0, 1, []byte{0x00})
	requireT.NoError(err)
	_, _, err = s.Append(1, 1, 2, []byte{0x00})
	requireT.NoError(err)

	r, ts := newReactor(s)
	expectedElectionTime := ts.Add(time.Hour)

	msg, commitInfo, err := r.ApplyAppendEntriesRequest(peer1ID, &types.AppendEntriesRequest{
		Term:         4,
		NextLogIndex: 2,
		NextLogTerm:  4,
		LastLogTerm:  2,
		Data:         []byte{0x01},
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(&types.AppendEntriesResponse{
		Term:         4,
		NextLogIndex: 3,
	}, msg)
	requireT.Equal(types.CommitInfo{NextLogIndex: 0}, commitInfo)
	requireT.Equal(expectedElectionTime, r.electionTime)

	requireT.EqualValues(4, s.CurrentTerm())
	_, _, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00}, entries)
	_, _, entries, err = s.Entries(1)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00}, entries)
	_, _, entries, err = s.Entries(2)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x01}, entries)
}

func TestFollowerAppendEntriesRequestDiscardEntriesOnTermMismatch(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append(0, 0, 1, []byte{0x00})
	requireT.NoError(err)
	_, _, err = s.Append(1, 1, 2, []byte{0x00, 0x00})
	requireT.NoError(err)

	r, ts := newReactor(s)
	notExpectedElectionTime := ts.Add(time.Hour)

	msg, commitInfo, err := r.ApplyAppendEntriesRequest(peer1ID, &types.AppendEntriesRequest{
		Term:         4,
		NextLogIndex: 3,
		NextLogTerm:  4,
		LastLogTerm:  3,
		Data:         []byte{0x01},
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(&types.AppendEntriesResponse{
		Term:         4,
		NextLogIndex: 1,
	}, msg)
	requireT.Equal(types.CommitInfo{NextLogIndex: 0}, commitInfo)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)

	requireT.EqualValues(4, s.CurrentTerm())
	_, _, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00}, entries)
	_, _, entries, err = s.Entries(1)
	requireT.NoError(err)
	requireT.Empty(entries)
}

func TestFollowerAppendEntriesRequestDiscardEntriesOnTermMismatchTwice(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(3))
	_, _, err := s.Append(0, 0, 1, []byte{0x01})
	requireT.NoError(err)
	_, _, err = s.Append(1, 1, 2, []byte{0x02, 0x02})
	requireT.NoError(err)
	_, _, err = s.Append(3, 2, 3, []byte{0x03, 0x03})
	requireT.NoError(err)

	r, _ := newReactor(s)

	// First time.

	msg, commitInfo, err := r.ApplyAppendEntriesRequest(peer1ID, &types.AppendEntriesRequest{
		Term:         5,
		NextLogIndex: 5,
		NextLogTerm:  5,
		LastLogTerm:  4,
		Data:         []byte{0x05},
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(&types.AppendEntriesResponse{
		Term:         5,
		NextLogIndex: 3,
	}, msg)
	requireT.Equal(types.CommitInfo{NextLogIndex: 0}, commitInfo)
	requireT.EqualValues(3, r.nextLogIndex)
	requireT.EqualValues(2, r.lastLogTerm)

	requireT.EqualValues(5, s.CurrentTerm())
	_, _, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x01}, entries)
	_, _, entries, err = s.Entries(1)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x02, 0x02}, entries)
	_, _, entries, err = s.Entries(3)
	requireT.NoError(err)
	requireT.Empty(entries)

	// Second time.

	msg, commitInfo, err = r.ApplyAppendEntriesRequest(peer1ID, &types.AppendEntriesRequest{
		Term:         6,
		NextLogIndex: 3,
		NextLogTerm:  6,
		LastLogTerm:  3,
		Data:         []byte{0x06},
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(&types.AppendEntriesResponse{
		Term:         6,
		NextLogIndex: 1,
	}, msg)
	requireT.Equal(types.CommitInfo{NextLogIndex: 0}, commitInfo)
	requireT.EqualValues(1, r.nextLogIndex)
	requireT.EqualValues(1, r.lastLogTerm)

	requireT.EqualValues(6, s.CurrentTerm())
	_, _, entries, err = s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x01}, entries)

	_, _, entries, err = s.Entries(1)
	requireT.NoError(err)
	requireT.Empty(entries)
}

func TestFollowerAppendEntriesRequestRejectIfNoPreviousEntry(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append(0, 0, 1, []byte{0x00})
	requireT.NoError(err)
	_, _, err = s.Append(1, 1, 2, []byte{0x00, 0x00})
	requireT.NoError(err)

	r, ts := newReactor(s)
	notExpectedElectionTime := ts.Add(time.Hour)

	msg, commitInfo, err := r.ApplyAppendEntriesRequest(peer1ID, &types.AppendEntriesRequest{
		Term:         4,
		NextLogIndex: 1000,
		NextLogTerm:  4,
		LastLogTerm:  3,
		Data:         []byte{0x01},
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(&types.AppendEntriesResponse{
		Term:         4,
		NextLogIndex: 3,
	}, msg)
	requireT.Equal(types.CommitInfo{NextLogIndex: 0}, commitInfo)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)
	requireT.Equal(peer1ID, r.leaderID)

	requireT.EqualValues(4, s.CurrentTerm())
	_, _, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00}, entries)
	_, _, entries, err = s.Entries(1)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00, 0x00}, entries)
}

func TestFollowerAppendEntriesRequestUpdateCurrentTermOnHeartbeat(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append(0, 0, 1, []byte{0x00})
	requireT.NoError(err)
	_, _, err = s.Append(1, 1, 2, []byte{0x00, 0x00})
	requireT.NoError(err)

	r, ts := newReactor(s)
	expectedElectionTime := ts.Add(time.Hour)

	msg, commitInfo, err := r.ApplyAppendEntriesRequest(peer1ID, &types.AppendEntriesRequest{
		Term:         4,
		NextLogIndex: 3,
		NextLogTerm:  4,
		LastLogTerm:  2,
		Data:         nil,
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(&types.AppendEntriesResponse{
		Term:         4,
		NextLogIndex: 3,
	}, msg)
	requireT.Equal(types.CommitInfo{NextLogIndex: 0}, commitInfo)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Equal(peer1ID, r.leaderID)

	requireT.EqualValues(4, s.CurrentTerm())
	_, _, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00}, entries)
	_, _, entries, err = s.Entries(1)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00, 0x00}, entries)
}

func TestFollowerAppendEntriesRequestDoNothingOnHeartbeat(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append(0, 0, 1, []byte{0x00})
	requireT.NoError(err)
	_, _, err = s.Append(1, 1, 2, []byte{0x00, 0x00})
	requireT.NoError(err)

	r, ts := newReactor(s)
	expectedElectionTime := ts.Add(time.Hour)

	msg, commitInfo, err := r.ApplyAppendEntriesRequest(peer1ID, &types.AppendEntriesRequest{
		Term:         2,
		NextLogIndex: 3,
		NextLogTerm:  2,
		LastLogTerm:  2,
		Data:         nil,
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(&types.AppendEntriesResponse{
		Term:         2,
		NextLogIndex: 3,
	}, msg)
	requireT.Equal(types.CommitInfo{NextLogIndex: 0}, commitInfo)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Equal(peer1ID, r.leaderID)

	requireT.EqualValues(2, s.CurrentTerm())
	_, _, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00}, entries)
	_, _, entries, err = s.Entries(1)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00, 0x00}, entries)
}

func TestFollowerAppendEntriesRequestDoNothingOnLowerTerm(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(4))
	_, _, err := s.Append(0, 0, 1, []byte{0x00})
	requireT.NoError(err)
	_, _, err = s.Append(1, 1, 2, []byte{0x00, 0x00})
	requireT.NoError(err)

	r, ts := newReactor(s)
	notExpectedElectionTime := ts.Add(time.Hour)

	msg, commitInfo, err := r.ApplyAppendEntriesRequest(peer2ID, &types.AppendEntriesRequest{
		Term:         3,
		NextLogIndex: 3,
		NextLogTerm:  3,
		LastLogTerm:  2,
		Data:         []byte{0x01},
		LeaderCommit: 0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(&types.AppendEntriesResponse{
		Term:         4,
		NextLogIndex: 3,
	}, msg)
	requireT.Equal(types.CommitInfo{NextLogIndex: 0}, commitInfo)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)

	requireT.EqualValues(4, s.CurrentTerm())
	_, _, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00}, entries)
	_, _, entries, err = s.Entries(1)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00, 0x00}, entries)
}

func TestFollowerAppendEntriesRequestSetCommittedCountToLeaderCommit(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(1))
	_, _, err := s.Append(0, 0, 1, []byte{0x00, 0x00, 0x00})
	requireT.NoError(err)

	r, _ := newReactor(s)
	r.commitInfo = types.CommitInfo{NextLogIndex: 1}

	msg, commitInfo, err := r.ApplyAppendEntriesRequest(peer1ID, &types.AppendEntriesRequest{
		Term:         1,
		NextLogIndex: 3,
		NextLogTerm:  1,
		LastLogTerm:  1,
		Data:         []byte{0x01},
		LeaderCommit: 2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(&types.AppendEntriesResponse{
		Term:         1,
		NextLogIndex: 4,
	}, msg)
	requireT.Equal(types.CommitInfo{NextLogIndex: 2}, commitInfo)

	requireT.EqualValues(1, s.CurrentTerm())
	_, _, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00, 0x00, 0x00, 0x01}, entries)
}

func TestFollowerAppendEntriesRequestSetCommittedCountToLeaderCommitOnHeartbeat(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(1))
	_, _, err := s.Append(0, 0, 1, []byte{0x00, 0x00, 0x00})
	requireT.NoError(err)

	r, _ := newReactor(s)
	r.commitInfo = types.CommitInfo{NextLogIndex: 1}

	msg, commitInfo, err := r.ApplyAppendEntriesRequest(peer1ID, &types.AppendEntriesRequest{
		Term:         1,
		NextLogIndex: 3,
		NextLogTerm:  1,
		LastLogTerm:  1,
		Data:         nil,
		LeaderCommit: 2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(&types.AppendEntriesResponse{
		Term:         1,
		NextLogIndex: 3,
	}, msg)
	requireT.Equal(types.CommitInfo{NextLogIndex: 2}, commitInfo)

	requireT.EqualValues(1, s.CurrentTerm())
	_, _, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00, 0x00, 0x00}, entries)
}

func TestFollowerAppendEntriesRequestSetCommittedCountToLogLength(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(1))
	_, _, err := s.Append(0, 0, 1, []byte{0x00, 0x00, 0x00})
	requireT.NoError(err)

	r, _ := newReactor(s)
	r.commitInfo = types.CommitInfo{NextLogIndex: 1}

	msg, commitInfo, err := r.ApplyAppendEntriesRequest(peer1ID, &types.AppendEntriesRequest{
		Term:         1,
		NextLogIndex: 3,
		NextLogTerm:  1,
		LastLogTerm:  1,
		Data:         []byte{0x01},
		LeaderCommit: 100,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(&types.AppendEntriesResponse{
		Term:         1,
		NextLogIndex: 4,
	}, msg)
	requireT.Equal(types.CommitInfo{NextLogIndex: 4}, commitInfo)

	requireT.EqualValues(1, s.CurrentTerm())
	_, _, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]byte{
		0x00, 0x00, 0x00,
		0x01,
	}, entries)
}

func TestFollowerAppendEntriesRequestSetCommittedCountToLogLengthOnHeartbeat(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(1))
	_, _, err := s.Append(0, 0, 1, []byte{0x00, 0x00, 0x00})
	requireT.NoError(err)

	r, _ := newReactor(s)
	r.commitInfo = types.CommitInfo{NextLogIndex: 1}

	msg, commitInfo, err := r.ApplyAppendEntriesRequest(peer1ID, &types.AppendEntriesRequest{
		Term:         1,
		NextLogIndex: 3,
		NextLogTerm:  1,
		LastLogTerm:  1,
		Data:         nil,
		LeaderCommit: 100,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(&types.AppendEntriesResponse{
		Term:         1,
		NextLogIndex: 3,
	}, msg)
	requireT.Equal(types.CommitInfo{NextLogIndex: 3}, commitInfo)

	requireT.EqualValues(1, s.CurrentTerm())
	_, _, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00, 0x00, 0x00}, entries)
}

func TestFollowerAppendEntriesRequestDoNotSetCommittedCountToStaleLogLength(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	_, _, err := s.Append(0, 0, 1, []byte{0x00, 0x00, 0x00})
	requireT.NoError(err)
	_, _, err = s.Append(3, 1, 2, []byte{0x00, 0x00})
	requireT.NoError(err)

	r, _ := newReactor(s)
	r.commitInfo = types.CommitInfo{NextLogIndex: 1}

	msg, commitInfo, err := r.ApplyAppendEntriesRequest(peer1ID, &types.AppendEntriesRequest{
		Term:         3,
		NextLogIndex: 5,
		NextLogTerm:  3,
		LastLogTerm:  3,
		Data:         nil,
		LeaderCommit: 3,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(&types.AppendEntriesResponse{
		Term:         3,
		NextLogIndex: 3,
	}, msg)
	requireT.Equal(types.CommitInfo{NextLogIndex: 1}, commitInfo)

	requireT.EqualValues(3, s.CurrentTerm())
	_, _, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00, 0x00, 0x00}, entries)
	_, _, entries, err = s.Entries(3)
	requireT.NoError(err)
	requireT.Empty(entries)
}

func TestFollowerAppendEntriesRequestDoNotSetCommittedCountToStaleCommit(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(1))
	_, _, err := s.Append(0, 0, 1, []byte{0x00, 0x00, 0x00})
	requireT.NoError(err)

	r, _ := newReactor(s)
	r.commitInfo = types.CommitInfo{NextLogIndex: 3}

	msg, commitInfo, err := r.ApplyAppendEntriesRequest(peer1ID, &types.AppendEntriesRequest{
		Term:         1,
		NextLogIndex: 3,
		NextLogTerm:  1,
		LastLogTerm:  1,
		Data:         nil,
		LeaderCommit: 2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(&types.AppendEntriesResponse{
		Term:         1,
		NextLogIndex: 3,
	}, msg)
	requireT.Equal(types.CommitInfo{NextLogIndex: 3}, commitInfo)

	requireT.EqualValues(1, s.CurrentTerm())
	_, _, entries, err := s.Entries(0)
	requireT.NoError(err)
	requireT.EqualValues([]byte{0x00, 0x00, 0x00}, entries)
}

func TestFollowerApplyVoteRequestGrantedOnEmptyLog(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(1))
	r, ts := newReactor(s)
	expectedElectionTime := ts.Add(time.Hour)

	msg, err := r.ApplyVoteRequest(peer1ID, &types.VoteRequest{
		Term:         1,
		NextLogIndex: 0,
		LastLogTerm:  0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(&types.VoteResponse{
		Term:        1,
		VoteGranted: true,
	}, msg)
	requireT.Equal(expectedElectionTime, r.electionTime)
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
	_, _, err := s.Append(0, 0, 1, []byte{0x00, 0x00})
	requireT.NoError(err)
	_, _, err = s.Append(2, 1, 2, []byte{0x00})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(2))
	r, ts := newReactor(s)
	expectedElectionTime := ts.Add(time.Hour)

	msg, err := r.ApplyVoteRequest(peer1ID, &types.VoteRequest{
		Term:         2,
		NextLogIndex: 3,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(&types.VoteResponse{
		Term:        2,
		VoteGranted: true,
	}, msg)
	requireT.Equal(expectedElectionTime, r.electionTime)
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
	_, _, err := s.Append(0, 0, 1, []byte{0x00, 0x00})
	requireT.NoError(err)
	_, _, err = s.Append(2, 1, 2, []byte{0x00, 0x00})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(2))
	r, ts := newReactor(s)
	expectedElectionTime := ts.Add(time.Hour)

	msg, err := r.ApplyVoteRequest(peer1ID, &types.VoteRequest{
		Term:         2,
		NextLogIndex: 4,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(&types.VoteResponse{
		Term:        2,
		VoteGranted: true,
	}, msg)
	requireT.Equal(expectedElectionTime, r.electionTime)
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
	r, ts := newReactor(s)
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

func TestFollowerApplyVoteRequestGrantedTwice(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	_, _, err := s.Append(0, 0, 1, []byte{0x00, 0x00})
	requireT.NoError(err)
	_, _, err = s.Append(2, 1, 2, []byte{0x00})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(2))
	r, ts := newReactor(s)
	expectedElectionTime := ts.Add(time.Hour)

	msg, err := r.ApplyVoteRequest(peer1ID, &types.VoteRequest{
		Term:         2,
		NextLogIndex: 3,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(&types.VoteResponse{
		Term:        2,
		VoteGranted: true,
	}, msg)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.EqualValues(2, s.CurrentTerm())

	expectedElectionTime = ts.Add(time.Hour)

	msg, err = r.ApplyVoteRequest(peer1ID, &types.VoteRequest{
		Term:         2,
		NextLogIndex: 3,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(&types.VoteResponse{
		Term:        2,
		VoteGranted: true,
	}, msg)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)
	requireT.EqualValues(2, s.CurrentTerm())
}

func TestFollowerApplyVoteRequestGrantVoteToOtherCandidateInNextTerm(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	_, _, err := s.Append(0, 0, 1, []byte{0x00, 0x00})
	requireT.NoError(err)
	_, _, err = s.Append(2, 1, 2, []byte{0x00})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(2))
	r, ts := newReactor(s)
	expectedElectionTime := ts.Add(time.Hour)

	msg, err := r.ApplyVoteRequest(peer1ID, &types.VoteRequest{
		Term:         2,
		NextLogIndex: 3,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(&types.VoteResponse{
		Term:        2,
		VoteGranted: true,
	}, msg)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.EqualValues(2, s.CurrentTerm())

	expectedElectionTime = ts.Add(time.Hour)

	msg, err = r.ApplyVoteRequest(peer2ID, &types.VoteRequest{
		Term:         3,
		NextLogIndex: 3,
		LastLogTerm:  2,
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
}

func TestFollowerApplyVoteRequestRejectedOnPastTerm(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(2))
	r, ts := newReactor(s)
	notExpectedElectionTime := ts.Add(time.Hour)

	msg, err := r.ApplyVoteRequest(peer1ID, &types.VoteRequest{
		Term:         1,
		NextLogIndex: 0,
		LastLogTerm:  0,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(&types.VoteResponse{
		Term:        2,
		VoteGranted: false,
	}, msg)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)

	requireT.EqualValues(2, s.CurrentTerm())

	granted, err := s.VoteFor(peer2ID)
	requireT.NoError(err)
	requireT.True(granted)
}

func TestFollowerApplyVoteRequestRejectedOnLowerLastLogTerm(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	_, _, err := s.Append(0, 0, 1, []byte{0x00, 0x00})
	requireT.NoError(err)
	_, _, err = s.Append(2, 1, 2, []byte{0x00})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(2))
	r, ts := newReactor(s)
	notExpectedElectionTime := ts.Add(time.Hour)

	msg, err := r.ApplyVoteRequest(peer1ID, &types.VoteRequest{
		Term:         3,
		NextLogIndex: 3,
		LastLogTerm:  1,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(&types.VoteResponse{
		Term:        3,
		VoteGranted: false,
	}, msg)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)

	requireT.EqualValues(3, s.CurrentTerm())

	granted, err := s.VoteFor(peer2ID)
	requireT.NoError(err)
	requireT.True(granted)
}

func TestFollowerApplyVoteRequestRejectedOnShorterLog(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	_, _, err := s.Append(0, 0, 1, []byte{0x00, 0x00})
	requireT.NoError(err)
	_, _, err = s.Append(2, 1, 2, []byte{0x00, 0x00})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(2))
	r, ts := newReactor(s)
	notExpectedElectionTime := ts.Add(time.Hour)

	msg, err := r.ApplyVoteRequest(peer1ID, &types.VoteRequest{
		Term:         2,
		NextLogIndex: 3,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(&types.VoteResponse{
		Term:        2,
		VoteGranted: false,
	}, msg)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)

	requireT.EqualValues(2, s.CurrentTerm())

	granted, err := s.VoteFor(peer2ID)
	requireT.NoError(err)
	requireT.True(granted)
}

func TestFollowerApplyVoteRequestRejectOtherCandidates(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	_, _, err := s.Append(0, 0, 1, []byte{0x00, 0x00})
	requireT.NoError(err)
	_, _, err = s.Append(2, 1, 2, []byte{0x00})
	requireT.NoError(err)
	requireT.NoError(s.SetCurrentTerm(2))
	r, ts := newReactor(s)
	expectedElectionTime := ts.Add(time.Hour)

	msg, err := r.ApplyVoteRequest(peer1ID, &types.VoteRequest{
		Term:         2,
		NextLogIndex: 3,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(&types.VoteResponse{
		Term:        2,
		VoteGranted: true,
	}, msg)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.EqualValues(2, s.CurrentTerm())

	ts.Add(time.Hour)

	msg, err = r.ApplyVoteRequest(peer2ID, &types.VoteRequest{
		Term:         2,
		NextLogIndex: 3,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(&types.VoteResponse{
		Term:        2,
		VoteGranted: false,
	}, msg)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)
	requireT.EqualValues(2, s.CurrentTerm())
}

func TestFollowerApplyElectionTimeoutAfterElectionTime(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	r, ts := newReactor(s)

	electionTimeoutTime := ts.Add(time.Hour)
	expectedElectionTime := ts.Add(time.Hour)

	msg, commitInfo, err := r.ApplyElectionTimeout(electionTimeoutTime)
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.Equal(expectedElectionTime, r.electionTime)
	requireT.EqualValues(1, s.CurrentTerm())
	requireT.EqualValues(1, r.votedForMe)
	requireT.Equal(&types.VoteRequest{
		Term:         1,
		NextLogIndex: 0,
		LastLogTerm:  0,
	}, msg)
	requireT.Equal(types.CommitInfo{NextLogIndex: 0}, commitInfo)

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
	r, ts := newReactor(s)

	electionTimeoutTime := ts.Add(time.Hour)
	r.electionTime = ts.Add(time.Hour)
	notExpectedElectionTime := ts.Add(time.Hour)

	msg, commitInfo, err := r.ApplyElectionTimeout(electionTimeoutTime)
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.NotEqual(notExpectedElectionTime, r.electionTime)
	requireT.EqualValues(0, s.CurrentTerm())
	requireT.EqualValues(0, r.votedForMe)
	requireT.Nil(msg)
	requireT.Equal(types.CommitInfo{NextLogIndex: 0}, commitInfo)

	granted, err := s.VoteFor(peer1ID)
	requireT.NoError(err)
	requireT.True(granted)
}

func TestFollowerApplyHeartbeatTimeoutDoesNothing(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	r, ts := newReactor(s)

	heartbeatTimeoutTime := ts.Add(time.Hour)
	notExpectedHeartbeatTime := ts.Add(time.Hour)

	msg, err := r.ApplyHeartbeatTimeout(heartbeatTimeoutTime)
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.NotEqual(notExpectedHeartbeatTime, r.electionTime)
	requireT.Nil(msg)
}

func TestFollowerApplyPeerConnectedDoesNothing(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	r, _ := newReactor(s)

	msg, err := r.ApplyPeerConnected(magmatypes.ServerID(uuid.New()))
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Nil(msg)
}

func TestFollowerApplyClientRequestIgnoreIfNotLeader(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(1))
	r, ts := newReactor(s)

	notExpectedHeartbeatTime := ts.Add(time.Hour)

	msg, commitInfo, err := r.ApplyClientRequest(&types.ClientRequest{
		Data: []byte{0x01},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Nil(msg)
	requireT.Equal(types.CommitInfo{NextLogIndex: 0}, commitInfo)
	requireT.NotEqual(notExpectedHeartbeatTime, r.heartBeatTime)
	requireT.Zero(r.nextLogIndex)
	requireT.Empty(r.nextLogIndex)
	requireT.Empty(r.matchIndex)
}
