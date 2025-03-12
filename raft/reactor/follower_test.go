package reactor

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"

	"github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/state"
	"github.com/outofforest/magma/state/events"
	"github.com/outofforest/magma/state/repository"
	magmatypes "github.com/outofforest/magma/types"
	"github.com/outofforest/varuint64"
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

//nolint:unparam
func newState(t *testing.T, dir string) (*state.State, string) {
	if dir == "" {
		dir = t.TempDir()
	}
	repo, err := repository.Open(filepath.Join(dir, "repo"), uint64(os.Getpagesize()))
	require.NoError(t, err)
	em, err := events.Open(filepath.Join(dir, "events"))
	require.NoError(t, err)
	s, sCloser, err := state.New(repo, em)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, sCloser())
	})
	return s, dir
}

func newReactor(s *state.State) *Reactor {
	return New(config, s)
}

func logEqual(requireT *require.Assertions, dir string, expectedLog []byte) {
	repo, err := repository.Open(filepath.Join(dir, "repo"), uint64(os.Getpagesize()))
	requireT.NoError(err)
	it := repo.Iterator(0)
	var index types.Index
	buf := bytes.NewBuffer(nil)
	for index < types.Index(len(expectedLog)) {
		file, err := it.Next()
		requireT.NoError(err)
		requireT.NotNil(file)
		limit := file.ValidUntil() - index
		if limit > types.Index(len(expectedLog))-index {
			limit = types.Index(len(expectedLog)) - index
		}
		n, err := io.Copy(buf, io.LimitReader(file.Reader(), int64(limit)))
		requireT.NoError(err)
		index += types.Index(n)
		requireT.NoError(file.Close())
	}
	requireT.Equal(expectedLog, buf.Bytes())
}

func newTxBuilder() func(data ...byte) []byte {
	var seed uint64
	return func(data ...byte) []byte {
		totalSize := uint64(len(data)) + 8
		totalSize += varuint64.Size(totalSize)

		tx := make([]byte, totalSize)
		n := varuint64.Put(tx, uint64(len(data))+8)
		copy(tx[n:], data)

		checksum := xxh3.HashSeed(tx[:len(tx)-8], seed)
		binary.LittleEndian.PutUint64(tx[len(tx)-8:], checksum)
		seed = checksum
		return tx
	}
}

func txs(txs ...[]byte) []byte {
	buf := bytes.NewBuffer(nil)
	for _, tx := range txs {
		buf.Write(tx)
	}
	return buf.Bytes()
}

func TestFollowerInitialRole(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	r := newReactor(s)

	requireT.Equal(types.RoleFollower, r.role)
	requireT.EqualValues(0, r.electionTick)
	requireT.EqualValues(0, r.ignoreElectionTick)
}

func TestFollowerSetup(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	r := newReactor(s)

	r.role = types.RoleCandidate
	r.leaderID = serverID
	r.votedForMe = 2
	r.electionTick = 1
	r.ignoreElectionTick = 0
	r.nextIndex[peer1ID] = 100
	r.matchIndex[peer1ID] = 100

	r.lastLogTerm = 3
	r.commitInfo = types.CommitInfo{
		NextLogIndex:   10,
		CommittedCount: 5,
	}

	r.transitionToFollower()

	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)
	requireT.Zero(r.votedForMe)
	requireT.EqualValues(1, r.electionTick)
	requireT.EqualValues(2, r.ignoreElectionTick)
	requireT.Empty(r.nextIndex)
	requireT.Empty(r.matchIndex)

	requireT.EqualValues(3, r.lastLogTerm)
	requireT.Equal(types.CommitInfo{
		NextLogIndex:   10,
		CommittedCount: 5,
	}, r.commitInfo)

	requireT.EqualValues(0, s.CurrentTerm())
}

func TestFollowerAppendTxAppendToEmptyLog(t *testing.T) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(1))
	r := newReactor(s)
	r.leaderID = peer1ID

	txb := newTxBuilder()
	tx := txs(
		txb(0x01),
		txb(0x01, 0x00),
	)
	result, err := r.Apply(peer1ID, tx)
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   types.Index(len(tx)),
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(peer1ID, r.leaderID)

	logEqual(requireT, dir, tx)
}

func TestFollowerAppendTxAppendToNonEmptyLog(t *testing.T) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(1))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01),
		txb(0x02, 0x00, 0x00),
	), true, true)
	requireT.NoError(err)

	r := newReactor(s)
	r.leaderID = peer1ID

	result, err := r.Apply(peer1ID, txb(0x03, 0x01, 0x02, 0x03))
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   35,
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(peer1ID, r.leaderID)

	requireT.EqualValues(1, s.CurrentTerm())

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01),
		txb(0x02, 0x00, 0x00),
		txb(0x03, 0x01, 0x02, 0x03),
	))
}

func TestFollowerAppendEntriesRequestOnFutureTerm(t *testing.T) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(2))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
	), true, true)
	requireT.NoError(err)

	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         3,
		NextLogIndex: 43,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   43,
			CommittedCount: 0,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Message: &types.AppendEntriesResponse{
			Term:         3,
			NextLogIndex: 43,
			SyncLogIndex: 0,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(peer1ID, r.leaderID)

	requireT.EqualValues(3, s.CurrentTerm())

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
	))
}

func TestFollowerAppendEntriesRequestDiscardEntries(t *testing.T) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(3))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x01),
		txb(0x02), txb(0x01, 0x02),
		txb(0x03), txb(0x01, 0x03),
	), true, true)
	requireT.NoError(err)

	r := newReactor(s)
	r.syncedCount = 21

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
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
		Message: &types.AppendEntriesResponse{
			Term:         4,
			NextLogIndex: 42,
			SyncLogIndex: 21,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.EqualValues(21, r.syncedCount)

	requireT.EqualValues(4, s.CurrentTerm())

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x01),
		txb(0x02), txb(0x01, 0x02),
	))
}

func TestFollowerAppendEntriesRequestDiscardAtSynced(t *testing.T) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(3))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x01),
		txb(0x02), txb(0x01, 0x02),
		txb(0x03), txb(0x01, 0x03),
	), true, true)
	requireT.NoError(err)

	r := newReactor(s)
	r.syncedCount = 42

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
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
		Message: &types.AppendEntriesResponse{
			Term:         4,
			NextLogIndex: 42,
			SyncLogIndex: 42,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.EqualValues(42, r.syncedCount)

	requireT.EqualValues(4, s.CurrentTerm())

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x01),
		txb(0x02), txb(0x01, 0x02),
	))
}

func TestFollowerAppendEntriesRequestDiscardOnTermMismatch(t *testing.T) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(2))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
	), true, true)
	requireT.NoError(err)

	r := newReactor(s)
	r.syncedCount = 43

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         4,
		NextLogIndex: 43,
		LastLogTerm:  3,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   21,
			CommittedCount: 0,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Message: &types.AppendEntriesResponse{
			Term:         4,
			NextLogIndex: 21,
			SyncLogIndex: 21,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.EqualValues(21, r.syncedCount)

	requireT.EqualValues(4, s.CurrentTerm())

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x00),
	))
}

func TestFollowerAppendEntriesRequestDiscardOnTermMismatchTwice(t *testing.T) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(3))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x01),
		txb(0x02), txb(0x02, 0x02, 0x02),
		txb(0x03), txb(0x02, 0x03, 0x03),
	), true, true)
	requireT.NoError(err)

	r := newReactor(s)

	// First time.

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         5,
		NextLogIndex: 62,
		LastLogTerm:  4,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   43,
			CommittedCount: 0,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Message: &types.AppendEntriesResponse{
			Term:         5,
			NextLogIndex: 43,
		},
	}, result)
	requireT.EqualValues(2, r.lastLogTerm)

	requireT.EqualValues(5, s.CurrentTerm())

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x01),
		txb(0x02), txb(0x02, 0x02, 0x02),
	))

	// Second time.

	result, err = r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         6,
		NextLogIndex: 22,
		LastLogTerm:  3,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   21,
			CommittedCount: 0,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Message: &types.AppendEntriesResponse{
			Term:         6,
			NextLogIndex: 21,
		},
	}, result)
	requireT.EqualValues(1, r.lastLogTerm)

	requireT.EqualValues(6, s.CurrentTerm())

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x01),
	))
}

func TestFollowerAppendEntriesRequestRejectIfNoPreviousEntry(t *testing.T) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(2))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
	), true, true)
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
			NextLogIndex:   43,
			CommittedCount: 0,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Message: &types.AppendEntriesResponse{
			Term:         4,
			NextLogIndex: 43,
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

func TestFollowerAppendEntriesRequestSendResponseIfLastLogTermIsLower(t *testing.T) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(2))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         4,
		NextLogIndex: 43,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   43,
			CommittedCount: 0,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Message: &types.AppendEntriesResponse{
			Term:         4,
			NextLogIndex: 43,
			SyncLogIndex: 0,
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

func TestFollowerAppendEntriesRequestSendResponseIfNextLogIndexIsLower(t *testing.T) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(2))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.AppendEntriesRequest{
		Term:         4,
		NextLogIndex: 44,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   43,
			CommittedCount: 0,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Message: &types.AppendEntriesResponse{
			Term:         4,
			NextLogIndex: 43,
			SyncLogIndex: 0,
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

func TestFollowerAppendEntriesRequestDoNothingOnLowerTerm(t *testing.T) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(4))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)

	result, err := r.Apply(peer2ID, &types.AppendEntriesRequest{
		Term:         3,
		NextLogIndex: 43,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   43,
			CommittedCount: 0,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer2ID,
		},
		Message: &types.AppendEntriesResponse{
			Term:         4,
			NextLogIndex: 43,
		},
	}, result)
	requireT.Zero(r.ignoreElectionTick)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)

	requireT.EqualValues(4, s.CurrentTerm())

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
	))
}

func TestFollowerApplyVoteRequestGrantedOnEmptyLog(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
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
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(2))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x02, 0x00, 0x00),
		txb(0x02), txb(0x01, 0x00),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.VoteRequest{
		Term:         2,
		NextLogIndex: 43,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   43,
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
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(2))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x02, 0x00, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.VoteRequest{
		Term:         2,
		NextLogIndex: 50,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   44,
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
	s, _ := newState(t, "")
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
			NextLogIndex:   0,
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
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(2))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x02, 0x00, 0x00),
		txb(0x02), txb(0x01, 0x00),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.VoteRequest{
		Term:         2,
		NextLogIndex: 43,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   43,
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
		NextLogIndex: 43,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   43,
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
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(2))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x02, 0x00, 0x00),
		txb(0x02), txb(0x01, 0x00),
	), true, true)
	requireT.NoError(err)
	requireT.NoError(err)
	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.VoteRequest{
		Term:         2,
		NextLogIndex: 43,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   43,
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
		NextLogIndex: 43,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   43,
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
	s, _ := newState(t, "")
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
			NextLogIndex:   0,
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
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(2))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x02, 0x00, 0x00),
		txb(0x02), txb(0x01, 0x00),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.VoteRequest{
		Term:         3,
		NextLogIndex: 43,
		LastLogTerm:  1,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   43,
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
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(2))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x02, 0x00, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.VoteRequest{
		Term:         2,
		NextLogIndex: 43,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   44,
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
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(2))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x02, 0x00, 0x00),
		txb(0x02), txb(0x01, 0x00),
	), true, true)
	requireT.NoError(err)
	r := newReactor(s)

	result, err := r.Apply(peer1ID, &types.VoteRequest{
		Term:         2,
		NextLogIndex: 43,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   43,
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
		NextLogIndex: 43,
		LastLogTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   43,
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
	s, _ := newState(t, "")
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
			NextLogIndex:   0,
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
	s, _ := newState(t, "")
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
			NextLogIndex:   0,
			CommittedCount: 0,
		},
	}, result)

	requireT.NoError(s.SetCurrentTerm(1))
	granted, err := s.VoteFor(peer1ID)
	requireT.NoError(err)
	requireT.True(granted)
}

func TestFollowerApplyPeerConnectedDoesNothing(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	r := newReactor(s)

	result, err := r.Apply(magmatypes.ServerID(uuid.New()), nil)
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{}, result)
}

func TestFollowerApplyClientRequestIgnoreIfNotLeader(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(1))
	r := newReactor(s)

	result, err := r.Apply(magmatypes.ZeroServerID, &types.ClientRequest{
		Data: []byte{0x01},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		CommitInfo: types.CommitInfo{
			NextLogIndex:   0,
			CommittedCount: 0,
		},
	}, result)
	requireT.Zero(r.ignoreHeartbeatTick)
	requireT.Empty(r.matchIndex)
}
