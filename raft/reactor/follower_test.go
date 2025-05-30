package reactor

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"

	"github.com/outofforest/magma/gossip/wire"
	"github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/state"
	"github.com/outofforest/magma/state/events"
	"github.com/outofforest/magma/state/repository"
	magmatypes "github.com/outofforest/magma/types"
	"github.com/outofforest/varuint64"
)

var (
	serverID = magmatypes.ServerID("S")
	peer1ID  = magmatypes.ServerID("P1")
	peer2ID  = magmatypes.ServerID("P2")
	peer3ID  = magmatypes.ServerID("P3")
	peer4ID  = magmatypes.ServerID("P4")

	passivePeerID = magmatypes.ServerID("PP")

	config = magmatypes.Config{
		ServerID: serverID,
		Servers: []magmatypes.ServerConfig{
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

func newReactor(serverID magmatypes.ServerID, s *state.State) *Reactor {
	activePeers := make([]magmatypes.ServerID, 0, len(config.Servers))
	for _, s := range config.Servers {
		if s.ID != serverID {
			activePeers = append(activePeers, s.ID)
		}
	}

	return New(serverID, activePeers, []magmatypes.ServerID{passivePeerID}, 600, s)
}

func logEqual(requireT *require.Assertions, dir string, expectedLog []byte) {
	repo, err := repository.Open(filepath.Join(dir, "repo"), uint64(os.Getpagesize()))
	requireT.NoError(err)

	tp := repository.NewTailProvider()
	tp.SetTail(magmatypes.Index(len(expectedLog)), 0)

	it := repo.Iterator(tp, 0)
	defer it.Close()

	buf := bytes.NewBuffer(nil)
	for buf.Len() < len(expectedLog) {
		reader, _, err := it.Reader(true)
		requireT.NoError(err)
		_, err = io.Copy(buf, reader)
		requireT.NoError(err)
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
	t.Parallel()

	requireT := require.New(t)
	s, _ := newState(t, "")
	r := newReactor(serverID, s)

	requireT.Equal(types.RoleFollower, r.role)
	requireT.EqualValues(0, r.electionTick)
	requireT.EqualValues(0, r.ignoreElectionTick)
}

func TestFollowerSetup(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	s, _ := newState(t, "")
	r := newReactor(serverID, s)

	r.role = types.RoleCandidate
	r.leaderID = serverID
	r.votedForMe = 2
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

	r.transitionToFollower()

	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(magmatypes.ZeroServerID, r.leaderID)
	requireT.Zero(r.votedForMe)
	requireT.EqualValues(1, r.electionTick)
	requireT.EqualValues(2, r.ignoreElectionTick)
	requireT.Empty(r.nextIndex)
	requireT.Empty(r.matchIndex)

	requireT.EqualValues(3, r.lastTerm)
	requireT.Equal(types.CommitInfo{
		NextIndex:      10,
		CommittedCount: 5,
		HotEndIndex:    0,
	}, r.commitInfo)

	requireT.EqualValues(0, s.CurrentTerm())
}

func TestFollowerAppendTxAppendToEmptyLog(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(1))
	r := newReactor(serverID, s)
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
			NextIndex:      magmatypes.Index(len(tx)),
			CommittedCount: 0,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.Equal(peer1ID, r.leaderID)

	logEqual(requireT, dir, tx)
}

func TestFollowerAppendTxAppendToNonEmptyLog(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(1))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01),
		txb(0x02, 0x00, 0x00),
	), true, true)
	requireT.NoError(err)

	r := newReactor(serverID, s)
	r.leaderID = peer1ID

	result, err := r.Apply(peer1ID, txb(0x03, 0x01, 0x02, 0x03))
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			NextIndex:      35,
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

func TestFollowerAppendTxDoesNothingIfNotFromLeader(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(1))
	r := newReactor(serverID, s)
	r.leaderID = peer1ID

	txb := newTxBuilder()
	tx := txs(
		txb(0x01),
		txb(0x01, 0x00),
	)
	result, err := r.Apply(peer2ID, tx)
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			NextIndex:      0,
			CommittedCount: 0,
		},
	}, result)
	requireT.Zero(r.ignoreElectionTick)
	requireT.Equal(peer1ID, r.leaderID)
}

func TestFollowerAppendTxFailsIfTxDoesNotContainChecksum(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(1))
	r := newReactor(serverID, s)
	r.leaderID = peer1ID

	result, err := r.Apply(peer1ID, []byte{0x01, 0x01})
	requireT.Error(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{}, result)
	requireT.Zero(r.ignoreElectionTick)
	requireT.Equal(peer1ID, r.leaderID)
}

func TestFollowerLogACKDoesNothing(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(1))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x00),
	), true, true)
	requireT.NoError(err)
	r := newReactor(serverID, s)
	r.leaderID = peer1ID

	result, err := r.Apply(peer1ID, &types.LogACK{
		Term:      1,
		NextIndex: 21,
		SyncIndex: 21,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			NextIndex:      21,
			CommittedCount: 0,
		},
	}, result)
	requireT.Zero(r.nextIndex[peer1ID])
	requireT.Zero(r.matchIndex[peer1ID])

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x00),
	))
}

func TestFollowerLogSyncRequestOnFutureTerm(t *testing.T) {
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

	result, err := r.Apply(peer1ID, &types.LogSyncRequest{
		Term:      3,
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
			Term:      3,
			NextIndex: 43,
			SyncIndex: 0,
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

func TestFollowerLogSyncRequestDiscardEntries(t *testing.T) {
	t.Parallel()

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

	r := newReactor(serverID, s)
	r.syncedCount = 21

	result, err := r.Apply(peer1ID, &types.LogSyncRequest{
		Term:      4,
		NextIndex: 42,
		LastTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			NextIndex:      42,
			CommittedCount: 0,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Message: &types.LogSyncResponse{
			Term:      4,
			NextIndex: 42,
			SyncIndex: 21,
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

func TestFollowerLogSyncRequestDiscardAtSynced(t *testing.T) {
	t.Parallel()

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

	r := newReactor(serverID, s)
	r.syncedCount = 42

	result, err := r.Apply(peer1ID, &types.LogSyncRequest{
		Term:      4,
		NextIndex: 42,
		LastTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			NextIndex:      42,
			CommittedCount: 0,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Message: &types.LogSyncResponse{
			Term:      4,
			NextIndex: 42,
			SyncIndex: 42,
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

func TestFollowerLogSyncRequestTermMismatch(t *testing.T) {
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
	r.syncedCount = 43

	result, err := r.Apply(peer1ID, &types.LogSyncRequest{
		Term:           4,
		NextIndex:      43,
		LastTerm:       3,
		TermStartIndex: 21,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
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
			NextIndex: 21,
			SyncIndex: 0,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
	requireT.EqualValues(43, r.syncedCount)

	requireT.EqualValues(4, s.CurrentTerm())

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x00),
	))
}

func TestFollowerLogSyncRequestRejectIfNoPreviousEntry(t *testing.T) {
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

	result, err := r.Apply(peer1ID, &types.LogSyncRequest{
		Term:      4,
		NextIndex: 1000,
		LastTerm:  3,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
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
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)

	requireT.EqualValues(4, s.CurrentTerm())

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
	))
}

func TestFollowerLogSyncRequestSendResponseIfLastTermIsLower(t *testing.T) {
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

func TestFollowerLogSyncRequestSendResponseIfNextIndexIsLower(t *testing.T) {
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

	result, err := r.Apply(peer1ID, &types.LogSyncRequest{
		Term:      4,
		NextIndex: 44,
		LastTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
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

	requireT.EqualValues(4, s.CurrentTerm())

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
	))
}

func TestFollowerLogSyncRequestDoNothingOnLowerTerm(t *testing.T) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(4))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
	), true, true)
	requireT.NoError(err)
	r := newReactor(serverID, s)

	result, err := r.Apply(peer2ID, &types.LogSyncRequest{
		Term:      3,
		NextIndex: 43,
		LastTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextIndex:      43,
			CommittedCount: 0,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer2ID,
		},
		Message: &types.LogSyncResponse{
			Term:      4,
			NextIndex: 43,
		},
	}, result)
	requireT.Zero(r.ignoreElectionTick)

	requireT.EqualValues(4, s.CurrentTerm())

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
	))
}

func TestFollowerLogSyncRequestDoNothingOnLowerTermAndNextLogItemBelowCommittedCount(t *testing.T) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(4))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
	), true, true)
	requireT.NoError(err)
	r := newReactor(serverID, s)

	r.commitInfo = types.CommitInfo{
		NextIndex:      43,
		CommittedCount: 43,
		HotEndIndex:    43,
	}

	result, err := r.Apply(peer2ID, &types.LogSyncRequest{
		Term:      3,
		NextIndex: 40,
		LastTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextIndex:      43,
			CommittedCount: 43,
			HotEndIndex:    43,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer2ID,
		},
		Message: &types.LogSyncResponse{
			Term:      4,
			NextIndex: 43,
		},
	}, result)
	requireT.Zero(r.ignoreElectionTick)

	requireT.EqualValues(4, s.CurrentTerm())

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
	))
}

func TestFollowerLogSyncRequestErrorIfNextIndexEquals(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(2))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01),
		txb(0x02),
	), true, true)
	requireT.NoError(err)
	r := newReactor(serverID, s)

	_, err = r.Apply(peer1ID, &types.LogSyncRequest{
		Term:           4,
		NextIndex:      10,
		LastTerm:       3,
		TermStartIndex: 10,
	})
	requireT.Error(err)
}

func TestFollowerLogSyncRequestErrorIfNextLogItemBelowCommittedCount(t *testing.T) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(4))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
	), true, true)
	requireT.NoError(err)
	r := newReactor(serverID, s)

	r.commitInfo = types.CommitInfo{
		NextIndex:      43,
		CommittedCount: 43,
		HotEndIndex:    43,
	}

	_, err = r.Apply(peer2ID, &types.LogSyncRequest{
		Term:      5,
		NextIndex: 40,
		LastTerm:  2,
	})
	requireT.Error(err)
	requireT.Equal(types.RoleFollower, r.role)

	requireT.EqualValues(5, s.CurrentTerm())

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
	r := newReactor(serverID, s)

	result, err := r.Apply(peer1ID, &types.VoteRequest{
		Term:      1,
		NextIndex: 0,
		LastTerm:  0,
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
	r := newReactor(serverID, s)

	result, err := r.Apply(peer1ID, &types.VoteRequest{
		Term:      2,
		NextIndex: 43,
		LastTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextIndex:      43,
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
	r := newReactor(serverID, s)

	result, err := r.Apply(peer1ID, &types.VoteRequest{
		Term:      2,
		NextIndex: 50,
		LastTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextIndex:      44,
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
	r := newReactor(serverID, s)

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
	r := newReactor(serverID, s)

	result, err := r.Apply(peer1ID, &types.VoteRequest{
		Term:      2,
		NextIndex: 43,
		LastTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextIndex:      43,
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
		Term:      2,
		NextIndex: 43,
		LastTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextIndex:      43,
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
	r := newReactor(serverID, s)

	result, err := r.Apply(peer1ID, &types.VoteRequest{
		Term:      2,
		NextIndex: 43,
		LastTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextIndex:      43,
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
		Term:      3,
		NextIndex: 43,
		LastTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextIndex:      43,
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
	r := newReactor(serverID, s)

	result, err := r.Apply(peer1ID, &types.VoteRequest{
		Term:      1,
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

func TestFollowerApplyVoteRequestRejectedOnLowerLogTerm(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(2))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x02, 0x00, 0x00),
		txb(0x02), txb(0x01, 0x00),
	), true, true)
	requireT.NoError(err)
	r := newReactor(serverID, s)

	result, err := r.Apply(peer1ID, &types.VoteRequest{
		Term:      3,
		NextIndex: 43,
		LastTerm:  1,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextIndex:      43,
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
	r := newReactor(serverID, s)

	result, err := r.Apply(peer1ID, &types.VoteRequest{
		Term:      2,
		NextIndex: 43,
		LastTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextIndex:      44,
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
	r := newReactor(serverID, s)

	result, err := r.Apply(peer1ID, &types.VoteRequest{
		Term:      2,
		NextIndex: 43,
		LastTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextIndex:      43,
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
		Term:      2,
		NextIndex: 43,
		LastTerm:  2,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextIndex:      43,
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

func TestFollowerApplyElectionTickAfterElectionTime(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	r := newReactor(serverID, s)

	result, err := r.Apply(magmatypes.ZeroServerID, types.ElectionTick(1))
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, r.role)
	requireT.EqualValues(2, r.ignoreElectionTick)
	requireT.EqualValues(1, r.electionTick)
	requireT.EqualValues(1, s.CurrentTerm())
	requireT.Equal(1, r.votedForMe)
	requireT.Equal(Result{
		Role:     types.RoleCandidate,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextIndex:      0,
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
			Term:      1,
			NextIndex: 0,
			LastTerm:  0,
		},
	}, result)

	granted, err := s.VoteFor(peer1ID)
	requireT.NoError(err)
	requireT.False(granted)

	granted, err = s.VoteFor(serverID)
	requireT.NoError(err)
	requireT.True(granted)
}

func TestFollowerApplyElectionTickBeforeElectionTime(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	r := newReactor(serverID, s)

	r.ignoreElectionTick = 2

	result, err := r.Apply(magmatypes.ZeroServerID, types.ElectionTick(1))
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.EqualValues(2, r.ignoreElectionTick)
	requireT.EqualValues(1, r.electionTick)
	requireT.EqualValues(0, s.CurrentTerm())
	requireT.Equal(0, r.votedForMe)
	requireT.Equal(Result{
		CommitInfo: types.CommitInfo{
			NextIndex:      0,
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
	r := newReactor(serverID, s)

	result, err := r.Apply(magmatypes.ServerID("PeerID"), nil)
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{}, result)
}

func TestFollowerApplyClientRequestIgnoreIfNotLeader(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(1))
	r := newReactor(serverID, s)

	result, err := r.Apply(magmatypes.ZeroServerID, &types.ClientRequest{
		Data: []byte{0x01},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		CommitInfo: types.CommitInfo{
			NextIndex:      0,
			CommittedCount: 0,
		},
	}, result)
	requireT.Empty(r.matchIndex)
}

func TestFollowerApplyHeartbeatTickCommitToLeaderCommit(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(5))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x01),
		txb(0x02), txb(0x01, 0x02),
		txb(0x03), txb(0x01, 0x03),
		txb(0x04), txb(0x01, 0x04),
		txb(0x05),
	), true, true)
	requireT.NoError(err)
	r := newReactor(serverID, s)
	r.leaderID = peer1ID
	r.leaderCommittedCount = 84

	result, err := r.Apply(magmatypes.ZeroServerID, types.HeartbeatTick(20))
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			NextIndex:      94,
			CommittedCount: 84,
		},
		Channel: ChannelP2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Message: &types.LogACK{
			Term:      5,
			NextIndex: 94,
			SyncIndex: 94,
		},
		Force: true,
	}, result)
}

func TestFollowerApplyHeartbeatTickCommitToNextLog(t *testing.T) {
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
	r := newReactor(serverID, s)
	r.leaderID = peer1ID
	r.leaderCommittedCount = 94

	result, err := r.Apply(magmatypes.ZeroServerID, types.HeartbeatTick(20))
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			NextIndex:      84,
			CommittedCount: 84,
		},
		Channel: ChannelP2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Message: &types.LogACK{
			Term:      5,
			NextIndex: 84,
			SyncIndex: 84,
		},
		Force: true,
	}, result)
}

func TestFollowerApplyHeartbeatTickNoLeader(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(5))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x01),
		txb(0x02), txb(0x01, 0x02),
		txb(0x03), txb(0x01, 0x03),
		txb(0x04), txb(0x01, 0x04),
		txb(0x05),
	), true, true)
	requireT.NoError(err)
	r := newReactor(serverID, s)
	r.leaderCommittedCount = 94

	result, err := r.Apply(magmatypes.ZeroServerID, types.HeartbeatTick(20))
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextIndex:      94,
			CommittedCount: 94,
		},
		Force: true,
	}, result)
}

func TestFollowerApplyHeartbeat(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(5))

	txb := newTxBuilder()
	_, _, err := s.Append(txb(0x05), true, true)
	requireT.NoError(err)

	r := newReactor(serverID, s)
	r.leaderID = peer1ID

	result, err := r.Apply(peer1ID, &types.Heartbeat{
		Term:         5,
		LeaderCommit: 10,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			NextIndex:      10,
			CommittedCount: 10,
		},
	}, result)
	requireT.EqualValues(1, r.ignoreElectionTick)
}

func TestFollowerApplyHeartbeatIgnoreIfNotLeader(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(5))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x04),
		txb(0x05),
	), true, true)
	requireT.NoError(err)

	r := newReactor(serverID, s)

	result, err := r.Apply(peer1ID, &types.Heartbeat{
		Term:         5,
		LeaderCommit: 20,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: magmatypes.ZeroServerID,
		CommitInfo: types.CommitInfo{
			NextIndex:      20,
			CommittedCount: 0,
		},
	}, result)
	requireT.Zero(r.ignoreElectionTick)
}

func TestFollowerApplyHeartbeatIgnoreLowerTerm(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(5))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x04),
		txb(0x05),
	), true, true)
	requireT.NoError(err)

	r := newReactor(serverID, s)
	r.leaderID = peer1ID

	result, err := r.Apply(peer1ID, &types.Heartbeat{
		Term:         4,
		LeaderCommit: 10,
	})
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			NextIndex:      20,
			CommittedCount: 0,
		},
	}, result)
	requireT.Zero(r.ignoreElectionTick)
}

func TestFollowerApplyHeartbeatErrorIfNewLeaderCommitIsLower(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(5))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x04),
		txb(0x05),
	), true, true)
	requireT.NoError(err)

	r := newReactor(serverID, s)
	r.leaderID = peer1ID
	r.commitInfo = types.CommitInfo{
		NextIndex:      20,
		CommittedCount: 20,
	}

	_, err = r.Apply(peer1ID, &types.Heartbeat{
		Term:         5,
		LeaderCommit: 10,
	})
	requireT.Error(err)
}

func TestFollowerApplyHotEnd(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(5))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x04),
		txb(0x05),
	), true, true)
	requireT.NoError(err)

	r := newReactor(serverID, s)
	r.leaderID = peer1ID
	r.commitInfo = types.CommitInfo{
		NextIndex:      20,
		CommittedCount: 10,
		HotEndIndex:    0,
	}

	result, err := r.Apply(peer1ID, &wire.HotEnd{})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			NextIndex:      20,
			CommittedCount: 10,
			HotEndIndex:    20,
		},
	}, result)
}

func TestFollowerApplyHotEndIgnoreFromNonLeader(t *testing.T) {
	requireT := require.New(t)
	s, _ := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(5))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x04),
		txb(0x05),
	), true, true)
	requireT.NoError(err)

	r := newReactor(serverID, s)
	r.leaderID = peer1ID
	r.commitInfo = types.CommitInfo{
		NextIndex:      20,
		CommittedCount: 10,
		HotEndIndex:    0,
	}

	result, err := r.Apply(peer2ID, &wire.HotEnd{})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, r.role)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: peer1ID,
		CommitInfo: types.CommitInfo{
			NextIndex:      20,
			CommittedCount: 10,
			HotEndIndex:    0,
		},
	}, result)
}
