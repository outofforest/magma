package reactor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/raft/types"
	magmatypes "github.com/outofforest/magma/types"
)

func TestScenarioA(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	lID, l, initMsg := leaderReactor(t)
	txb := newTxBuilder()
	fID, f, dir := followerReactor(t, 6, txs(
		txb(0x01), txb(0x01, 0x01), txb(0x01, 0x02),
		txb(0x04), txb(0x04, 0x01),
		txb(0x05), txb(0x05, 0x01),
		txb(0x06), txb(0x06, 0x01),
	))

	result, err := f.Apply(lID, initMsg)
	requireT.NoError(err)
	requireT.Equal(Result{
		Role: types.RoleFollower,
		CommitInfo: types.CommitInfo{
			NextIndex: 95,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			lID,
		},
		Message: &types.LogSyncResponse{
			Term:      6,
			NextIndex: 95,
		},
	}, result)

	result, err = l.Apply(fID, result.Message)
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: lID,
		CommitInfo: types.CommitInfo{
			NextIndex:   106,
			HotEndIndex: 106,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			fID,
		},
		Message: &types.LogSyncRequest{
			Term:           6,
			LastTerm:       6,
			NextIndex:      95,
			TermStartIndex: 74,
		},
	}, result)

	result, err = f.Apply(lID, result.Message)
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: lID,
		CommitInfo: types.CommitInfo{
			NextIndex: 95,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			lID,
		},
		Message: &types.LogSyncResponse{
			Term:      6,
			NextIndex: 95,
		},
	}, result)

	result, err = l.Apply(fID, result.Message)
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: lID,
		CommitInfo: types.CommitInfo{
			NextIndex:   106,
			HotEndIndex: 106,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			fID,
		},
		Message: &StartTransfer{
			NextIndex: 95,
		},
	}, result)

	lastTerm, nextIndex, err := f.state.Append([]byte{0x02, 0xff, 0xff}, false, false)
	requireT.NoError(err)
	requireT.EqualValues(6, lastTerm)
	requireT.EqualValues(106, nextIndex)
	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x01), txb(0x01, 0x02),
		txb(0x04), txb(0x04, 0x01),
		txb(0x05), txb(0x05, 0x01),
		txb(0x06), txb(0x06, 0x01), txb(0xff, 0xff),
	))
}

func TestScenarioB(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	lID, l, initMsg := leaderReactor(t)
	txb := newTxBuilder()
	fID, f, dir := followerReactor(t, 4, txs(
		txb(0x01), txb(0x01, 0x01), txb(0x01, 0x02),
		txb(0x04),
	))

	result, err := f.Apply(lID, initMsg)
	requireT.NoError(err)
	requireT.Equal(Result{
		Role: types.RoleFollower,
		CommitInfo: types.CommitInfo{
			NextIndex: 42,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			lID,
		},
		Message: &types.LogSyncResponse{
			Term:      6,
			NextIndex: 42,
		},
	}, result)

	result, err = l.Apply(fID, result.Message)
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: lID,
		CommitInfo: types.CommitInfo{
			NextIndex:   106,
			HotEndIndex: 106,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			fID,
		},
		Message: &types.LogSyncRequest{
			Term:           6,
			LastTerm:       4,
			NextIndex:      42,
			TermStartIndex: 32,
		},
	}, result)

	result, err = f.Apply(lID, result.Message)
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: lID,
		CommitInfo: types.CommitInfo{
			NextIndex: 42,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			lID,
		},
		Message: &types.LogSyncResponse{
			Term:      6,
			NextIndex: 42,
		},
	}, result)

	result, err = l.Apply(fID, result.Message)
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: lID,
		CommitInfo: types.CommitInfo{
			NextIndex:   106,
			HotEndIndex: 106,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			fID,
		},
		Message: &StartTransfer{
			NextIndex: 42,
		},
	}, result)

	lastTerm, nextIndex, err := f.state.Append([]byte{0x02, 0xff, 0xff}, false, false)
	requireT.NoError(err)
	requireT.EqualValues(4, lastTerm)
	requireT.EqualValues(53, nextIndex)
	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x01), txb(0x01, 0x02),
		txb(0x04), txb(0xff, 0xff),
	))
}

func TestScenarioC(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	lID, l, initMsg := leaderReactor(t)
	txb := newTxBuilder()
	fID, f, dir := followerReactor(t, 6, txs(
		txb(0x01), txb(0x01, 0x01), txb(0x01, 0x02),
		txb(0x04), txb(0x04, 0x01),
		txb(0x05), txb(0x05, 0x01),
		txb(0x06), txb(0x06, 0x01), txb(0x06, 0x02), txb(0x06, 0x03),
	))

	result, err := f.Apply(lID, initMsg)
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: lID,
		CommitInfo: types.CommitInfo{
			NextIndex: 106,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			lID,
		},
		Message: &types.LogSyncResponse{
			Term:      6,
			NextIndex: 106,
		},
	}, result)

	result, err = l.Apply(fID, result.Message)
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: lID,
		CommitInfo: types.CommitInfo{
			NextIndex:   106,
			HotEndIndex: 106,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			fID,
		},
		Message: &StartTransfer{
			NextIndex: 106,
		},
	}, result)

	lastTerm, nextIndex, err := f.state.Append([]byte{0x02, 0xff, 0xff}, false, false)
	requireT.NoError(err)
	requireT.EqualValues(6, lastTerm)
	requireT.EqualValues(117, nextIndex)
	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x01), txb(0x01, 0x02),
		txb(0x04), txb(0x04, 0x01),
		txb(0x05), txb(0x05, 0x01),
		txb(0x06), txb(0x06, 0x01), txb(0x06, 0x02), txb(0xff, 0xff),
	))
}

func TestScenarioD(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	lID, l, initMsg := leaderReactor(t)
	txb := newTxBuilder()
	fID, f, dir := followerReactor(t, 7, txs(
		txb(0x01), txb(0x01, 0x01), txb(0x01, 0x02),
		txb(0x04), txb(0x04, 0x01),
		txb(0x05), txb(0x05, 0x01),
		txb(0x06), txb(0x06, 0x01), txb(0x06, 0x02),
		txb(0x07), txb(0x07, 0x01),
	))

	result, err := f.Apply(lID, initMsg)
	requireT.NoError(err)
	requireT.Equal(Result{
		Role: types.RoleFollower,
		CommitInfo: types.CommitInfo{
			NextIndex: 127,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			lID,
		},
		Message: &types.LogSyncResponse{
			Term:      7,
			NextIndex: 127,
		},
	}, result)

	result, err = l.Apply(fID, result.Message)
	requireT.NoError(err)
	requireT.Equal(Result{
		Role: types.RoleFollower,
		CommitInfo: types.CommitInfo{
			NextIndex: 106,
		},
	}, result)

	lastTerm, nextIndex, err := f.state.Append([]byte{0x02, 0xff, 0xff}, false, false)
	requireT.NoError(err)
	requireT.EqualValues(7, lastTerm)
	requireT.EqualValues(138, nextIndex)
	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x01), txb(0x01, 0x02),
		txb(0x04), txb(0x04, 0x01),
		txb(0x05), txb(0x05, 0x01),
		txb(0x06), txb(0x06, 0x01), txb(0x06, 0x02),
		txb(0x07), txb(0x07, 0x01), txb(0xff, 0xff),
	))
}

func TestScenarioE(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	lID, l, initMsg := leaderReactor(t)
	txb := newTxBuilder()
	fID, f, dir := followerReactor(t, 4, txs(
		txb(0x01), txb(0x01, 0x01), txb(0x01, 0x02),
		txb(0x04), txb(0x04, 0x01), txb(0x04, 0x02), txb(0x04, 0x03),
	))

	result, err := f.Apply(lID, initMsg)
	requireT.NoError(err)
	requireT.Equal(Result{
		Role: types.RoleFollower,
		CommitInfo: types.CommitInfo{
			NextIndex: 75,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			lID,
		},
		Message: &types.LogSyncResponse{
			Term:      6,
			NextIndex: 75,
		},
	}, result)

	result, err = l.Apply(fID, result.Message)
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: lID,
		CommitInfo: types.CommitInfo{
			NextIndex:   106,
			HotEndIndex: 106,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			fID,
		},
		Message: &types.LogSyncRequest{
			Term:           6,
			LastTerm:       6,
			NextIndex:      75,
			TermStartIndex: 74,
		},
	}, result)

	result, err = f.Apply(lID, result.Message)
	requireT.NoError(err)
	requireT.Equal(Result{
		Role: types.RoleFollower,
		CommitInfo: types.CommitInfo{
			NextIndex: 75,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			lID,
		},
		Message: &types.LogSyncResponse{
			Term:      6,
			NextIndex: 74,
		},
	}, result)

	result, err = l.Apply(fID, result.Message)
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: lID,
		CommitInfo: types.CommitInfo{
			NextIndex:   106,
			HotEndIndex: 106,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			fID,
		},
		Message: &types.LogSyncRequest{
			Term:           6,
			LastTerm:       5,
			NextIndex:      74,
			TermStartIndex: 53,
		},
	}, result)

	result, err = f.Apply(lID, result.Message)
	requireT.NoError(err)
	requireT.Equal(Result{
		Role: types.RoleFollower,
		CommitInfo: types.CommitInfo{
			NextIndex: 75,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			lID,
		},
		Message: &types.LogSyncResponse{
			Term:      6,
			NextIndex: 53,
		},
	}, result)

	result, err = l.Apply(fID, result.Message)
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: lID,
		CommitInfo: types.CommitInfo{
			NextIndex:   106,
			HotEndIndex: 106,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			fID,
		},
		Message: &types.LogSyncRequest{
			Term:           6,
			LastTerm:       4,
			NextIndex:      53,
			TermStartIndex: 32,
		},
	}, result)

	result, err = f.Apply(lID, result.Message)
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: lID,
		CommitInfo: types.CommitInfo{
			NextIndex: 53,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			lID,
		},
		Message: &types.LogSyncResponse{
			Term:      6,
			NextIndex: 53,
		},
	}, result)

	result, err = l.Apply(fID, result.Message)
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: lID,
		CommitInfo: types.CommitInfo{
			NextIndex:   106,
			HotEndIndex: 106,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			fID,
		},
		Message: &StartTransfer{
			NextIndex: 53,
		},
	}, result)

	lastTerm, nextIndex, err := f.state.Append([]byte{0x02, 0xff, 0xff}, false, false)
	requireT.NoError(err)
	requireT.EqualValues(4, lastTerm)
	requireT.EqualValues(64, nextIndex)
	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x01), txb(0x01, 0x02),
		txb(0x04), txb(0x04, 0x01), txb(0xff, 0xff),
	))
}

func TestScenarioF(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	lID, l, initMsg := leaderReactor(t)
	txb := newTxBuilder()
	fID, f, dir := followerReactor(t, 3, txs(
		txb(0x01), txb(0x01, 0x01), txb(0x01, 0x02),
		txb(0x02), txb(0x02, 0x01), txb(0x02, 0x02),
		txb(0x03), txb(0x03, 0x01), txb(0x03, 0x02), txb(0x03, 0x03), txb(0x03, 0x04),
	))

	result, err := f.Apply(lID, initMsg)
	requireT.NoError(err)
	requireT.Equal(Result{
		Role: types.RoleFollower,
		CommitInfo: types.CommitInfo{
			NextIndex: 118,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			lID,
		},
		Message: &types.LogSyncResponse{
			Term:      6,
			NextIndex: 74,
		},
	}, result)

	result, err = l.Apply(fID, result.Message)
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: lID,
		CommitInfo: types.CommitInfo{
			NextIndex:   106,
			HotEndIndex: 106,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			fID,
		},
		Message: &types.LogSyncRequest{
			Term:           6,
			LastTerm:       5,
			NextIndex:      74,
			TermStartIndex: 53,
		},
	}, result)

	result, err = f.Apply(lID, result.Message)
	requireT.NoError(err)
	requireT.Equal(Result{
		Role: types.RoleFollower,
		CommitInfo: types.CommitInfo{
			NextIndex: 118,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			lID,
		},
		Message: &types.LogSyncResponse{
			Term:      6,
			NextIndex: 53,
		},
	}, result)

	result, err = l.Apply(fID, result.Message)
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: lID,
		CommitInfo: types.CommitInfo{
			NextIndex:   106,
			HotEndIndex: 106,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			fID,
		},
		Message: &types.LogSyncRequest{
			Term:           6,
			LastTerm:       4,
			NextIndex:      53,
			TermStartIndex: 32,
		},
	}, result)

	result, err = f.Apply(lID, result.Message)
	requireT.NoError(err)
	requireT.Equal(Result{
		Role: types.RoleFollower,
		CommitInfo: types.CommitInfo{
			NextIndex: 118,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			lID,
		},
		Message: &types.LogSyncResponse{
			Term:      6,
			NextIndex: 32,
		},
	}, result)

	result, err = l.Apply(fID, result.Message)
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: lID,
		CommitInfo: types.CommitInfo{
			NextIndex:   106,
			HotEndIndex: 106,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			fID,
		},
		Message: &types.LogSyncRequest{
			Term:           6,
			LastTerm:       1,
			NextIndex:      32,
			TermStartIndex: 0,
		},
	}, result)

	result, err = f.Apply(lID, result.Message)
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleFollower,
		LeaderID: lID,
		CommitInfo: types.CommitInfo{
			NextIndex: 32,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			lID,
		},
		Message: &types.LogSyncResponse{
			Term:      6,
			NextIndex: 32,
		},
	}, result)

	result, err = l.Apply(fID, result.Message)
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: lID,
		CommitInfo: types.CommitInfo{
			NextIndex:   106,
			HotEndIndex: 106,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			fID,
		},
		Message: &StartTransfer{
			NextIndex: 32,
		},
	}, result)

	lastTerm, nextIndex, err := f.state.Append([]byte{0x02, 0xff, 0xff}, false, false)
	requireT.NoError(err)
	requireT.EqualValues(1, lastTerm)
	requireT.EqualValues(43, nextIndex)
	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x01), txb(0x01, 0x02), txb(0xff, 0xff),
	))
}

func leaderReactor(t *testing.T) (magmatypes.ServerID, *Reactor, *types.LogSyncRequest) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(6))
	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x01), txb(0x01, 0x02),
		txb(0x04), txb(0x04, 0x01),
		txb(0x05), txb(0x05, 0x01),
	), true, true)
	requireT.NoError(err)

	r := newReactor(serverID, s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	_, err = r.Apply(magmatypes.ZeroServerID, &types.ClientRequest{
		Data: []byte{0x02, 0x06, 0x01},
	})
	requireT.NoError(err)
	_, err = r.Apply(magmatypes.ZeroServerID, &types.ClientRequest{
		Data: []byte{0x02, 0x06, 0x02},
	})
	requireT.NoError(err)

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x01), txb(0x01, 0x02),
		txb(0x04), txb(0x04, 0x01),
		txb(0x05), txb(0x05, 0x01),
		txb(0x06), txb(0x06, 0x01), txb(0x06, 0x02),
	))

	result, err := r.Apply(peer1ID, nil)
	requireT.NoError(err)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextIndex:      106,
			CommittedCount: 0,
			HotEndIndex:    106,
		},
		Channel: ChannelL2P,
		Recipients: []magmatypes.ServerID{
			peer1ID,
		},
		Message: &types.LogSyncRequest{
			Term:           6,
			NextIndex:      106,
			LastTerm:       6,
			TermStartIndex: 74,
		},
	}, result)

	return serverID, r, result.Message.(*types.LogSyncRequest)
}

func followerReactor(t *testing.T, term types.Term, log []byte) (magmatypes.ServerID, *Reactor, string) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(term))
	_, _, err := s.Append(txs(log), true, true)
	requireT.NoError(err)

	return peer1ID, newReactor(peer1ID, s), dir
}
