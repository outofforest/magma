package reactor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/state"
	magmatypes "github.com/outofforest/magma/types"
)

func newReactorSingleMode(s *state.State) *Reactor {
	return New(serverID, nil, []magmatypes.ServerID{passivePeerID}, s)
}

func TestSingleModeApplyElectionTickTransitionToLeader(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	s, dir := newState(t, "")
	r := newReactorSingleMode(s)

	result, err := r.Apply(magmatypes.ZeroServerID, types.ElectionTick(1))
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.EqualValues(2, r.ignoreElectionTick)
	requireT.EqualValues(1, s.CurrentTerm())
	requireT.Equal(1, r.votedForMe)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextIndex:      10,
			CommittedCount: 10,
			HotEndIndex:    10,
		},
	}, result)
	requireT.Empty(r.nextIndex)
	requireT.Empty(r.matchIndex)
	requireT.EqualValues(1, r.lastTerm)

	granted, err := s.VoteFor(peer1ID)
	requireT.NoError(err)
	requireT.False(granted)

	granted, err = s.VoteFor(serverID)
	requireT.NoError(err)
	requireT.True(granted)

	txb := newTxBuilder()
	logEqual(requireT, dir, txb(0x01))
}

func TestSingleModeApplyClientRequestAppend(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(4))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
		txb(0x03), txb(0x02, 0x00, 0x00),
	), true, true)
	requireT.NoError(err)
	r := newReactorSingleMode(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	result, err := r.Apply(magmatypes.ZeroServerID, &types.ClientRequest{
		Data: []byte{0x02, 0x01, 0x00},
	})
	requireT.NoError(err)

	requireT.Equal(types.RoleLeader, r.role)
	requireT.EqualValues(4, s.CurrentTerm())
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextIndex:      86,
			CommittedCount: 75,
			HotEndIndex:    86,
		},
	}, result)
	requireT.Empty(r.nextIndex)
	requireT.Empty(r.matchIndex)
	requireT.EqualValues(4, r.lastTerm)

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
		txb(0x03), txb(0x02, 0x00, 0x00),
		txb(0x04), txb(0x01, 0x00),
	))
}

func TestSingleModeApplyHeartbeatTickDoNothing(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)
	s, dir := newState(t, "")
	requireT.NoError(s.SetCurrentTerm(4))

	txb := newTxBuilder()
	_, _, err := s.Append(txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
		txb(0x03), txb(0x02, 0x00, 0x00),
	), true, true)
	requireT.NoError(err)
	r := newReactorSingleMode(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	result, err := r.Apply(magmatypes.ZeroServerID, types.HeartbeatTick(1))
	requireT.NoError(err)

	requireT.Equal(types.RoleLeader, r.role)
	requireT.EqualValues(4, s.CurrentTerm())
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextIndex:      75,
			CommittedCount: 75,
			HotEndIndex:    75,
		},
		Force: true,
	}, result)
	requireT.Empty(r.nextIndex)
	requireT.Empty(r.matchIndex)
	requireT.Equal(types.CommitInfo{
		NextIndex:      75,
		CommittedCount: 75,
		HotEndIndex:    75,
	}, r.commitInfo)
	requireT.EqualValues(4, r.lastTerm)

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
		txb(0x03), txb(0x02, 0x00, 0x00),
		txb(0x04),
	))
}
