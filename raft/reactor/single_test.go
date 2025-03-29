package reactor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/state"
	magmatypes "github.com/outofforest/magma/types"
)

func newReactorSingleMode(s *state.State) *Reactor {
	config := config
	config.Servers = []magmatypes.PeerConfig{{ID: serverID}}
	return New(config, s)
}

func TestSingleModeApplyElectionTimeoutTransitionToLeader(t *testing.T) {
	requireT := require.New(t)
	s, dir := newState(t, "")
	r := newReactorSingleMode(s)

	result, err := r.Apply(magmatypes.ZeroServerID, types.ElectionTick(1))
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, r.role)
	requireT.EqualValues(2, r.ignoreElectionTick)
	requireT.EqualValues(1, r.ignoreHeartbeatTick)
	requireT.EqualValues(1, s.CurrentTerm())
	requireT.EqualValues(1, r.votedForMe)
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   10,
			CommittedCount: 10,
		},
	}, result)
	requireT.Empty(r.nextIndex)
	requireT.Empty(r.matchIndex)
	requireT.EqualValues(1, r.lastLogTerm)

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
	requireT.EqualValues(1, r.ignoreHeartbeatTick)
	requireT.EqualValues(4, s.CurrentTerm())
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			NextLogIndex:   86,
			CommittedCount: 75,
		},
	}, result)
	requireT.Empty(r.nextIndex)
	requireT.Empty(r.matchIndex)
	requireT.EqualValues(4, r.lastLogTerm)

	txb = newTxBuilder()
	logEqual(requireT, dir, txs(
		txb(0x01), txb(0x01, 0x00),
		txb(0x02), txb(0x02, 0x00, 0x00),
		txb(0x03), txb(0x02, 0x00, 0x00),
		txb(0x04), txb(0x01, 0x00),
	))
}

/*
func TestSingleModeApplyHeartbeatTimeoutDoNothing(t *testing.T) {
	requireT := require.New(t)
	s := newState()
	requireT.NoError(s.SetCurrentTerm(4))
	_, _, err := s.Append(0, 0, []byte{
		0x01, 0x01, 0x02, 0x01, 0x00,
		0x01, 0x02, 0x03, 0x02, 0x00, 0x00,
		0x01, 0x03, 0x03, 0x02, 0x00, 0x00,
	}, true)
	requireT.NoError(err)
	r := newReactorSingleMode(s)
	_, err = r.transitionToLeader()
	requireT.NoError(err)

	result, err := r.Apply(magmatypes.ZeroServerID, types.HeartbeatTick(1))
	requireT.NoError(err)

	requireT.Equal(types.RoleLeader, r.role)
	requireT.EqualValues(1, r.heartbeatTick)
	requireT.EqualValues(4, s.CurrentTerm())
	requireT.Equal(Result{
		Role:     types.RoleLeader,
		LeaderID: serverID,
		CommitInfo: types.CommitInfo{
			CommittedCount: 19,
		},
	}, result)
	requireT.Empty(r.sync)
	requireT.Empty(r.matchIndex)
	requireT.Equal(types.CommitInfo{CommittedCount: 19}, r.commitInfo)
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
*/
