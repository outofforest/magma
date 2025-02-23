package engine

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/raft/reactor"
	"github.com/outofforest/magma/raft/state"
	"github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/raft/wire/p2p"
)

var (
	serverID = types.ServerID(uuid.New())
	peer1ID  = types.ServerID(uuid.New())
	peer2ID  = types.ServerID(uuid.New())
	peer3ID  = types.ServerID(uuid.New())
	peer4ID  = types.ServerID(uuid.New())

	peers = []types.ServerID{peer1ID, peer2ID, peer3ID, peer4ID}
)

func newEngine(s *state.State) (*Engine, reactor.TimeAdvancer) {
	ts := &reactor.TestTimeSource{}
	r := reactor.New(serverID, len(peers)/2+1, s, ts)
	return New(r, peers), ts
}

func TestApplyAppendEntriesRequest(t *testing.T) {
	requireT := require.New(t)

	s := &state.State{}
	e, _ := newEngine(s)

	messageID := p2p.NewMessageID()
	role, toSend, err := e.Apply(types.Command{
		PeerID: peer1ID,
		Cmd: p2p.AppendEntriesRequest{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 0,
			LastLogTerm:  0,
			Entries: []state.LogItem{
				{
					Term: 1,
					Data: []byte{0x01},
				},
			},
			LeaderCommit: 1,
		},
	})

	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Equal(Send{
		Recipients: []types.ServerID{peer1ID},
		Message: p2p.AppendEntriesResponse{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 1,
		},
	}, toSend)
}

func TestApplyVoteRequest(t *testing.T) {
	requireT := require.New(t)

	s := &state.State{}
	e, _ := newEngine(s)

	messageID := p2p.NewMessageID()
	role, toSend, err := e.Apply(types.Command{
		PeerID: peer1ID,
		Cmd: p2p.VoteRequest{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 0,
			LastLogTerm:  0,
		},
	})

	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Equal(Send{
		Recipients: []types.ServerID{peer1ID},
		Message: p2p.VoteResponse{
			MessageID:   messageID,
			Term:        1,
			VoteGranted: true,
		},
	}, toSend)
}

func TestTransition(t *testing.T) {
	requireT := require.New(t)

	s := &state.State{}
	e, ts := newEngine(s)

	messageID := transitionToCandidate(requireT, e, ts)
	transitionToLeader(requireT, e, messageID)
}

func transitionToCandidate(requireT *require.Assertions, e *Engine, ts reactor.TimeAdvancer) p2p.MessageID {
	role, toSend, err := e.Apply(types.Command{
		Cmd: types.ElectionTimeout(ts.Add(time.Hour)),
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, role)
	requireT.NotNil(toSend.Message)

	messageID := toSend.Message.(p2p.VoteRequest).MessageID
	requireT.Equal(Send{
		Recipients: peers,
		Message: p2p.VoteRequest{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 0,
			LastLogTerm:  0,
		},
	}, toSend)

	return messageID
}

func transitionToLeader(requireT *require.Assertions, e *Engine, messageID p2p.MessageID) {
	var role types.Role
	var toSend Send
	for _, peer := range []types.ServerID{peer1ID, peer2ID} {
		var err error
		role, toSend, err = e.Apply(types.Command{
			PeerID: peer,
			Cmd: p2p.VoteResponse{
				MessageID:   messageID,
				Term:        1,
				VoteGranted: true,
			},
		})
		requireT.NoError(err)
	}

	requireT.Equal(types.RoleLeader, role)
	requireT.NotNil(toSend.Message)

	messageID = toSend.Message.(p2p.AppendEntriesRequest).MessageID
	requireT.Equal(Send{
		Recipients: peers,
		Message: p2p.AppendEntriesRequest{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 0,
			LastLogTerm:  0,
			Entries: []state.LogItem{
				{Term: 1},
			},
			LeaderCommit: 0,
		},
	}, toSend)

	for _, peer := range peers {
		var err error
		role, toSend, err = e.Apply(types.Command{
			PeerID: peer,
			Cmd: p2p.AppendEntriesResponse{
				MessageID:    messageID,
				Term:         1,
				NextLogIndex: 1,
			},
		})
		requireT.NoError(err)
		requireT.Equal(types.RoleLeader, role)
		requireT.Empty(toSend.Recipients)
	}
}
