package engine

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/raft/reactor"
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

func newEngine(s *state.State) (*Engine, reactor.TimeAdvancer) {
	ts := &reactor.TestTimeSource{}
	r := reactor.New(serverID, len(peers)/2+1, s, ts)
	return New(r, peers), ts
}

func TestTransition(t *testing.T) {
	requireT := require.New(t)

	s := &state.State{}
	e, ts := newEngine(s)

	transitionToLeader(requireT, e, transitionToCandidate(requireT, e, ts))
}

func TestApplyAppendEntriesRequest(t *testing.T) {
	requireT := require.New(t)

	s := &state.State{}
	e, _ := newEngine(s)

	messageID := types.NewMessageID()
	role, toSend, err := e.Apply(types.Command{
		PeerID: peer1ID,
		Cmd: &types.AppendEntriesRequest{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 0,
			LastLogTerm:  0,
			Entries: []types.LogItem{
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
		Recipients: []magmatypes.ServerID{peer1ID},
		Message: &types.AppendEntriesResponse{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 1,
		},
	}, toSend)
}

func TestApplyAppendEntriesResponseMore(t *testing.T) {
	requireT := require.New(t)

	s := &state.State{}
	e, ts := newEngine(s)

	transitionToLeader(requireT, e, transitionToCandidate(requireT, e, ts))

	messageID := types.NewMessageID()
	e.expectedResponses[peer1ID] = messageID

	role, toSend, err := e.Apply(types.Command{
		PeerID: peer1ID,
		Cmd: &types.AppendEntriesResponse{
			MessageID:    messageID,
			Term:         0,
			NextLogIndex: 0,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, role)
	requireT.NotNil(toSend.Message)

	messageID = toSend.Message.(*types.AppendEntriesRequest).MessageID
	requireT.NotEqual(types.ZeroMessageID, messageID)
	requireT.Equal(Send{
		Recipients: []magmatypes.ServerID{peer1ID},
		Message: &types.AppendEntriesRequest{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 0,
			LastLogTerm:  0,
			Entries: []types.LogItem{
				{Term: 1},
			},
			LeaderCommit: 1,
		},
	}, toSend)

	requireT.Equal(messageID, e.expectedResponses[peer1ID])
}

func TestApplyAppendEntriesResponseIgnore(t *testing.T) {
	requireT := require.New(t)

	s := &state.State{}
	e, ts := newEngine(s)

	transitionToLeader(requireT, e, transitionToCandidate(requireT, e, ts))

	messageID := types.NewMessageID()
	e.expectedResponses[peer2ID] = messageID
	requireT.Equal(types.ZeroMessageID, e.expectedResponses[peer1ID])

	role, toSend, err := e.Apply(types.Command{
		PeerID: peer1ID,
		Cmd: &types.AppendEntriesResponse{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 1,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, role)
	requireT.Equal(Send{}, toSend)

	requireT.Equal(types.ZeroMessageID, e.expectedResponses[peer1ID])
	requireT.Equal(messageID, e.expectedResponses[peer2ID])
}

func TestApplyVoteRequest(t *testing.T) {
	requireT := require.New(t)

	s := &state.State{}
	e, _ := newEngine(s)

	messageID := types.NewMessageID()
	role, toSend, err := e.Apply(types.Command{
		PeerID: peer1ID,
		Cmd: &types.VoteRequest{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 0,
			LastLogTerm:  0,
		},
	})

	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Equal(Send{
		Recipients: []magmatypes.ServerID{peer1ID},
		Message: &types.VoteResponse{
			MessageID:   messageID,
			Term:        1,
			VoteGranted: true,
		},
	}, toSend)
}

func TestApplyVoteResponseIgnore(t *testing.T) {
	requireT := require.New(t)

	s := &state.State{}
	e, ts := newEngine(s)

	messageID := transitionToCandidate(requireT, e, ts)
	messageID2 := types.NewMessageID()
	e.expectedResponses[peer1ID] = messageID2

	requireT.Equal(map[magmatypes.ServerID]types.MessageID{
		peer1ID: messageID2,
		peer2ID: messageID,
		peer3ID: messageID,
		peer4ID: messageID,
	}, e.expectedResponses)

	role, toSend, err := e.Apply(types.Command{
		PeerID: peer1ID,
		Cmd: &types.VoteResponse{
			MessageID:   messageID,
			Term:        1,
			VoteGranted: true,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, role)
	requireT.Equal(Send{}, toSend)

	requireT.Equal(map[magmatypes.ServerID]types.MessageID{
		peer1ID: messageID2,
		peer2ID: messageID,
		peer3ID: messageID,
		peer4ID: messageID,
	}, e.expectedResponses)
}

func TestApplyClientRequestIfNoLeader(t *testing.T) {
	requireT := require.New(t)

	s := &state.State{}
	e, _ := newEngine(s)

	role, toSend, err := e.Apply(types.Command{
		Cmd: &types.ClientRequest{
			Data: []byte{0x01},
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Equal(Send{}, toSend)
	requireT.Empty(e.expectedResponses)
}

func TestApplyClientRequestIfLeaderAndNoPeer(t *testing.T) {
	requireT := require.New(t)

	s := &state.State{}
	e, ts := newEngine(s)

	transitionToLeader(requireT, e, transitionToCandidate(requireT, e, ts))

	role, toSend, err := e.Apply(types.Command{
		Cmd: &types.ClientRequest{
			Data: []byte{0x01},
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, role)
	requireT.NotNil(toSend.Message)

	messageID := toSend.Message.(*types.AppendEntriesRequest).MessageID
	requireT.NotEqual(types.ZeroMessageID, messageID)
	requireT.Equal(Send{
		Recipients: peers,
		Message: &types.AppendEntriesRequest{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 1,
			LastLogTerm:  1,
			Entries: []types.LogItem{
				{
					Term: 1,
					Data: []byte{0x01},
				},
			},
			LeaderCommit: 1,
		},
	}, toSend)
	requireT.Equal(map[magmatypes.ServerID]types.MessageID{
		peer1ID: messageID,
		peer2ID: messageID,
		peer3ID: messageID,
		peer4ID: messageID,
	}, e.expectedResponses)
}

func TestApplyClientRequestIfLeaderAndPeer(t *testing.T) {
	requireT := require.New(t)

	s := &state.State{}
	e, ts := newEngine(s)

	transitionToLeader(requireT, e, transitionToCandidate(requireT, e, ts))

	role, toSend, err := e.Apply(types.Command{
		PeerID: peer1ID,
		Cmd: &types.ClientRequest{
			Data: []byte{0x01},
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, role)
	requireT.NotNil(toSend.Message)

	messageID := toSend.Message.(*types.AppendEntriesRequest).MessageID
	requireT.NotEqual(types.ZeroMessageID, messageID)
	requireT.Equal(Send{
		Recipients: peers,
		Message: &types.AppendEntriesRequest{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 1,
			LastLogTerm:  1,
			Entries: []types.LogItem{
				{
					Term: 1,
					Data: []byte{0x01},
				},
			},
			LeaderCommit: 1,
		},
	}, toSend)
	requireT.Equal(map[magmatypes.ServerID]types.MessageID{
		peer1ID: messageID,
		peer2ID: messageID,
		peer3ID: messageID,
		peer4ID: messageID,
	}, e.expectedResponses)
}

func TestApplyClientRequestIfNotLeaderAndNoPeer(t *testing.T) {
	requireT := require.New(t)

	s := &state.State{}
	e, _ := newEngine(s)

	// To set leader.
	_, _, err := e.Apply(types.Command{
		PeerID: peer1ID,
		Cmd: &types.AppendEntriesRequest{
			MessageID:    types.NewMessageID(),
			Term:         1,
			NextLogIndex: 0,
			LastLogTerm:  0,
			Entries: []types.LogItem{
				{Term: 1},
			},
		},
	})
	requireT.NoError(err)

	role, toSend, err := e.Apply(types.Command{
		Cmd: &types.ClientRequest{
			Data: []byte{0x01},
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Equal(Send{
		Recipients: []magmatypes.ServerID{peer1ID},
		Message: &types.ClientRequest{
			Data: []byte{0x01},
		},
	}, toSend)
	requireT.Empty(e.expectedResponses)
}

func TestApplyClientRequestIfNotLeaderAndPeer(t *testing.T) {
	requireT := require.New(t)

	s := &state.State{}
	e, _ := newEngine(s)

	// To set leader.
	_, _, err := e.Apply(types.Command{
		PeerID: peer1ID,
		Cmd: &types.AppendEntriesRequest{
			MessageID:    types.NewMessageID(),
			Term:         1,
			NextLogIndex: 0,
			LastLogTerm:  0,
			Entries: []types.LogItem{
				{Term: 1},
			},
		},
	})
	requireT.NoError(err)

	role, toSend, err := e.Apply(types.Command{
		PeerID: peer2ID,
		Cmd: &types.ClientRequest{
			Data: []byte{0x01},
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Equal(Send{}, toSend)
	requireT.Empty(e.expectedResponses)
}

func TestApplyClientRequestPeersIgnored(t *testing.T) {
	requireT := require.New(t)

	s := &state.State{}
	e, ts := newEngine(s)

	transitionToLeader(requireT, e, transitionToCandidate(requireT, e, ts))

	oldMessageID := types.NewMessageID()
	e.expectedResponses = map[magmatypes.ServerID]types.MessageID{
		peer1ID: oldMessageID,
		peer2ID: oldMessageID,
	}

	role, toSend, err := e.Apply(types.Command{
		Cmd: &types.ClientRequest{
			Data: []byte{0x01},
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, role)
	requireT.NotNil(toSend.Message)

	messageID := toSend.Message.(*types.AppendEntriesRequest).MessageID
	requireT.NotEqual(types.ZeroMessageID, messageID)
	requireT.Equal(Send{
		Recipients: []magmatypes.ServerID{peer3ID, peer4ID},
		Message: &types.AppendEntriesRequest{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 1,
			LastLogTerm:  1,
			Entries: []types.LogItem{
				{
					Term: 1,
					Data: []byte{0x01},
				},
			},
			LeaderCommit: 1,
		},
	}, toSend)
	requireT.Equal(map[magmatypes.ServerID]types.MessageID{
		peer1ID: oldMessageID,
		peer2ID: oldMessageID,
		peer3ID: messageID,
		peer4ID: messageID,
	}, e.expectedResponses)
}

func TestApplyHeartbeatTimeout(t *testing.T) {
	requireT := require.New(t)

	s := &state.State{}
	e, ts := newEngine(s)

	transitionToLeader(requireT, e, transitionToCandidate(requireT, e, ts))

	role, toSend, err := e.Apply(types.Command{
		Cmd: types.HeartbeatTimeout(ts.Add(time.Hour)),
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, role)
	requireT.NotNil(toSend.Message)

	messageID := toSend.Message.(*types.AppendEntriesRequest).MessageID
	requireT.NotEqual(types.ZeroMessageID, messageID)
	requireT.Equal(Send{
		Recipients: peers,
		Message: &types.AppendEntriesRequest{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 1,
			LastLogTerm:  1,
			Entries:      []types.LogItem{},
			LeaderCommit: 1,
		},
	}, toSend)
	requireT.Equal(map[magmatypes.ServerID]types.MessageID{
		peer1ID: messageID,
		peer2ID: messageID,
		peer3ID: messageID,
		peer4ID: messageID,
	}, e.expectedResponses)
}

func TestApplyHeartbeatTimeoutIgnorePeer(t *testing.T) {
	requireT := require.New(t)

	s := &state.State{}
	e, ts := newEngine(s)

	transitionToLeader(requireT, e, transitionToCandidate(requireT, e, ts))

	oldMessageID := types.NewMessageID()
	e.expectedResponses[peer1ID] = oldMessageID

	role, toSend, err := e.Apply(types.Command{
		Cmd: types.HeartbeatTimeout(ts.Add(time.Hour)),
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, role)
	requireT.NotNil(toSend.Message)

	messageID := toSend.Message.(*types.AppendEntriesRequest).MessageID
	requireT.NotEqual(types.ZeroMessageID, messageID)
	requireT.Equal(Send{
		Recipients: []magmatypes.ServerID{peer2ID, peer3ID, peer4ID},
		Message: &types.AppendEntriesRequest{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 1,
			LastLogTerm:  1,
			Entries:      []types.LogItem{},
			LeaderCommit: 1,
		},
	}, toSend)
	requireT.Equal(map[magmatypes.ServerID]types.MessageID{
		peer1ID: oldMessageID,
		peer2ID: messageID,
		peer3ID: messageID,
		peer4ID: messageID,
	}, e.expectedResponses)
}

func TestApplyHeartbeatTimeoutNothingToDo(t *testing.T) {
	requireT := require.New(t)

	s := &state.State{}
	e, ts := newEngine(s)

	transitionToLeader(requireT, e, transitionToCandidate(requireT, e, ts))

	role, toSend, err := e.Apply(types.Command{
		Cmd: types.HeartbeatTimeout(ts.Add(-time.Second)),
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, role)
	requireT.Equal(Send{}, toSend)
	requireT.Equal(map[magmatypes.ServerID]types.MessageID{
		peer1ID: types.ZeroMessageID,
		peer2ID: types.ZeroMessageID,
		peer3ID: types.ZeroMessageID,
		peer4ID: types.ZeroMessageID,
	}, e.expectedResponses)
}

func TestApplyElectionTimeout(t *testing.T) {
	requireT := require.New(t)

	s := &state.State{}
	e, ts := newEngine(s)

	role, toSend, err := e.Apply(types.Command{
		Cmd: types.ElectionTimeout(ts.Add(time.Hour)),
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, role)
	requireT.NotNil(toSend.Message)

	messageID := toSend.Message.(*types.VoteRequest).MessageID
	requireT.NotEqual(types.ZeroMessageID, messageID)
	requireT.Equal(Send{
		Recipients: peers,
		Message: &types.VoteRequest{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 0,
			LastLogTerm:  0,
		},
	}, toSend)
	requireT.Equal(map[magmatypes.ServerID]types.MessageID{
		peer1ID: messageID,
		peer2ID: messageID,
		peer3ID: messageID,
		peer4ID: messageID,
	}, e.expectedResponses)
}

func TestApplyElectionTimeoutExpectationsIgnored(t *testing.T) {
	requireT := require.New(t)

	s := &state.State{}
	e, ts := newEngine(s)

	oldMessageID := types.NewMessageID()
	e.expectedResponses = map[magmatypes.ServerID]types.MessageID{
		peer1ID: oldMessageID,
		peer2ID: oldMessageID,
		peer3ID: oldMessageID,
		peer4ID: oldMessageID,
	}

	role, toSend, err := e.Apply(types.Command{
		Cmd: types.ElectionTimeout(ts.Add(time.Hour)),
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, role)
	requireT.NotNil(toSend.Message)

	messageID := toSend.Message.(*types.VoteRequest).MessageID
	requireT.NotEqual(types.ZeroMessageID, messageID)
	requireT.Equal(Send{
		Recipients: peers,
		Message: &types.VoteRequest{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 0,
			LastLogTerm:  0,
		},
	}, toSend)
	requireT.Equal(map[magmatypes.ServerID]types.MessageID{
		peer1ID: messageID,
		peer2ID: messageID,
		peer3ID: messageID,
		peer4ID: messageID,
	}, e.expectedResponses)
}

func TestApplyElectionTimeoutNothingToDo(t *testing.T) {
	requireT := require.New(t)

	s := &state.State{}
	e, ts := newEngine(s)

	role, toSend, err := e.Apply(types.Command{
		Cmd: types.ElectionTimeout(ts.Add(-time.Hour)),
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Equal(Send{}, toSend)
	requireT.Empty(e.expectedResponses)
}

func TestApplyPeerConnected(t *testing.T) {
	requireT := require.New(t)

	s := &state.State{}
	e, ts := newEngine(s)

	transitionToLeader(requireT, e, transitionToCandidate(requireT, e, ts))

	e.expectedResponses[peer1ID] = types.NewMessageID()

	role, toSend, err := e.Apply(types.Command{
		PeerID: peer1ID,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, role)
	requireT.NotNil(toSend.Message)

	messageID := toSend.Message.(*types.AppendEntriesRequest).MessageID
	requireT.NotEqual(types.ZeroMessageID, messageID)
	requireT.Equal(Send{
		Recipients: []magmatypes.ServerID{peer1ID},
		Message: &types.AppendEntriesRequest{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 1,
			LastLogTerm:  1,
			Entries:      []types.LogItem{},
			LeaderCommit: 1,
		},
	}, toSend)

	requireT.Equal(messageID, e.expectedResponses[peer1ID])
}

func TestApplyPeerConnectedNotLeader(t *testing.T) {
	requireT := require.New(t)

	s := &state.State{}
	e, _ := newEngine(s)

	role, toSend, err := e.Apply(types.Command{
		PeerID: peer1ID,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Equal(Send{}, toSend)
}

func transitionToCandidate(requireT *require.Assertions, e *Engine, ts reactor.TimeAdvancer) types.MessageID {
	role, toSend, err := e.Apply(types.Command{
		Cmd: types.ElectionTimeout(ts.Add(time.Hour)),
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, role)
	requireT.NotNil(toSend.Message)

	messageID := toSend.Message.(*types.VoteRequest).MessageID
	requireT.NotEqual(types.ZeroMessageID, messageID)
	requireT.Equal(Send{
		Recipients: peers,
		Message: &types.VoteRequest{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 0,
			LastLogTerm:  0,
		},
	}, toSend)
	requireT.Equal(map[magmatypes.ServerID]types.MessageID{
		peer1ID: messageID,
		peer2ID: messageID,
		peer3ID: messageID,
		peer4ID: messageID,
	}, e.expectedResponses)

	return messageID
}

func transitionToLeader(requireT *require.Assertions, e *Engine, messageID types.MessageID) {
	var role types.Role
	var toSend Send
	for _, peer := range []magmatypes.ServerID{peer1ID, peer2ID} {
		var err error
		role, toSend, err = e.Apply(types.Command{
			PeerID: peer,
			Cmd: &types.VoteResponse{
				MessageID:   messageID,
				Term:        1,
				VoteGranted: true,
			},
		})
		requireT.NoError(err)

		if peer == peer1ID {
			requireT.Equal(types.ZeroMessageID, e.expectedResponses[peer1ID])
		}
	}

	requireT.Equal(types.RoleLeader, role)
	requireT.NotNil(toSend.Message)

	messageID = toSend.Message.(*types.AppendEntriesRequest).MessageID
	requireT.NotEqual(types.ZeroMessageID, messageID)
	requireT.Equal(Send{
		Recipients: peers,
		Message: &types.AppendEntriesRequest{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 0,
			LastLogTerm:  0,
			Entries: []types.LogItem{
				{Term: 1},
			},
			LeaderCommit: 0,
		},
	}, toSend)

	requireT.Equal(map[magmatypes.ServerID]types.MessageID{
		peer1ID: messageID,
		peer2ID: messageID,
		peer3ID: messageID,
		peer4ID: messageID,
	}, e.expectedResponses)

	for _, peer := range peers {
		var err error
		role, toSend, err = e.Apply(types.Command{
			PeerID: peer,
			Cmd: &types.AppendEntriesResponse{
				MessageID:    messageID,
				Term:         1,
				NextLogIndex: 1,
			},
		})
		requireT.NoError(err)
		requireT.Equal(types.RoleLeader, role)
		requireT.Empty(toSend.Recipients)
		requireT.Equal(types.ZeroMessageID, e.expectedResponses[peer])
	}
}
