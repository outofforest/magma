package engine

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/raft/reactor"
	"github.com/outofforest/magma/raft/state"
	"github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/raft/wire/p2c"
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

	messageID := p2p.NewMessageID()
	role, toSend, err := e.Apply(types.Command{
		PeerID: peer1ID,
		Cmd: &p2p.AppendEntriesRequest{
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
		Message: &p2p.AppendEntriesResponse{
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

	messageID := p2p.NewMessageID()
	e.expectedResponses[peer1ID] = messageID

	role, toSend, err := e.Apply(types.Command{
		PeerID: peer1ID,
		Cmd: &p2p.AppendEntriesResponse{
			MessageID:    messageID,
			Term:         0,
			NextLogIndex: 0,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, role)
	requireT.NotNil(toSend.Message)

	messageID = toSend.Message.(*p2p.AppendEntriesRequest).MessageID
	requireT.NotEqual(p2p.ZeroMessageID, messageID)
	requireT.Equal(Send{
		Recipients: []types.ServerID{peer1ID},
		Message: &p2p.AppendEntriesRequest{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 0,
			LastLogTerm:  0,
			Entries: []state.LogItem{
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

	messageID := p2p.NewMessageID()
	e.expectedResponses[peer2ID] = messageID
	requireT.Equal(p2p.ZeroMessageID, e.expectedResponses[peer1ID])

	role, toSend, err := e.Apply(types.Command{
		PeerID: peer1ID,
		Cmd: &p2p.AppendEntriesResponse{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 1,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, role)
	requireT.Equal(Send{}, toSend)

	requireT.Equal(p2p.ZeroMessageID, e.expectedResponses[peer1ID])
	requireT.Equal(messageID, e.expectedResponses[peer2ID])
}

func TestApplyVoteRequest(t *testing.T) {
	requireT := require.New(t)

	s := &state.State{}
	e, _ := newEngine(s)

	messageID := p2p.NewMessageID()
	role, toSend, err := e.Apply(types.Command{
		PeerID: peer1ID,
		Cmd: &p2p.VoteRequest{
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
		Message: &p2p.VoteResponse{
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
	messageID2 := p2p.NewMessageID()
	e.expectedResponses[peer1ID] = messageID2

	requireT.Equal(map[types.ServerID]p2p.MessageID{
		peer1ID: messageID2,
		peer2ID: messageID,
		peer3ID: messageID,
		peer4ID: messageID,
	}, e.expectedResponses)

	role, toSend, err := e.Apply(types.Command{
		PeerID: peer1ID,
		Cmd: &p2p.VoteResponse{
			MessageID:   messageID,
			Term:        1,
			VoteGranted: true,
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, role)
	requireT.Equal(Send{}, toSend)

	requireT.Equal(map[types.ServerID]p2p.MessageID{
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
		Cmd: &p2c.ClientRequest{
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
		Cmd: &p2c.ClientRequest{
			Data: []byte{0x01},
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, role)
	requireT.NotNil(toSend.Message)

	messageID := toSend.Message.(*p2p.AppendEntriesRequest).MessageID
	requireT.NotEqual(p2p.ZeroMessageID, messageID)
	requireT.Equal(Send{
		Recipients: peers,
		Message: &p2p.AppendEntriesRequest{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 1,
			LastLogTerm:  1,
			Entries: []state.LogItem{
				{
					Term: 1,
					Data: []byte{0x01},
				},
			},
			LeaderCommit: 1,
		},
	}, toSend)
	requireT.Equal(map[types.ServerID]p2p.MessageID{
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
		Cmd: &p2c.ClientRequest{
			Data: []byte{0x01},
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, role)
	requireT.NotNil(toSend.Message)

	messageID := toSend.Message.(*p2p.AppendEntriesRequest).MessageID
	requireT.NotEqual(p2p.ZeroMessageID, messageID)
	requireT.Equal(Send{
		Recipients: peers,
		Message: &p2p.AppendEntriesRequest{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 1,
			LastLogTerm:  1,
			Entries: []state.LogItem{
				{
					Term: 1,
					Data: []byte{0x01},
				},
			},
			LeaderCommit: 1,
		},
	}, toSend)
	requireT.Equal(map[types.ServerID]p2p.MessageID{
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
		Cmd: &p2p.AppendEntriesRequest{
			MessageID:    p2p.NewMessageID(),
			Term:         1,
			NextLogIndex: 0,
			LastLogTerm:  0,
			Entries: []state.LogItem{
				{Term: 1},
			},
		},
	})
	requireT.NoError(err)

	role, toSend, err := e.Apply(types.Command{
		Cmd: &p2c.ClientRequest{
			Data: []byte{0x01},
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Equal(Send{
		Recipients: []types.ServerID{peer1ID},
		Message: &p2c.ClientRequest{
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
		Cmd: &p2p.AppendEntriesRequest{
			MessageID:    p2p.NewMessageID(),
			Term:         1,
			NextLogIndex: 0,
			LastLogTerm:  0,
			Entries: []state.LogItem{
				{Term: 1},
			},
		},
	})
	requireT.NoError(err)

	role, toSend, err := e.Apply(types.Command{
		PeerID: peer2ID,
		Cmd: &p2c.ClientRequest{
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

	oldMessageID := p2p.NewMessageID()
	e.expectedResponses = map[types.ServerID]p2p.MessageID{
		peer1ID: oldMessageID,
		peer2ID: oldMessageID,
	}

	role, toSend, err := e.Apply(types.Command{
		Cmd: &p2c.ClientRequest{
			Data: []byte{0x01},
		},
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, role)
	requireT.NotNil(toSend.Message)

	messageID := toSend.Message.(*p2p.AppendEntriesRequest).MessageID
	requireT.NotEqual(p2p.ZeroMessageID, messageID)
	requireT.Equal(Send{
		Recipients: []types.ServerID{peer3ID, peer4ID},
		Message: &p2p.AppendEntriesRequest{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 1,
			LastLogTerm:  1,
			Entries: []state.LogItem{
				{
					Term: 1,
					Data: []byte{0x01},
				},
			},
			LeaderCommit: 1,
		},
	}, toSend)
	requireT.Equal(map[types.ServerID]p2p.MessageID{
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

	messageID := toSend.Message.(*p2p.AppendEntriesRequest).MessageID
	requireT.NotEqual(p2p.ZeroMessageID, messageID)
	requireT.Equal(Send{
		Recipients: peers,
		Message: &p2p.AppendEntriesRequest{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 1,
			LastLogTerm:  1,
			Entries:      []state.LogItem{},
			LeaderCommit: 1,
		},
	}, toSend)
	requireT.Equal(map[types.ServerID]p2p.MessageID{
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

	oldMessageID := p2p.NewMessageID()
	e.expectedResponses[peer1ID] = oldMessageID

	role, toSend, err := e.Apply(types.Command{
		Cmd: types.HeartbeatTimeout(ts.Add(time.Hour)),
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, role)
	requireT.NotNil(toSend.Message)

	messageID := toSend.Message.(*p2p.AppendEntriesRequest).MessageID
	requireT.NotEqual(p2p.ZeroMessageID, messageID)
	requireT.Equal(Send{
		Recipients: []types.ServerID{peer2ID, peer3ID, peer4ID},
		Message: &p2p.AppendEntriesRequest{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 1,
			LastLogTerm:  1,
			Entries:      []state.LogItem{},
			LeaderCommit: 1,
		},
	}, toSend)
	requireT.Equal(map[types.ServerID]p2p.MessageID{
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
	requireT.Equal(map[types.ServerID]p2p.MessageID{
		peer1ID: p2p.ZeroMessageID,
		peer2ID: p2p.ZeroMessageID,
		peer3ID: p2p.ZeroMessageID,
		peer4ID: p2p.ZeroMessageID,
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

	messageID := toSend.Message.(*p2p.VoteRequest).MessageID
	requireT.NotEqual(p2p.ZeroMessageID, messageID)
	requireT.Equal(Send{
		Recipients: peers,
		Message: &p2p.VoteRequest{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 0,
			LastLogTerm:  0,
		},
	}, toSend)
	requireT.Equal(map[types.ServerID]p2p.MessageID{
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

	oldMessageID := p2p.NewMessageID()
	e.expectedResponses = map[types.ServerID]p2p.MessageID{
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

	messageID := toSend.Message.(*p2p.VoteRequest).MessageID
	requireT.NotEqual(p2p.ZeroMessageID, messageID)
	requireT.Equal(Send{
		Recipients: peers,
		Message: &p2p.VoteRequest{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 0,
			LastLogTerm:  0,
		},
	}, toSend)
	requireT.Equal(map[types.ServerID]p2p.MessageID{
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

	e.expectedResponses[peer1ID] = p2p.NewMessageID()

	role, toSend, err := e.Apply(types.Command{
		PeerID: peer1ID,
		Cmd:    peer1ID,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleLeader, role)
	requireT.NotNil(toSend.Message)

	messageID := toSend.Message.(*p2p.AppendEntriesRequest).MessageID
	requireT.NotEqual(p2p.ZeroMessageID, messageID)
	requireT.Equal(Send{
		Recipients: []types.ServerID{peer1ID},
		Message: &p2p.AppendEntriesRequest{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 1,
			LastLogTerm:  1,
			Entries:      []state.LogItem{},
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
		Cmd:    peer1ID,
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleFollower, role)
	requireT.Equal(Send{}, toSend)
}

func transitionToCandidate(requireT *require.Assertions, e *Engine, ts reactor.TimeAdvancer) p2p.MessageID {
	role, toSend, err := e.Apply(types.Command{
		Cmd: types.ElectionTimeout(ts.Add(time.Hour)),
	})
	requireT.NoError(err)
	requireT.Equal(types.RoleCandidate, role)
	requireT.NotNil(toSend.Message)

	messageID := toSend.Message.(*p2p.VoteRequest).MessageID
	requireT.NotEqual(p2p.ZeroMessageID, messageID)
	requireT.Equal(Send{
		Recipients: peers,
		Message: &p2p.VoteRequest{
			MessageID:    messageID,
			Term:         1,
			NextLogIndex: 0,
			LastLogTerm:  0,
		},
	}, toSend)
	requireT.Equal(map[types.ServerID]p2p.MessageID{
		peer1ID: messageID,
		peer2ID: messageID,
		peer3ID: messageID,
		peer4ID: messageID,
	}, e.expectedResponses)

	return messageID
}

func transitionToLeader(requireT *require.Assertions, e *Engine, messageID p2p.MessageID) {
	var role types.Role
	var toSend Send
	for _, peer := range []types.ServerID{peer1ID, peer2ID} {
		var err error
		role, toSend, err = e.Apply(types.Command{
			PeerID: peer,
			Cmd: &p2p.VoteResponse{
				MessageID:   messageID,
				Term:        1,
				VoteGranted: true,
			},
		})
		requireT.NoError(err)

		if peer == peer1ID {
			requireT.Equal(p2p.ZeroMessageID, e.expectedResponses[peer1ID])
		}
	}

	requireT.Equal(types.RoleLeader, role)
	requireT.NotNil(toSend.Message)

	messageID = toSend.Message.(*p2p.AppendEntriesRequest).MessageID
	requireT.NotEqual(p2p.ZeroMessageID, messageID)
	requireT.Equal(Send{
		Recipients: peers,
		Message: &p2p.AppendEntriesRequest{
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

	requireT.Equal(map[types.ServerID]p2p.MessageID{
		peer1ID: messageID,
		peer2ID: messageID,
		peer3ID: messageID,
		peer4ID: messageID,
	}, e.expectedResponses)

	for _, peer := range peers {
		var err error
		role, toSend, err = e.Apply(types.Command{
			PeerID: peer,
			Cmd: &p2p.AppendEntriesResponse{
				MessageID:    messageID,
				Term:         1,
				NextLogIndex: 1,
			},
		})
		requireT.NoError(err)
		requireT.Equal(types.RoleLeader, role)
		requireT.Empty(toSend.Recipients)
		requireT.Equal(p2p.ZeroMessageID, e.expectedResponses[peer])
	}
}
