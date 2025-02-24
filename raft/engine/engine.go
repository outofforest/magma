package engine

import (
	"time"

	"github.com/pkg/errors"

	"github.com/outofforest/magma/raft/reactor"
	"github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/raft/wire/p2c"
	"github.com/outofforest/magma/raft/wire/p2p"
)

// Send is an instruction to send messages to peers.
type Send struct {
	// PeerID is the recipient, if equal to `ZeroServerID` message is broadcasted to all connected peers.
	Recipients []types.ServerID
	// Message is message to send.
	Message any
}

// New creates new engine.
func New(r *reactor.Reactor, peers []types.ServerID) *Engine {
	return &Engine{
		reactor:           r,
		peers:             peers,
		expectedResponses: map[types.ServerID]p2p.MessageID{},
	}
}

// Engine is responsible for applying commands to reactor, and transform response into send instructions.
// It also tracks the expected responses to not send redundant messages.
type Engine struct {
	reactor           *reactor.Reactor
	peers             []types.ServerID
	expectedResponses map[types.ServerID]p2p.MessageID
}

// Apply applied command and returns message to be sent to peers.
func (e *Engine) Apply(cmd types.Command) (types.Role, Send, error) {
	var messageID p2p.MessageID
	var toSend Send

	role := e.reactor.Role()

	switch {
	case cmd.PeerID == types.ZeroServerID:
		switch c := cmd.Cmd.(type) {
		case *p2c.ClientRequest:
			leaderID := e.reactor.LeaderID()
			if leaderID == types.ZeroServerID {
				return e.reactor.Role(), Send{}, nil
			}
			if leaderID == e.reactor.ID() {
				req, err := e.reactor.ApplyClientRequest(c)
				if err != nil {
					return 0, Send{}, err
				}
				messageID, toSend = e.broadcastAppendEntriesRequest(req)
				break
			}

			toSend = Send{
				Recipients: []types.ServerID{leaderID},
				Message:    c,
			}
		case types.HeartbeatTimeout:
			req, err := e.reactor.ApplyHeartbeatTimeout(time.Time(c))
			if err != nil {
				return 0, Send{}, err
			}
			messageID, toSend = e.broadcastAppendEntriesRequest(req)
		case types.ElectionTimeout:
			req, err := e.reactor.ApplyElectionTimeout(time.Time(c))
			if err != nil {
				return 0, Send{}, err
			}
			messageID, toSend = e.broadcastVoteRequest(req)
		default:
			return 0, Send{}, errors.Errorf("unexpected message type %T", c)
		}
	case cmd.Cmd == nil:
		req, err := e.reactor.ApplyPeerConnected(cmd.PeerID)
		if err != nil {
			return 0, Send{}, err
		}
		e.expectedResponses[cmd.PeerID] = p2p.ZeroMessageID
		messageID, toSend = e.unicastAppendEntriesRequest(cmd.PeerID, req)
	default:
		switch c := cmd.Cmd.(type) {
		case *p2p.AppendEntriesRequest:
			resp, err := e.reactor.ApplyAppendEntriesRequest(cmd.PeerID, c)
			if err != nil {
				return 0, Send{}, err
			}
			toSend = e.unicastAppendEntriesResponse(cmd.PeerID, c.MessageID, resp)
		case *p2p.AppendEntriesResponse:
			if !e.isExpected(cmd.PeerID, c.MessageID) {
				return e.reactor.Role(), Send{}, nil
			}

			req, err := e.reactor.ApplyAppendEntriesResponse(cmd.PeerID, c)
			if err != nil {
				return 0, Send{}, err
			}
			messageID, toSend = e.unicastAppendEntriesRequest(cmd.PeerID, req)
		case *p2p.VoteRequest:
			resp, err := e.reactor.ApplyVoteRequest(cmd.PeerID, c)
			if err != nil {
				return 0, Send{}, err
			}
			toSend = e.unicastVoteResponse(cmd.PeerID, c.MessageID, resp)
		case *p2p.VoteResponse:
			if !e.isExpected(cmd.PeerID, c.MessageID) {
				return e.reactor.Role(), Send{}, nil
			}

			req, err := e.reactor.ApplyVoteResponse(cmd.PeerID, c)
			if err != nil {
				return 0, Send{}, err
			}
			messageID, toSend = e.broadcastAppendEntriesRequest(req)
		case *p2c.ClientRequest:
			leaderID := e.reactor.LeaderID()
			if leaderID == types.ZeroServerID {
				return e.reactor.Role(), Send{}, nil
			}
			if leaderID == e.reactor.ID() {
				req, err := e.reactor.ApplyClientRequest(c)
				if err != nil {
					return 0, Send{}, err
				}
				messageID, toSend = e.broadcastAppendEntriesRequest(req)
				break
			}

			// We redirect request to leader, but only once, to avoid infinite hops.
			return e.reactor.Role(), Send{}, nil
		default:
			return 0, Send{}, errors.Errorf("unexpected message type %T", c)
		}
	}

	newRole := e.reactor.Role()
	if newRole != role {
		clear(e.expectedResponses)
	}

	if messageID == p2p.ZeroMessageID {
		return newRole, toSend, nil
	}

	// FIXME (wojciech): Avoid allocation.
	recipients := make([]types.ServerID, 0, len(e.peers))
	for _, p := range toSend.Recipients {
		if e.expectedResponses[p] == p2p.ZeroMessageID {
			e.expectedResponses[p] = messageID
			recipients = append(recipients, p)
		}
	}
	toSend.Recipients = recipients

	return newRole, toSend, nil
}

func (e *Engine) unicastAppendEntriesRequest(
	peerID types.ServerID,
	req *p2p.AppendEntriesRequest,
) (p2p.MessageID, Send) {
	if req == nil {
		return p2p.ZeroMessageID, Send{}
	}
	req.MessageID = p2p.NewMessageID()
	return req.MessageID, Send{
		// FIXME (wojciech): Avoid allocation.
		Recipients: []types.ServerID{peerID},
		Message:    req,
	}
}

func (e *Engine) broadcastAppendEntriesRequest(req *p2p.AppendEntriesRequest) (p2p.MessageID, Send) {
	if req == nil {
		return p2p.ZeroMessageID, Send{}
	}
	req.MessageID = p2p.NewMessageID()
	return req.MessageID, Send{
		Recipients: e.peers,
		Message:    req,
	}
}

func (e *Engine) unicastAppendEntriesResponse(
	peerID types.ServerID,
	messageID p2p.MessageID,
	resp *p2p.AppendEntriesResponse,
) Send {
	if resp == nil {
		return Send{}
	}
	resp.MessageID = messageID
	return Send{
		// FIXME (wojciech): Avoid allocation.
		Recipients: []types.ServerID{peerID},
		Message:    resp,
	}
}

func (e *Engine) broadcastVoteRequest(req *p2p.VoteRequest) (p2p.MessageID, Send) {
	if req == nil {
		return p2p.ZeroMessageID, Send{}
	}
	req.MessageID = p2p.NewMessageID()
	return req.MessageID, Send{
		Recipients: e.peers,
		Message:    req,
	}
}

func (e *Engine) unicastVoteResponse(peerID types.ServerID, messageID p2p.MessageID, resp *p2p.VoteResponse) Send {
	if resp == nil {
		return Send{}
	}
	resp.MessageID = messageID
	return Send{
		// FIXME (wojciech): Avoid allocation.
		Recipients: []types.ServerID{peerID},
		Message:    resp,
	}
}

func (e *Engine) isExpected(peerID types.ServerID, messageID p2p.MessageID) bool {
	if e.expectedResponses[peerID] != messageID {
		return false
	}
	e.expectedResponses[peerID] = p2p.ZeroMessageID
	return true
}
