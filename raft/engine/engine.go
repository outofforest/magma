package engine

import (
	"time"

	"github.com/pkg/errors"

	"github.com/outofforest/magma/raft/reactor"
	"github.com/outofforest/magma/raft/types"
	magmatypes "github.com/outofforest/magma/types"
)

// Result is the result of applied command.
type Result struct {
	// PeerID is the recipient, if equal to `ZeroServerID` message is broadcasted to all connected peers.
	Recipients []magmatypes.ServerID
	// Message is message to send.
	Message any
	// CommitInfo reports latest committed log index.
	CommitInfo types.CommitInfo
	// LeaderID is the ID of current leader.
	LeaderID magmatypes.ServerID
}

// New creates new engine.
func New(r *reactor.Reactor, peers []magmatypes.ServerID) *Engine {
	return &Engine{
		reactor:            r,
		peers:              peers,
		expectedResponses:  map[magmatypes.ServerID]types.MessageID{},
		messageIDGenerator: types.MessageID(time.Now().Unix()),
	}
}

// Engine is responsible for applying commands to reactor, and transform response into send instructions.
// It also tracks the expected responses to not send redundant messages.
type Engine struct {
	reactor            *reactor.Reactor
	peers              []magmatypes.ServerID
	expectedResponses  map[magmatypes.ServerID]types.MessageID
	messageIDGenerator types.MessageID
}

// Apply applied command and returns message to be sent to peers.
func (e *Engine) Apply(cmd types.Command) (types.Role, Result, error) {
	var messageID types.MessageID
	var result Result

	role := e.reactor.Role()

	switch {
	case cmd.PeerID == magmatypes.ZeroServerID:
		switch c := cmd.Cmd.(type) {
		case *types.ClientRequest:
			req, commitInfo, err := e.reactor.ApplyClientRequest(c)
			if err != nil {
				return 0, Result{}, err
			}
			messageID, result = e.broadcastAppendEntriesRequest(req)
			result.CommitInfo = commitInfo
		case types.HeartbeatTimeout:
			req, err := e.reactor.ApplyHeartbeatTimeout(time.Time(c))
			if err != nil {
				return 0, Result{}, err
			}
			messageID, result = e.broadcastAppendEntriesRequest(req)
		case types.ElectionTimeout:
			req, commitInfo, err := e.reactor.ApplyElectionTimeout(time.Time(c))
			if err != nil {
				return 0, Result{}, err
			}
			messageID, result = e.broadcastVoteRequest(req)
			result.CommitInfo = commitInfo
		default:
			return 0, Result{}, errors.Errorf("unexpected message type %T", c)
		}
	case cmd.Cmd == nil:
		req, err := e.reactor.ApplyPeerConnected(cmd.PeerID)
		if err != nil {
			return 0, Result{}, err
		}
		e.expectedResponses[cmd.PeerID] = types.ZeroMessageID
		messageID, result = e.unicastAppendEntriesRequest(cmd.PeerID, e.newMessageID(), req)
	default:
		switch c := cmd.Cmd.(type) {
		case *types.AppendEntriesRequest:
			resp, commitInfo, err := e.reactor.ApplyAppendEntriesRequest(cmd.PeerID, c)
			if err != nil {
				return 0, Result{}, err
			}
			result = e.unicastAppendEntriesResponse(cmd.PeerID, c.MessageID, resp)
			result.CommitInfo = commitInfo
		case *types.AppendEntriesResponse:
			if !e.isExpected(cmd.PeerID, c.MessageID) {
				return e.reactor.Role(), Result{}, nil
			}

			req, commitInfo, err := e.reactor.ApplyAppendEntriesResponse(cmd.PeerID, c)
			if err != nil {
				return 0, Result{}, err
			}
			messageID, result = e.unicastAppendEntriesRequest(cmd.PeerID, c.MessageID, req)
			result.CommitInfo = commitInfo
		case *types.VoteRequest:
			resp, err := e.reactor.ApplyVoteRequest(cmd.PeerID, c)
			if err != nil {
				return 0, Result{}, err
			}
			result = e.unicastVoteResponse(cmd.PeerID, c.MessageID, resp)
		case *types.VoteResponse:
			if !e.isExpected(cmd.PeerID, c.MessageID) {
				return e.reactor.Role(), Result{}, nil
			}

			req, commitInfo, err := e.reactor.ApplyVoteResponse(cmd.PeerID, c)
			if err != nil {
				return 0, Result{}, err
			}
			messageID, result = e.broadcastAppendEntriesRequest(req)
			result.CommitInfo = commitInfo
		default:
			return 0, Result{}, errors.Errorf("unexpected message type %T", c)
		}
	}

	result.LeaderID = e.reactor.LeaderID()

	newRole := e.reactor.Role()
	if newRole != role {
		clear(e.expectedResponses)
	}

	if messageID == types.ZeroMessageID {
		return newRole, result, nil
	}

	// FIXME (wojciech): Avoid allocation.
	recipients := make([]magmatypes.ServerID, 0, len(e.peers))
	for _, p := range result.Recipients {
		if e.expectedResponses[p] == types.ZeroMessageID {
			e.expectedResponses[p] = messageID
			recipients = append(recipients, p)
		}
	}
	result.Recipients = recipients

	return newRole, result, nil
}

func (e *Engine) unicastAppendEntriesRequest(
	peerID magmatypes.ServerID,
	messageID types.MessageID,
	req *types.AppendEntriesRequest,
) (types.MessageID, Result) {
	if req == nil {
		return types.ZeroMessageID, Result{}
	}
	req.MessageID = messageID
	return messageID, Result{
		// FIXME (wojciech): Avoid allocation.
		Recipients: []magmatypes.ServerID{peerID},
		Message:    req,
	}
}

func (e *Engine) broadcastAppendEntriesRequest(req *types.AppendEntriesRequest) (types.MessageID, Result) {
	if req == nil {
		return types.ZeroMessageID, Result{}
	}
	req.MessageID = e.newMessageID()
	return req.MessageID, Result{
		Recipients: e.peers,
		Message:    req,
	}
}

func (e *Engine) unicastAppendEntriesResponse(
	peerID magmatypes.ServerID,
	messageID types.MessageID,
	resp *types.AppendEntriesResponse,
) Result {
	if resp == nil {
		return Result{}
	}
	resp.MessageID = messageID
	return Result{
		// FIXME (wojciech): Avoid allocation.
		Recipients: []magmatypes.ServerID{peerID},
		Message:    resp,
	}
}

func (e *Engine) broadcastVoteRequest(req *types.VoteRequest) (types.MessageID, Result) {
	if req == nil {
		return types.ZeroMessageID, Result{}
	}
	req.MessageID = e.newMessageID()
	return req.MessageID, Result{
		Recipients: e.peers,
		Message:    req,
	}
}

func (e *Engine) unicastVoteResponse(
	peerID magmatypes.ServerID,
	messageID types.MessageID,
	resp *types.VoteResponse,
) Result {
	if resp == nil {
		return Result{}
	}
	resp.MessageID = messageID
	return Result{
		// FIXME (wojciech): Avoid allocation.
		Recipients: []magmatypes.ServerID{peerID},
		Message:    resp,
	}
}

func (e *Engine) isExpected(peerID magmatypes.ServerID, messageID types.MessageID) bool {
	if e.expectedResponses[peerID] != messageID {
		return false
	}
	e.expectedResponses[peerID] = types.ZeroMessageID
	return true
}

func (e *Engine) newMessageID() types.MessageID {
	e.messageIDGenerator++
	if e.messageIDGenerator == 0 {
		e.messageIDGenerator = 1
	}

	return e.messageIDGenerator
}
