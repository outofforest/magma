package reactor

// FIXME (wojciech): Limit the amount of data sent in single message.
// FIXME (wojciech): Adding new peers.
// FIXME (wojciech): Preventing server from being a leader.
// FIXME (wojciech): Rebalance reactors across servers.
// FIXME (wojciech): Read and write state and logs.

import (
	"sort"
	"time"

	"github.com/pkg/errors"

	"github.com/outofforest/magma/raft/state"
	"github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/raft/wire/p2c"
	"github.com/outofforest/magma/raft/wire/p2p"
)

// New creates new reactor of raft consensus algorithm.
func New(id types.ServerID, servers []types.ServerID, s *state.State) *Reactor {
	r := &Reactor{
		id:             id,
		peers:          make([]types.ServerID, 0, len(servers)),
		state:          s,
		lastLogTerm:    s.LastLogTerm(),
		nextLogIndex:   s.NextLogIndex(),
		callInProgress: map[types.ServerID]p2p.MessageID{},
		nextIndex:      map[types.ServerID]types.Index{},
		matchIndex:     map[types.ServerID]types.Index{},
	}
	for _, s := range servers {
		if s != id {
			r.peers = append(r.peers, s)
		}
	}
	r.minority = (len(r.peers) + 1) / 2
	r.transitionToFollower()

	return r
}

// Reactor implements Raft's state machine.
type Reactor struct {
	id       types.ServerID
	peers    []types.ServerID
	minority int
	state    *state.State

	role           types.Role
	lastLogTerm    types.Term
	nextLogIndex   types.Index
	committedCount types.Index

	// Follower and candidate specific.
	electionTime time.Time

	// Candidate specific.
	votedForMe int

	// Candidate and leader specific.
	callInProgress map[types.ServerID]p2p.MessageID

	// Leader specific.
	nextIndex     map[types.ServerID]types.Index
	matchIndex    map[types.ServerID]types.Index
	heartBeatTime time.Time
}

// Apply processes incoming raft messages and transitions the reactor's role or
// generates response messages as needed based on the message type.
// It handles various Raft protocol-specific message types such as AppendEntries,
// RequestVote, ClientRequests, and timeout events. In case of an unexpected
// message type, it returns an error. This method ensures the reactor's state
// remains consistent with the Raft protocol and returns the updated role,
// any outgoing messages, and an error if applicable.
func (r *Reactor) Apply(msg p2p.Message) (types.Role, []p2p.Message, error) {
	var messages []p2p.Message
	var err error
	switch m := msg.Msg.(type) {
	case p2p.AppendEntriesRequest:
		messages, err = r.applyAppendEntriesRequest(msg.ID, msg.PeerID, m)
	case p2p.AppendEntriesResponse:
		messages, err = r.applyAppendEntriesResponse(msg.ID, msg.PeerID, m)
	case p2p.RequestVoteRequest:
		messages, err = r.applyRequestVoteRequest(msg.ID, msg.PeerID, m)
	case p2p.RequestVoteResponse:
		messages, err = r.applyRequestVoteResponse(m)
	case p2c.ClientRequest:
		messages, err = r.applyClientRequest(m)
	case types.HeartbeatTimeout:
		messages, err = r.applyHeartbeatTimeout(m)
	case types.ElectionTimeout:
		messages, err = r.applyElectionTimeout(m)
	case types.ServerID:
		messages, err = r.applyPeerConnected(m)
	default:
		return types.RoleFollower, nil, errors.Errorf("unexpected message type %T", m)
	}

	if err != nil {
		return types.RoleFollower, nil, err
	}

	return r.role, messages, nil
}

func (r *Reactor) applyAppendEntriesRequest(
	id p2p.MessageID,
	peerID types.ServerID,
	m p2p.AppendEntriesRequest,
) ([]p2p.Message, error) {
	if r.role == types.RoleLeader && m.Term == r.state.CurrentTerm() {
		return nil, errors.New("bug in protocol")
	}

	if err := r.maybeTransitionToFollower(m.Term, true); err != nil {
		return nil, err
	}

	resp, err := r.handleAppendEntriesRequest(m)
	if err != nil {
		return nil, err
	}
	return []p2p.Message{
		{
			ID:     id,
			PeerID: peerID,
			Msg:    resp,
		},
	}, nil
}

func (r *Reactor) applyAppendEntriesResponse(
	id p2p.MessageID,
	peerID types.ServerID,
	m p2p.AppendEntriesResponse,
) ([]p2p.Message, error) {
	if r.role != types.RoleLeader {
		return nil, nil
	}

	if r.callInProgress[peerID] != id {
		return nil, nil
	}

	if err := r.maybeTransitionToFollower(m.Term, false); err != nil {
		return nil, err
	}

	r.nextIndex[peerID] = m.NextLogIndex
	if !m.Success {
		resp, err := r.newAppendEntriesRequest(r.nextIndex[peerID])
		if err != nil {
			return nil, err
		}

		messageID := p2p.NewMessageID()
		r.callInProgress[peerID] = messageID

		return []p2p.Message{
			{
				ID:     messageID,
				PeerID: peerID,
				Msg:    resp,
			},
		}, nil
	}

	if r.matchIndex[peerID] != m.NextLogIndex {
		r.matchIndex[peerID] = m.NextLogIndex
		r.committedCount = r.computeCommitedCount()
	}

	if r.nextIndex[peerID] < r.nextLogIndex {
		resp, err := r.newAppendEntriesRequest(r.nextIndex[peerID])
		if err != nil {
			return nil, err
		}

		messageID := p2p.NewMessageID()
		r.callInProgress[peerID] = messageID

		return []p2p.Message{
			{
				ID:     messageID,
				PeerID: peerID,
				Msg:    resp,
			},
		}, nil
	}

	r.callInProgress[peerID] = p2p.ZeroMessageID

	return nil, nil
}

func (r *Reactor) applyRequestVoteRequest(
	id p2p.MessageID,
	peerID types.ServerID,
	m p2p.RequestVoteRequest,
) ([]p2p.Message, error) {
	if err := r.maybeTransitionToFollower(m.Term, false); err != nil {
		return nil, err
	}

	resp, err := r.handleRequestVoteRequest(m)
	if err != nil {
		return nil, err
	}
	return []p2p.Message{
		{
			ID:     id,
			PeerID: peerID,
			Msg:    resp,
		},
	}, nil
}

func (r *Reactor) applyRequestVoteResponse(m p2p.RequestVoteResponse) ([]p2p.Message, error) {
	if err := r.maybeTransitionToFollower(m.Term, false); err != nil {
		return nil, err
	}

	if r.role != types.RoleCandidate || m.Term != r.state.CurrentTerm() || !m.VoteGranted {
		return nil, nil
	}

	r.votedForMe++
	if r.votedForMe <= r.minority {
		return nil, nil
	}

	return r.transitionToLeader()
}

func (r *Reactor) applyClientRequest(m p2c.ClientRequest) ([]p2p.Message, error) {
	if r.role != types.RoleLeader {
		// FiXME (wojciech): Redirect client to leader.
		return nil, nil
	}

	logItem := state.LogItem{
		Term: r.state.CurrentTerm(),
		Data: m.Data,
	}

	nextLogIndex := r.nextLogIndex
	var success bool
	var err error
	r.lastLogTerm, r.nextLogIndex, success, err = r.state.Append(r.nextLogIndex, r.state.CurrentTerm(),
		[]state.LogItem{logItem})
	if err != nil {
		return nil, err
	}
	if !success {
		return nil, errors.New("bug in protocol")
	}

	r.matchIndex[r.id] = r.nextLogIndex

	r.heartBeatTime = time.Now()

	if len(r.peers) == 0 {
		r.committedCount = r.computeCommitedCount()
		return nil, nil
	}

	messages := make([]p2p.Message, 0, len(r.peers))
	msg, err := r.newAppendEntriesRequest(nextLogIndex)
	if err != nil {
		return nil, err
	}

	for _, peerID := range r.peers {
		if r.callInProgress[peerID] != p2p.ZeroMessageID {
			continue
		}

		messageID := p2p.NewMessageID()

		r.callInProgress[peerID] = messageID

		messages = append(messages, p2p.Message{
			ID:     messageID,
			PeerID: peerID,
			Msg:    msg,
		})
	}

	return messages, nil
}

func (r *Reactor) applyHeartbeatTimeout(m types.HeartbeatTimeout) ([]p2p.Message, error) {
	if r.role != types.RoleLeader || m.Time.Before(r.heartBeatTime) {
		return nil, nil
	}

	r.heartBeatTime = time.Now()

	if len(r.peers) == 0 {
		return nil, nil
	}

	messages := make([]p2p.Message, 0, len(r.peers))
	msg, err := r.newAppendEntriesRequest(r.nextLogIndex)
	if err != nil {
		return nil, err
	}

	for _, peerID := range r.peers {
		if r.callInProgress[peerID] != p2p.ZeroMessageID {
			continue
		}

		messageID := p2p.NewMessageID()
		r.callInProgress[peerID] = messageID

		messages = append(messages, p2p.Message{
			ID:     messageID,
			PeerID: peerID,
			Msg:    msg,
		})
	}

	return messages, nil
}

func (r *Reactor) applyElectionTimeout(m types.ElectionTimeout) ([]p2p.Message, error) {
	if r.role == types.RoleLeader || m.Time.Before(r.electionTime) {
		return nil, nil
	}

	return r.transitionToCandidate()
}

func (r *Reactor) applyPeerConnected(peerID types.ServerID) ([]p2p.Message, error) {
	if r.role != types.RoleLeader {
		return nil, nil
	}

	resp, err := r.newAppendEntriesRequest(r.nextLogIndex)
	if err != nil {
		return nil, err
	}

	messageID := p2p.NewMessageID()

	r.nextIndex[peerID] = r.nextLogIndex
	r.matchIndex[peerID] = 0
	r.callInProgress[peerID] = messageID

	return []p2p.Message{
		{
			ID:     messageID,
			PeerID: peerID,
			Msg:    resp,
		},
	}, nil
}

func (r *Reactor) maybeTransitionToFollower(term types.Term, onAppendEntryRequest bool) error {
	if term < r.state.CurrentTerm() || (term == r.state.CurrentTerm() && !onAppendEntryRequest) {
		return nil
	}

	if term > r.state.CurrentTerm() {
		if err := r.state.SetCurrentTerm(term); err != nil {
			return err
		}
	}

	if r.role != types.RoleFollower {
		r.transitionToFollower()
	}

	return nil
}

func (r *Reactor) transitionToFollower() {
	r.role = types.RoleFollower
	r.votedForMe = 0
	r.electionTime = time.Now()
	clear(r.nextIndex)
	clear(r.matchIndex)
	clear(r.callInProgress)
}

func (r *Reactor) transitionToCandidate() ([]p2p.Message, error) {
	if err := r.state.SetCurrentTerm(r.state.CurrentTerm() + 1); err != nil {
		return nil, err
	}
	granted, err := r.state.VoteFor(r.id)
	if err != nil {
		return nil, err
	}
	if !granted {
		return nil, errors.New("bug in protocol")
	}

	r.role = types.RoleCandidate
	r.votedForMe = 1
	r.electionTime = time.Now()
	clear(r.nextIndex)
	clear(r.matchIndex)

	if len(r.peers) == 0 {
		return r.transitionToLeader()
	}

	messages := make([]p2p.Message, 0, len(r.peers))
	msg := p2p.RequestVoteRequest{
		Term:         r.state.CurrentTerm(),
		CandidateID:  r.id,
		NextLogIndex: r.nextLogIndex,
		LastLogTerm:  r.lastLogTerm,
	}
	for _, peerID := range r.peers {
		messageID := p2p.NewMessageID()

		r.callInProgress[peerID] = messageID

		messages = append(messages, p2p.Message{
			ID:     messageID,
			PeerID: peerID,
			Msg:    msg,
		})
	}

	return messages, nil
}

func (r *Reactor) transitionToLeader() ([]p2p.Message, error) {
	r.role = types.RoleLeader
	clear(r.matchIndex)
	r.matchIndex[r.id] = r.nextLogIndex
	r.heartBeatTime = time.Now()

	if len(r.peers) == 0 {
		r.committedCount = r.computeCommitedCount()
		return nil, nil
	}

	messages := make([]p2p.Message, 0, len(r.peers))

	msg, err := r.newAppendEntriesRequest(r.nextLogIndex)
	if err != nil {
		return nil, err
	}
	msg.LeaderCommit = r.committedCount
	if msg.LeaderCommit > r.committedCount {
		r.committedCount = msg.LeaderCommit
		if r.committedCount > r.nextLogIndex {
			r.committedCount = r.nextLogIndex
		}
	}

	for _, peerID := range r.peers {
		messageID := p2p.NewMessageID()

		r.nextIndex[peerID] = r.nextLogIndex
		r.callInProgress[peerID] = messageID

		messages = append(messages, p2p.Message{
			ID:     messageID,
			PeerID: peerID,
			Msg:    msg,
		})
	}

	return messages, nil
}

func (r *Reactor) newAppendEntriesRequest(nextLogIndex types.Index) (p2p.AppendEntriesRequest, error) {
	lastLogTerm, entries, err := r.state.Entries(nextLogIndex)
	if err != nil {
		return p2p.AppendEntriesRequest{}, err
	}
	return p2p.AppendEntriesRequest{
		Term:         r.state.CurrentTerm(),
		NextLogIndex: nextLogIndex,
		LastLogTerm:  lastLogTerm,
		Entries:      entries,
		LeaderCommit: r.committedCount,
	}, nil
}

func (r *Reactor) handleAppendEntriesRequest(req p2p.AppendEntriesRequest) (p2p.AppendEntriesResponse, error) {
	resp := p2p.AppendEntriesResponse{
		Term:         r.state.CurrentTerm(),
		NextLogIndex: r.nextLogIndex,
	}
	if req.Term < r.state.CurrentTerm() {
		return resp, nil
	}

	var success bool
	var err error
	r.lastLogTerm, r.nextLogIndex, success, err = r.state.Append(req.NextLogIndex, req.LastLogTerm, req.Entries)
	if err != nil {
		return p2p.AppendEntriesResponse{}, err
	}

	if success {
		r.electionTime = time.Now()
		if req.LeaderCommit > r.committedCount {
			r.committedCount = req.LeaderCommit
			if r.committedCount > r.nextLogIndex {
				r.committedCount = r.nextLogIndex
			}
		}
	}

	resp.NextLogIndex = r.nextLogIndex
	resp.Success = success
	return resp, nil
}

func (r *Reactor) handleRequestVoteRequest(req p2p.RequestVoteRequest) (p2p.RequestVoteResponse, error) {
	if req.Term < r.state.CurrentTerm() || r.lastLogTerm > req.LastLogTerm ||
		(r.lastLogTerm == req.LastLogTerm && r.nextLogIndex > req.NextLogIndex) {
		return p2p.RequestVoteResponse{Term: r.state.CurrentTerm()}, nil
	}

	granted, err := r.state.VoteFor(req.CandidateID)
	if err != nil {
		return p2p.RequestVoteResponse{}, err
	}
	if granted {
		r.electionTime = time.Now()
	}

	return p2p.RequestVoteResponse{
		Term:        r.state.CurrentTerm(),
		VoteGranted: granted,
	}, nil
}

func (r *Reactor) computeCommitedCount() types.Index {
	// FIXME (wojciech): This is executed frequently and must be optimised.
	indexes := make([]types.Index, 0, len(r.matchIndex))
	for _, index := range r.matchIndex {
		if index > r.committedCount {
			indexes = append(indexes, index)
		}
	}

	if len(indexes) <= r.minority {
		return r.committedCount
	}

	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i] > indexes[j]
	})

	return indexes[r.minority]
}
