package reactor

// FIXME (wojciech): Limit the amount of data sent in single message.
// FIXME (wojciech): Adding new peers.
// FIXME (wojciech): Preventing server from being a leader.
// FIXME (wojciech): Rebalance reactors across servers.
// FIXME (wojciech): Read and write state and logs.
// FIXME (wojciech): Stop accepting client requests if there are too many uncommited entries.
// FIXME (wojciech): Keep in memory only uncommited changes. If append is requested below commit is a protocol bug.

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
func New(
	id types.ServerID,
	servers []types.ServerID,
	s *state.State,
	timeSource TimeSource,
) *Reactor {
	r := &Reactor{
		timeSource:     timeSource,
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
	timeSource TimeSource

	id       types.ServerID
	leaderID types.ServerID
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
	indexTermStarted types.Index
	nextIndex        map[types.ServerID]types.Index
	matchIndex       map[types.ServerID]types.Index
	heartBeatTime    time.Time
}

// Apply processes incoming raft messages and transitions the reactor's role or
// generates response messages as needed based on the message type.
// It handles various Raft protocol-specific message types and timeout events. In case of an unexpected
// message type, it returns an error. This method ensures the reactor's state
// remains consistent with the Raft protocol and returns the updated role,
// any outgoing messages, and an error if applicable.
func (r *Reactor) Apply(msg p2p.Message) (types.Role, []p2p.Message, error) {
	var messages []p2p.Message
	var err error
	switch m := msg.Msg.(type) {
	case p2p.AppendEntriesRequest:
		messages, err = r.applyAppendEntriesRequest(msg.PeerID, m)
	case p2p.AppendEntriesResponse:
		messages, err = r.applyAppendEntriesResponse(msg.PeerID, m)
	case p2p.VoteRequest:
		messages, err = r.applyVoteRequest(msg.PeerID, m)
	case p2p.VoteResponse:
		messages, err = r.applyVoteResponse(msg.PeerID, m)
	case p2c.ClientRequest:
		messages, err = r.applyClientRequest(msg.PeerID, m)
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

func (r *Reactor) applyAppendEntriesRequest(peerID types.ServerID, m p2p.AppendEntriesRequest) ([]p2p.Message, error) {
	if r.role == types.RoleLeader && m.Term == r.state.CurrentTerm() {
		return nil, errors.New("bug in protocol")
	}

	if err := r.maybeTransitionToFollower(peerID, m.Term, true); err != nil {
		return nil, err
	}

	resp, err := r.handleAppendEntriesRequest(m)
	if err != nil {
		return nil, err
	}
	return []p2p.Message{
		{
			PeerID: peerID,
			Msg:    resp,
		},
	}, nil
}

func (r *Reactor) applyAppendEntriesResponse(
	peerID types.ServerID,
	m p2p.AppendEntriesResponse,
) ([]p2p.Message, error) {
	if err := r.maybeTransitionToFollower(peerID, m.Term, false); err != nil {
		return nil, err
	}

	if r.role != types.RoleLeader {
		return nil, nil
	}

	if r.callInProgress[peerID] != m.MessageID {
		return nil, nil
	}

	if m.NextLogIndex > r.nextIndex[peerID] {
		r.matchIndex[peerID] = m.NextLogIndex
		r.committedCount = r.computeCommitedCount()
	}

	r.nextIndex[peerID] = m.NextLogIndex
	if m.NextLogIndex < r.nextLogIndex {
		resp, err := r.newAppendEntriesRequest(m.NextLogIndex)
		if err != nil {
			return nil, err
		}

		r.callInProgress[peerID] = resp.MessageID

		return []p2p.Message{
			{
				PeerID: peerID,
				Msg:    resp,
			},
		}, nil
	}

	r.callInProgress[peerID] = p2p.ZeroMessageID

	return nil, nil
}

func (r *Reactor) applyVoteRequest(
	peerID types.ServerID,
	m p2p.VoteRequest,
) ([]p2p.Message, error) {
	if err := r.maybeTransitionToFollower(peerID, m.Term, false); err != nil {
		return nil, err
	}

	resp, err := r.handleVoteRequest(peerID, m)
	if err != nil {
		return nil, err
	}
	return []p2p.Message{
		{
			PeerID: peerID,
			Msg:    resp,
		},
	}, nil
}

func (r *Reactor) applyVoteResponse(peerID types.ServerID, m p2p.VoteResponse) ([]p2p.Message, error) {
	if err := r.maybeTransitionToFollower(peerID, m.Term, false); err != nil {
		return nil, err
	}

	if r.role != types.RoleCandidate || m.Term != r.state.CurrentTerm() {
		return nil, nil
	}

	if r.callInProgress[peerID] != m.MessageID {
		return nil, nil
	}

	r.callInProgress[peerID] = p2p.ZeroMessageID

	if !m.VoteGranted {
		return nil, nil
	}

	r.votedForMe++
	if r.votedForMe <= r.minority {
		return nil, nil
	}

	return r.transitionToLeader()
}

func (r *Reactor) applyClientRequest(peerID types.ServerID, m p2c.ClientRequest) ([]p2p.Message, error) {
	if r.role != types.RoleLeader {
		// We redirect request to leader, but only once, to avoid infinite hops.
		if r.leaderID == types.ZeroServerID || peerID != types.ZeroServerID {
			return nil, nil
		}
		return []p2p.Message{
			{
				PeerID: r.leaderID,
				Msg:    m,
			},
		}, nil
	}

	newLogIndex, err := r.appendLogItem(state.LogItem{
		Term: r.state.CurrentTerm(),
		Data: m.Data,
	})
	if err != nil {
		return nil, err
	}

	r.heartBeatTime = r.timeSource.Now()

	if len(r.peers) == 0 {
		r.committedCount = r.computeCommitedCount()
		return nil, nil
	}

	messages := make([]p2p.Message, 0, len(r.peers))
	msg, err := r.newAppendEntriesRequest(newLogIndex)
	if err != nil {
		return nil, err
	}

	for _, peerID := range r.peers {
		if r.callInProgress[peerID] != p2p.ZeroMessageID {
			continue
		}

		r.callInProgress[peerID] = msg.MessageID

		messages = append(messages, p2p.Message{
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

	r.heartBeatTime = r.timeSource.Now()

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

		r.callInProgress[peerID] = msg.MessageID

		messages = append(messages, p2p.Message{
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

	msg, err := r.newAppendEntriesRequest(r.nextLogIndex)
	if err != nil {
		return nil, err
	}

	r.nextIndex[peerID] = r.nextLogIndex
	r.matchIndex[peerID] = 0
	r.callInProgress[peerID] = msg.MessageID

	return []p2p.Message{
		{
			PeerID: peerID,
			Msg:    msg,
		},
	}, nil
}

func (r *Reactor) maybeTransitionToFollower(peerID types.ServerID, term types.Term, onAppendEntryRequest bool) error {
	if term < r.state.CurrentTerm() || (term == r.state.CurrentTerm() && !onAppendEntryRequest) {
		return nil
	}

	if onAppendEntryRequest {
		r.leaderID = peerID
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
	r.electionTime = r.timeSource.Now()
	r.votedForMe = 0
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
	r.leaderID = types.ZeroServerID
	r.votedForMe = 1
	r.electionTime = r.timeSource.Now()
	clear(r.nextIndex)
	clear(r.matchIndex)

	if len(r.peers) == 0 {
		return r.transitionToLeader()
	}

	messages := make([]p2p.Message, 0, len(r.peers))
	msg := p2p.VoteRequest{
		MessageID:    p2p.NewMessageID(),
		Term:         r.state.CurrentTerm(),
		NextLogIndex: r.nextLogIndex,
		LastLogTerm:  r.lastLogTerm,
	}
	for _, peerID := range r.peers {
		r.callInProgress[peerID] = msg.MessageID

		messages = append(messages, p2p.Message{
			PeerID: peerID,
			Msg:    msg,
		})
	}

	return messages, nil
}

func (r *Reactor) transitionToLeader() ([]p2p.Message, error) {
	r.role = types.RoleLeader
	r.leaderID = r.id
	clear(r.matchIndex)

	// Add fake item to the log so commit is possible without waiting for a real one.
	var err error
	r.indexTermStarted, err = r.appendLogItem(state.LogItem{
		Term: r.state.CurrentTerm(),
	})
	if err != nil {
		return nil, err
	}

	r.heartBeatTime = r.timeSource.Now()

	if len(r.peers) == 0 {
		r.committedCount = r.computeCommitedCount()
		return nil, nil
	}

	messages := make([]p2p.Message, 0, len(r.peers))

	msg, err := r.newAppendEntriesRequest(r.indexTermStarted)
	if err != nil {
		return nil, err
	}

	for _, peerID := range r.peers {
		r.nextIndex[peerID] = r.indexTermStarted
		r.callInProgress[peerID] = msg.MessageID

		messages = append(messages, p2p.Message{
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
		MessageID:    p2p.NewMessageID(),
		Term:         r.state.CurrentTerm(),
		NextLogIndex: nextLogIndex,
		LastLogTerm:  lastLogTerm,
		Entries:      entries,
		LeaderCommit: r.committedCount,
	}, nil
}

func (r *Reactor) handleAppendEntriesRequest(req p2p.AppendEntriesRequest) (p2p.AppendEntriesResponse, error) {
	resp := p2p.AppendEntriesResponse{
		MessageID:    req.MessageID,
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
		r.electionTime = r.timeSource.Now()
		if req.LeaderCommit > r.committedCount {
			r.committedCount = req.LeaderCommit
			if r.committedCount > r.nextLogIndex {
				r.committedCount = r.nextLogIndex
			}
		}
	}

	resp.NextLogIndex = r.nextLogIndex
	return resp, nil
}

func (r *Reactor) handleVoteRequest(candidateID types.ServerID, req p2p.VoteRequest) (p2p.VoteResponse, error) {
	if req.Term < r.state.CurrentTerm() || r.lastLogTerm > req.LastLogTerm ||
		(r.lastLogTerm == req.LastLogTerm && r.nextLogIndex > req.NextLogIndex) {
		return p2p.VoteResponse{
			MessageID: req.MessageID,
			Term:      r.state.CurrentTerm(),
		}, nil
	}

	granted, err := r.state.VoteFor(candidateID)
	if err != nil {
		return p2p.VoteResponse{}, err
	}
	if granted {
		r.electionTime = r.timeSource.Now()
	}

	return p2p.VoteResponse{
		MessageID:   req.MessageID,
		Term:        r.state.CurrentTerm(),
		VoteGranted: granted,
	}, nil
}

func (r *Reactor) computeCommitedCount() types.Index {
	// FIXME (wojciech): This is executed frequently and must be optimised.
	indexes := make([]types.Index, 0, len(r.matchIndex))
	for _, index := range r.matchIndex {
		if index > r.committedCount && index > r.indexTermStarted {
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

func (r *Reactor) appendLogItem(item state.LogItem) (types.Index, error) {
	nextLogIndex := r.nextLogIndex
	var success bool
	var err error
	r.lastLogTerm, r.nextLogIndex, success, err = r.state.Append(r.nextLogIndex, r.lastLogTerm, []state.LogItem{item})
	if err != nil {
		return 0, err
	}
	if !success {
		return 0, errors.New("bug in protocol")
	}
	r.matchIndex[r.id] = r.nextLogIndex

	return nextLogIndex, nil
}
