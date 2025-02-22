package reactor

// FIXME (wojciech): Limit the amount of data sent in single message.
// FIXME (wojciech): Adding new peers.
// FIXME (wojciech): Preventing server from being a leader.
// FIXME (wojciech): Rebalance reactors across servers.
// FIXME (wojciech): Read and write state and logs.
// FIXME (wojciech): Stop accepting client requests if there are too many uncommitted entries.
// FIXME (wojciech): Keep in memory only uncommitted changes. If append is requested below commit is a protocol bug.

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
	s *state.State,
	timeSource TimeSource,
) *Reactor {
	r := &Reactor{
		timeSource:   timeSource,
		id:           id,
		state:        s,
		lastLogTerm:  s.LastLogTerm(),
		nextLogIndex: s.NextLogIndex(),
		nextIndex:    map[types.ServerID]types.Index{},
		matchIndex:   map[types.ServerID]types.Index{},
	}
	r.transitionToFollower()

	return r
}

// Reactor implements Raft's state machine.
type Reactor struct {
	timeSource TimeSource

	id       types.ServerID
	leaderID types.ServerID
	state    *state.State

	role           types.Role
	lastLogTerm    types.Term
	nextLogIndex   types.Index
	committedCount types.Index

	// Follower and candidate specific.
	electionTime time.Time

	// Candidate specific.
	votedForMe int

	// Leader specific.
	indexTermStarted types.Index
	nextIndex        map[types.ServerID]types.Index
	matchIndex       map[types.ServerID]types.Index
	heartBeatTime    time.Time
}

// Info returns the current role of the reactor (e.g., leader, follower, candidate) and the ID of the leader.
func (r *Reactor) Info() (types.Role, types.ServerID) {
	return r.role, r.leaderID
}

// ApplyAppendEntriesRequest handles an incoming AppendEntries request from a peer.
// It performs state transitions if necessary and validates the log consistency
// based on the request parameters. If the request is invalid or a protocol bug is detected,
// it returns an appropriate error.
func (r *Reactor) ApplyAppendEntriesRequest(
	peerID types.ServerID,
	m p2p.AppendEntriesRequest,
) (p2p.AppendEntriesResponse, error) {
	if r.role == types.RoleLeader && m.Term == r.state.CurrentTerm() {
		return p2p.AppendEntriesResponse{}, errors.New("bug in protocol")
	}

	if err := r.maybeTransitionToFollower(peerID, m.Term, true); err != nil {
		return p2p.AppendEntriesResponse{}, err
	}

	resp, err := r.handleAppendEntriesRequest(m)
	if err != nil {
		return p2p.AppendEntriesResponse{}, err
	}
	return resp, nil
}

// ApplyAppendEntriesResponse processes a response to a previously sent AppendEntries request.
// It handles potential state transitions due to term updates, adjusts match and next indexes for the peer,
// and recalculates the committed count if necessary.
// If the peer's log is behind, it prepares and returns a new AppendEntries request.
// Returns an empty request if no further action is required or an error occurred.
func (r *Reactor) ApplyAppendEntriesResponse(
	peerID types.ServerID,
	m p2p.AppendEntriesResponse,
	peers []types.ServerID,
) (p2p.AppendEntriesRequest, error) {
	if err := r.maybeTransitionToFollower(peerID, m.Term, false); err != nil {
		return p2p.AppendEntriesRequest{}, err
	}

	if r.role != types.RoleLeader {
		return p2p.AppendEntriesRequest{}, nil
	}

	if m.NextLogIndex > r.nextIndex[peerID] {
		r.matchIndex[peerID] = m.NextLogIndex
		r.committedCount = r.computeCommitedCount((len(peers) + 1) / 2)
	}

	r.nextIndex[peerID] = m.NextLogIndex
	if m.NextLogIndex >= r.nextLogIndex {
		return p2p.AppendEntriesRequest{}, nil
	}

	resp, err := r.newAppendEntriesRequest(m.NextLogIndex)
	if err != nil {
		return p2p.AppendEntriesRequest{}, err
	}

	return resp, nil
}

// ApplyVoteRequest processes an incoming VoteRequest from a peer.
// It ensures the reactor transitions to a follower if necessary,
// based on the term in the request, and then handles the vote request.
// The function returns a VoteResponse and an error if any issues occur during processing.
func (r *Reactor) ApplyVoteRequest(peerID types.ServerID, m p2p.VoteRequest) (p2p.VoteResponse, error) {
	if err := r.maybeTransitionToFollower(peerID, m.Term, false); err != nil {
		return p2p.VoteResponse{}, err
	}

	resp, err := r.handleVoteRequest(peerID, m)
	if err != nil {
		return p2p.VoteResponse{}, err
	}
	return resp, nil
}

// ApplyVoteResponse processes an incoming VoteResponse from a peer.
// It handles potential state transitions, such as transitioning to a follower
// if a higher term is observed. If the response is for the current term and
// the reactor is still a candidate, it tracks whether the vote is granted.
// When the majority of votes is achieved, it transitions the reactor to the leader.
// If no action is required or conditions aren't met, it returns an empty AppendEntriesRequest.
func (r *Reactor) ApplyVoteResponse(
	peerID types.ServerID,
	m p2p.VoteResponse,
	peers []types.ServerID,
) (p2p.AppendEntriesRequest, error) {
	if err := r.maybeTransitionToFollower(peerID, m.Term, false); err != nil {
		return p2p.AppendEntriesRequest{}, err
	}

	if r.role != types.RoleCandidate || m.Term != r.state.CurrentTerm() {
		return p2p.AppendEntriesRequest{}, nil
	}

	if !m.VoteGranted {
		return p2p.AppendEntriesRequest{}, nil
	}

	minority := (len(peers) + 1) / 2
	r.votedForMe++
	if r.votedForMe <= minority {
		return p2p.AppendEntriesRequest{}, nil
	}

	return r.transitionToLeader(peers)
}

// ApplyClientRequest processes a client request for appending a new log entry.
// It ensures the reactor can only process the request if it is in the Leader role.
// It appends the client's data as a new log entry, and returns an AppendEntriesRequest to replicate
// the log entry to peers.
func (r *Reactor) ApplyClientRequest(m p2c.ClientRequest, peers []types.ServerID) (p2p.AppendEntriesRequest, error) {
	// ApplyClientRequest processes a client request to append a log item.
	// If the reactor is not in the Leader role, it returns an empty AppendEntriesRequest.
	// As the leader, it appends the client's data as a new log item, updates the heartbeat time,
	// and returns an AppendEntriesRequest if there are peers to replicate to,
	// or updates the committed count if there are no peers.
	if r.role != types.RoleLeader {
		return p2p.AppendEntriesRequest{}, nil
	}

	newLogIndex, err := r.appendLogItem(state.LogItem{
		Term: r.state.CurrentTerm(),
		Data: m.Data,
	})
	if err != nil {
		return p2p.AppendEntriesRequest{}, err
	}

	r.heartBeatTime = r.timeSource.Now()

	if len(peers) == 0 {
		r.committedCount = r.nextLogIndex
		return p2p.AppendEntriesRequest{}, nil
	}

	msg, err := r.newAppendEntriesRequest(newLogIndex)
	if err != nil {
		return p2p.AppendEntriesRequest{}, err
	}

	return msg, nil
}

// ApplyHeartbeatTimeout processes a heartbeat timeout event and ensures the leader
// sends a heartbeat to other peers if the timeout has expired. The function checks
// if the reactor is still in the Leader role and whether the provided timeout is
// valid (i.e., after the last recorded heartbeat).
func (r *Reactor) ApplyHeartbeatTimeout(t time.Time, peers []types.ServerID) (p2p.AppendEntriesRequest, error) {
	if r.role != types.RoleLeader || t.Before(r.heartBeatTime) {
		return p2p.AppendEntriesRequest{}, nil
	}

	r.heartBeatTime = r.timeSource.Now()

	if len(peers) == 0 {
		return p2p.AppendEntriesRequest{}, nil
	}

	msg, err := r.newAppendEntriesRequest(r.nextLogIndex)
	if err != nil {
		return p2p.AppendEntriesRequest{}, err
	}

	return msg, nil
}

// ApplyElectionTimeout processes an election timeout event, transitioning the reactor
// to a candidate state if the timeout has expired and the reactor is not already a leader.
// It then sends a VoteRequest to peers to begin a new election.
func (r *Reactor) ApplyElectionTimeout(t time.Time, peers []types.ServerID) (p2p.VoteRequest, error) {
	if r.role == types.RoleLeader || t.Before(r.electionTime) {
		return p2p.VoteRequest{}, nil
	}

	return r.transitionToCandidate(peers)
}

// ApplyPeerConnected handles the event of a new peer connection.
// If the reactor is in the Leader role, it prepares an AppendEntriesRequest
// for the newly connected peer. It also initializes the peer's nextIndex and
// matchIndex to track replication state.
func (r *Reactor) ApplyPeerConnected(peerID types.ServerID) (p2p.AppendEntriesRequest, error) {
	if r.role != types.RoleLeader {
		return p2p.AppendEntriesRequest{}, nil
	}

	msg, err := r.newAppendEntriesRequest(r.nextLogIndex)
	if err != nil {
		return p2p.AppendEntriesRequest{}, err
	}

	r.nextIndex[peerID] = r.nextLogIndex
	r.matchIndex[peerID] = 0

	return msg, nil
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
}

func (r *Reactor) transitionToCandidate(peers []types.ServerID) (p2p.VoteRequest, error) {
	if err := r.state.SetCurrentTerm(r.state.CurrentTerm() + 1); err != nil {
		return p2p.VoteRequest{}, err
	}
	granted, err := r.state.VoteFor(r.id)
	if err != nil {
		return p2p.VoteRequest{}, err
	}
	if !granted {
		return p2p.VoteRequest{}, errors.New("bug in protocol")
	}

	r.role = types.RoleCandidate
	r.leaderID = types.ZeroServerID
	r.votedForMe = 1
	r.electionTime = r.timeSource.Now()
	clear(r.nextIndex)
	clear(r.matchIndex)

	if len(peers) == 0 {
		_, err := r.transitionToLeader(peers)
		return p2p.VoteRequest{}, err
	}

	return p2p.VoteRequest{
		MessageID:    p2p.NewMessageID(),
		Term:         r.state.CurrentTerm(),
		NextLogIndex: r.nextLogIndex,
		LastLogTerm:  r.lastLogTerm,
	}, nil
}

func (r *Reactor) transitionToLeader(peers []types.ServerID) (p2p.AppendEntriesRequest, error) {
	r.role = types.RoleLeader
	r.leaderID = r.id
	clear(r.matchIndex)

	// Add fake item to the log so commit is possible without waiting for a real one.
	var err error
	r.indexTermStarted, err = r.appendLogItem(state.LogItem{
		Term: r.state.CurrentTerm(),
	})
	if err != nil {
		return p2p.AppendEntriesRequest{}, err
	}

	r.heartBeatTime = r.timeSource.Now()

	if len(peers) == 0 {
		r.committedCount = r.nextLogIndex
		return p2p.AppendEntriesRequest{}, nil
	}

	msg, err := r.newAppendEntriesRequest(r.indexTermStarted)
	if err != nil {
		return p2p.AppendEntriesRequest{}, err
	}

	for _, peerID := range peers {
		r.nextIndex[peerID] = r.indexTermStarted
	}

	return msg, nil
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

	var err error
	r.lastLogTerm, r.nextLogIndex, err = r.state.Append(req.NextLogIndex, req.LastLogTerm, req.Entries)
	if err != nil {
		return p2p.AppendEntriesResponse{}, err
	}

	if r.nextLogIndex >= req.NextLogIndex {
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

func (r *Reactor) computeCommitedCount(minority int) types.Index {
	// FIXME (wojciech): This is executed frequently and must be optimised.
	indexes := make([]types.Index, 0, len(r.matchIndex))
	for _, index := range r.matchIndex {
		if index > r.committedCount && index > r.indexTermStarted {
			indexes = append(indexes, index)
		}
	}

	if len(indexes) <= minority {
		return r.committedCount
	}

	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i] > indexes[j]
	})

	return indexes[minority]
}

func (r *Reactor) appendLogItem(item state.LogItem) (types.Index, error) {
	nextLogIndex := r.nextLogIndex
	var err error
	r.lastLogTerm, r.nextLogIndex, err = r.state.Append(r.nextLogIndex, r.lastLogTerm, []state.LogItem{item})
	if err != nil {
		return 0, err
	}
	if r.nextLogIndex != nextLogIndex+1 {
		return 0, errors.New("bug in protocol")
	}
	r.matchIndex[r.id] = r.nextLogIndex

	return nextLogIndex, nil
}
