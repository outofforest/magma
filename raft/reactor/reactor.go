package reactor

// FIXME (wojciech): Limit the amount of data sent in single message.
// FIXME (wojciech): Adding new peers.
// FIXME (wojciech): Preventing server from being a leader.
// FIXME (wojciech): Rebalance reactors across servers.
// FIXME (wojciech): Read and write state and logs.
// FIXME (wojciech): Stop accepting client requests if there are too many uncommitted entries.

import (
	"sort"
	"time"

	"github.com/pkg/errors"

	"github.com/outofforest/magma/raft/state"
	"github.com/outofforest/magma/raft/types"
	magmatypes "github.com/outofforest/magma/types"
)

// New creates new reactor of raft consensus algorithm.
func New(
	id magmatypes.ServerID,
	majority int,
	s *state.State,
	timeSource TimeSource,
) *Reactor {
	r := &Reactor{
		timeSource:   timeSource,
		id:           id,
		state:        s,
		majority:     majority,
		lastLogTerm:  s.LastLogTerm(),
		nextLogIndex: s.NextLogIndex(),
		nextIndex:    map[magmatypes.ServerID]types.Index{},
		matchIndex:   map[magmatypes.ServerID]types.Index{},
	}
	r.transitionToFollower()

	return r
}

// Reactor implements Raft's state machine.
type Reactor struct {
	timeSource TimeSource

	id       magmatypes.ServerID
	leaderID magmatypes.ServerID
	state    *state.State

	majority       int
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
	nextIndex        map[magmatypes.ServerID]types.Index
	matchIndex       map[magmatypes.ServerID]types.Index
	heartBeatTime    time.Time
}

// ID returns the ID of the server.
func (r *Reactor) ID() magmatypes.ServerID {
	return r.id
}

// Role returns current role.
func (r *Reactor) Role() types.Role {
	return r.role
}

// LeaderID returns current leader.
func (r *Reactor) LeaderID() magmatypes.ServerID {
	return r.leaderID
}

// ApplyAppendEntriesRequest handles an incoming AppendEntries request from a peer.
// It performs state transitions if necessary and validates the log consistency
// based on the request parameters. If the request is invalid or a protocol bug is detected,
// it returns an appropriate error.
func (r *Reactor) ApplyAppendEntriesRequest(
	peerID magmatypes.ServerID,
	m *types.AppendEntriesRequest,
) (*types.AppendEntriesResponse, error) {
	if r.role == types.RoleLeader && m.Term == r.state.CurrentTerm() {
		return nil, errors.New("bug in protocol")
	}
	if m.NextLogIndex < r.committedCount {
		return nil, errors.New("bug in protocol")
	}

	if err := r.maybeTransitionToFollower(peerID, m.Term, true); err != nil {
		return nil, err
	}

	resp, err := r.handleAppendEntriesRequest(m)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ApplyAppendEntriesResponse processes a response to a previously sent AppendEntries request.
// It handles potential state transitions due to term updates, adjusts match and next indexes for the peer,
// and recalculates the committed count if necessary.
// If the peer's log is behind, it prepares and returns a new AppendEntries request.
// Returns an empty request if no further action is required or an error occurred.
func (r *Reactor) ApplyAppendEntriesResponse(
	peerID magmatypes.ServerID,
	m *types.AppendEntriesResponse,
) (*types.AppendEntriesRequest, error) {
	if err := r.maybeTransitionToFollower(peerID, m.Term, false); err != nil {
		return nil, err
	}

	if r.role != types.RoleLeader {
		return nil, nil //nolint:nilnil
	}

	if m.NextLogIndex > r.getNextIndex(peerID) {
		r.matchIndex[peerID] = m.NextLogIndex
		if m.NextLogIndex > r.committedCount {
			r.committedCount = r.computeCommittedCount()
		}
	}

	r.nextIndex[peerID] = m.NextLogIndex
	if m.NextLogIndex >= r.nextLogIndex {
		return nil, nil //nolint:nilnil
	}

	resp, err := r.newAppendEntriesRequest(m.NextLogIndex)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// ApplyVoteRequest processes an incoming VoteRequest from a peer.
// It ensures the reactor transitions to a follower if necessary,
// based on the term in the request, and then handles the vote request.
// The function returns a VoteResponse and an error if any issues occur during processing.
func (r *Reactor) ApplyVoteRequest(peerID magmatypes.ServerID, m *types.VoteRequest) (*types.VoteResponse, error) {
	if err := r.maybeTransitionToFollower(peerID, m.Term, false); err != nil {
		return nil, err
	}

	resp, err := r.handleVoteRequest(peerID, m)
	if err != nil {
		return nil, err
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
	peerID magmatypes.ServerID,
	m *types.VoteResponse,
) (*types.AppendEntriesRequest, error) {
	if err := r.maybeTransitionToFollower(peerID, m.Term, false); err != nil {
		return nil, err
	}

	if r.role != types.RoleCandidate || m.Term != r.state.CurrentTerm() {
		return nil, nil //nolint:nilnil
	}

	if !m.VoteGranted {
		return nil, nil //nolint:nilnil
	}

	r.votedForMe++
	if r.votedForMe < r.majority {
		return nil, nil //nolint:nilnil
	}

	return r.transitionToLeader()
}

// ApplyClientRequest processes a client request for appending a new log entry.
// It ensures the reactor can only process the request if it is in the Leader role.
// It appends the client's data as a new log entry, and returns an AppendEntriesRequest to replicate
// the log entry to peers.
func (r *Reactor) ApplyClientRequest(m *types.ClientRequest) (*types.AppendEntriesRequest, error) {
	// ApplyClientRequest processes a client request to append a log item.
	// If the reactor is not in the Leader role, it returns an empty AppendEntriesRequest.
	// As the leader, it appends the client's data as a new log item, updates the heartbeat time,
	// and returns an AppendEntriesRequest if there are peers to replicate to,
	// or updates the committed count if there are no peers.
	if r.role != types.RoleLeader {
		return nil, nil //nolint:nilnil
	}

	newLogIndex, err := r.appendData(m.Data)
	if err != nil {
		return nil, err
	}

	r.heartBeatTime = r.timeSource.Now()

	if r.majority == 1 {
		r.committedCount = r.nextLogIndex
		return nil, nil //nolint:nilnil
	}

	msg, err := r.newAppendEntriesRequest(newLogIndex)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

// ApplyHeartbeatTimeout processes a heartbeat timeout event and ensures the leader
// sends a heartbeat to other peers if the timeout has expired. The function checks
// if the reactor is still in the Leader role and whether the provided timeout is
// valid (i.e., after the last recorded heartbeat).
func (r *Reactor) ApplyHeartbeatTimeout(t time.Time) (*types.AppendEntriesRequest, error) {
	if r.role != types.RoleLeader || t.Before(r.heartBeatTime) {
		return nil, nil //nolint:nilnil
	}

	r.heartBeatTime = r.timeSource.Now()

	if r.majority == 1 {
		return nil, nil //nolint:nilnil
	}

	msg, err := r.newAppendEntriesRequest(r.nextLogIndex)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

// ApplyElectionTimeout processes an election timeout event, transitioning the reactor
// to a candidate state if the timeout has expired and the reactor is not already a leader.
// It then sends a VoteRequest to peers to begin a new election.
func (r *Reactor) ApplyElectionTimeout(t time.Time) (*types.VoteRequest, error) {
	if r.role == types.RoleLeader || t.Before(r.electionTime) {
		return nil, nil //nolint:nilnil
	}

	return r.transitionToCandidate()
}

// ApplyPeerConnected handles the event of a new peer connection.
// If the reactor is in the Leader role, it prepares an AppendEntriesRequest
// for the newly connected peer. It also initializes the peer's nextIndex and
// matchIndex to track replication state.
func (r *Reactor) ApplyPeerConnected(peerID magmatypes.ServerID) (*types.AppendEntriesRequest, error) {
	if r.role != types.RoleLeader {
		return nil, nil //nolint:nilnil
	}

	msg, err := r.newAppendEntriesRequest(r.nextLogIndex)
	if err != nil {
		return nil, err
	}

	r.nextIndex[peerID] = r.nextLogIndex
	r.matchIndex[peerID] = 0

	return msg, nil
}

func (r *Reactor) maybeTransitionToFollower(
	peerID magmatypes.ServerID,
	term types.Term,
	onAppendEntryRequest bool,
) error {
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

func (r *Reactor) transitionToCandidate() (*types.VoteRequest, error) {
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
	r.leaderID = magmatypes.ZeroServerID
	r.votedForMe = 1
	r.electionTime = r.timeSource.Now()
	clear(r.nextIndex)
	clear(r.matchIndex)

	if r.majority == 1 {
		_, err := r.transitionToLeader()
		return nil, err
	}

	return &types.VoteRequest{
		Term:         r.state.CurrentTerm(),
		NextLogIndex: r.nextLogIndex,
		LastLogTerm:  r.lastLogTerm,
	}, nil
}

func (r *Reactor) transitionToLeader() (*types.AppendEntriesRequest, error) {
	r.role = types.RoleLeader
	r.leaderID = r.id
	clear(r.nextIndex)
	clear(r.matchIndex)

	// Add fake item to the log so commit is possible without waiting for a real one.
	var err error
	r.indexTermStarted, err = r.appendData([]byte{0x00})
	if err != nil {
		return nil, err
	}

	r.heartBeatTime = r.timeSource.Now()

	if r.majority == 1 {
		r.committedCount = r.nextLogIndex
		return nil, nil //nolint:nilnil
	}

	msg, err := r.newAppendEntriesRequest(r.indexTermStarted)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

func (r *Reactor) newAppendEntriesRequest(nextLogIndex types.Index) (*types.AppendEntriesRequest, error) {
	lastLogTerm, nextLogTerm, data, err := r.state.Entries(nextLogIndex)
	if err != nil {
		return nil, err
	}
	return &types.AppendEntriesRequest{
		Term:         r.state.CurrentTerm(),
		NextLogIndex: nextLogIndex,
		LastLogTerm:  lastLogTerm,
		NextLogTerm:  nextLogTerm,
		Data:         data,
		LeaderCommit: r.committedCount,
	}, nil
}

func (r *Reactor) handleAppendEntriesRequest(req *types.AppendEntriesRequest) (*types.AppendEntriesResponse, error) {
	resp := &types.AppendEntriesResponse{
		Term:         r.state.CurrentTerm(),
		NextLogIndex: r.nextLogIndex,
	}
	if req.Term < r.state.CurrentTerm() {
		return resp, nil
	}

	var err error
	r.lastLogTerm, r.nextLogIndex, err = r.state.Append(req.NextLogIndex, req.LastLogTerm, req.NextLogTerm, req.Data)
	if err != nil {
		return nil, err
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

func (r *Reactor) handleVoteRequest(
	candidateID magmatypes.ServerID,
	req *types.VoteRequest,
) (*types.VoteResponse, error) {
	if req.Term < r.state.CurrentTerm() || r.lastLogTerm > req.LastLogTerm ||
		(r.lastLogTerm == req.LastLogTerm && r.nextLogIndex > req.NextLogIndex) {
		return &types.VoteResponse{
			Term: r.state.CurrentTerm(),
		}, nil
	}

	granted, err := r.state.VoteFor(candidateID)
	if err != nil {
		return nil, err
	}
	if granted {
		r.electionTime = r.timeSource.Now()
	}

	return &types.VoteResponse{
		Term:        r.state.CurrentTerm(),
		VoteGranted: granted,
	}, nil
}

func (r *Reactor) computeCommittedCount() types.Index {
	// FIXME (wojciech): This is executed frequently and must be optimised.
	indexes := make([]types.Index, 0, len(r.matchIndex))
	for _, index := range r.matchIndex {
		if index > r.committedCount && index > r.indexTermStarted {
			indexes = append(indexes, index)
		}
	}

	if len(indexes) < r.majority {
		return r.committedCount
	}

	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i] > indexes[j]
	})

	return indexes[r.majority-1]
}

func (r *Reactor) appendData(data []byte) (types.Index, error) {
	nextLogIndex := r.nextLogIndex
	var err error
	r.lastLogTerm, r.nextLogIndex, err = r.state.Append(r.nextLogIndex, r.lastLogTerm, r.state.CurrentTerm(), data)
	if err != nil {
		return 0, err
	}
	if r.nextLogIndex != nextLogIndex+types.Index(len(data)) {
		return 0, errors.New("bug in protocol")
	}
	r.matchIndex[r.id] = r.nextLogIndex

	return nextLogIndex, nil
}

func (r *Reactor) getNextIndex(peerID magmatypes.ServerID) types.Index {
	if i, exists := r.nextIndex[peerID]; exists {
		return i
	}
	return r.indexTermStarted
}
