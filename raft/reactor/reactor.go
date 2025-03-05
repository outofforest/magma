package reactor

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
	"github.com/outofforest/varuint64"
)

// Result is the result of state transition.
type Result struct {
	// Role is the current role.
	Role types.Role
	// LeaderID is the ID of current leader.
	LeaderID magmatypes.ServerID
	// CommitInfo reports latest committed log index.
	CommitInfo types.CommitInfo
	// PeerID is the recipient, if equal to `ZeroServerID` message is broadcasted to all connected peers.
	Recipients []magmatypes.ServerID
	// Messages is the list of messages to send.
	Messages []any
}

type syncProgress struct {
	NextIndex types.Index
	End       types.Index
}

// New creates new reactor of raft consensus algorithm.
func New(
	id magmatypes.ServerID,
	peers []magmatypes.ServerID,
	s *state.State,
	timeSource TimeSource,
) *Reactor {
	r := &Reactor{
		timeSource:   timeSource,
		id:           id,
		peers:        peers,
		state:        s,
		varuint64Buf: make([]byte, varuint64.MaxSize),
		majority:     (len(peers)+1)/2 + 1,
		lastLogTerm:  s.LastLogTerm(),
		nextLogIndex: s.NextLogIndex(),
		sync:         map[magmatypes.ServerID]syncProgress{},
		matchIndex:   map[magmatypes.ServerID]types.Index{},
	}
	r.transitionToFollower()

	return r
}

// Reactor implements Raft's state machine.
type Reactor struct {
	timeSource TimeSource

	id           magmatypes.ServerID
	peers        []magmatypes.ServerID
	leaderID     magmatypes.ServerID
	state        *state.State
	varuint64Buf []byte

	majority     int
	role         types.Role
	lastLogTerm  types.Term
	nextLogIndex types.Index
	commitInfo   types.CommitInfo

	// Follower and candidate specific.
	electionTime time.Time

	// Candidate specific.
	votedForMe int

	// Leader specific.
	indexTermStarted types.Index
	sync             map[magmatypes.ServerID]syncProgress
	matchIndex       map[magmatypes.ServerID]types.Index
	heartBeatTime    time.Time
}

// Apply applies command to the state machine.
func (r *Reactor) Apply(peerID magmatypes.ServerID, cmd any) (Result, error) {
	switch {
	case peerID == magmatypes.ZeroServerID:
		switch c := cmd.(type) {
		case *types.ClientRequest:
			return r.applyClientRequest(c)
		case types.HeartbeatTimeout:
			return r.applyHeartbeatTimeout(time.Time(c))
		case types.ElectionTimeout:
			return r.applyElectionTimeout(time.Time(c))
		}
	case cmd == nil:
		return r.applyPeerConnected(peerID)
	default:
		switch c := cmd.(type) {
		case *types.AppendEntriesRequest:
			return r.applyAppendEntriesRequest(peerID, c)
		case *types.AppendEntriesResponse:
			return r.applyAppendEntriesResponse(peerID, c)
		case *types.VoteRequest:
			return r.applyVoteRequest(peerID, c)
		case *types.VoteResponse:
			return r.applyVoteResponse(peerID, c)
		}
	}

	return r.resultError(errors.Errorf("unexpected message type %T", cmd))
}

func (r *Reactor) applyAppendEntriesRequest(peerID magmatypes.ServerID, m *types.AppendEntriesRequest) (Result, error) {
	if r.role == types.RoleLeader && m.Term == r.state.CurrentTerm() {
		return r.resultError(errors.New("bug in protocol"))
	}
	if m.NextLogIndex < r.commitInfo.NextLogIndex {
		return r.resultError(errors.New("bug in protocol"))
	}

	if err := r.maybeTransitionToFollower(peerID, m.Term, true); err != nil {
		return r.resultError(err)
	}

	resp, err := r.handleAppendEntriesRequest(m)
	if err != nil {
		return r.resultError(err)
	}
	return r.resultMessageAndRecipient(resp, peerID)
}

func (r *Reactor) applyAppendEntriesResponse(
	peerID magmatypes.ServerID,
	m *types.AppendEntriesResponse,
) (Result, error) {
	if err := r.maybeTransitionToFollower(peerID, m.Term, false); err != nil {
		return r.resultError(err)
	}

	if r.role != types.RoleLeader || m.Term != r.state.CurrentTerm() {
		return r.resultEmpty()
	}

	if m.NextLogIndex > r.nextLogIndex {
		return r.resultError(errors.New("bug in protocol"))
	}

	if m.NextLogIndex > r.sync[peerID].NextIndex {
		r.matchIndex[peerID] = m.NextLogIndex
		if m.NextLogIndex > r.commitInfo.NextLogIndex {
			r.updateCommit()
		}
	}

	if m.NextLogIndex == r.nextLogIndex {
		r.sync[peerID] = syncProgress{
			NextIndex: m.NextLogIndex,
			End:       m.NextLogIndex,
		}
		return r.resultEmpty()
	}

	req, err := r.newAppendEntriesRequest(m.NextLogIndex)
	if err != nil {
		return Result{}, err
	}

	if req.NextLogIndex > r.sync[peerID].NextIndex {
		r.sync[peerID] = syncProgress{
			NextIndex: req.NextLogIndex,
			End:       req.NextLogIndex + types.Index(len(req.Data)),
		}
	} else {
		r.sync[peerID] = syncProgress{
			NextIndex: req.NextLogIndex,
			End:       0,
		}
	}

	return r.resultMessageAndRecipient(req, peerID)
}

func (r *Reactor) applyVoteRequest(peerID magmatypes.ServerID, m *types.VoteRequest) (Result, error) {
	if err := r.maybeTransitionToFollower(peerID, m.Term, false); err != nil {
		return r.resultError(err)
	}

	resp, err := r.handleVoteRequest(peerID, m)
	if err != nil {
		return r.resultError(err)
	}
	return r.resultMessageAndRecipient(resp, peerID)
}

func (r *Reactor) applyVoteResponse(peerID magmatypes.ServerID, m *types.VoteResponse) (Result, error) {
	if err := r.maybeTransitionToFollower(peerID, m.Term, false); err != nil {
		return r.resultError(err)
	}

	if r.role != types.RoleCandidate || m.Term != r.state.CurrentTerm() {
		return r.resultEmpty()
	}

	if !m.VoteGranted {
		return r.resultEmpty()
	}

	r.votedForMe++
	if r.votedForMe < r.majority {
		return r.resultEmpty()
	}

	return r.transitionToLeader()
}

func (r *Reactor) applyClientRequest(m *types.ClientRequest) (Result, error) {
	if r.role != types.RoleLeader {
		return r.resultEmpty()
	}

	newLogIndex, err := r.appendData(m.Data)
	if err != nil {
		return r.resultError(err)
	}

	r.heartBeatTime = r.timeSource.Now()

	if r.majority == 1 {
		r.commitInfo.NextLogIndex = r.nextLogIndex
		return r.resultEmpty()
	}

	req, err := r.newAppendEntriesRequest(newLogIndex)
	if err != nil {
		return r.resultError(err)
	}

	recipients := make([]magmatypes.ServerID, 0, len(r.peers))
	endIndex := req.NextLogIndex + types.Index(len(req.Data))
	for _, p := range r.peers {
		if r.sync[p].NextIndex == req.NextLogIndex && r.sync[p].End == req.NextLogIndex {
			recipients = append(recipients, p)
			r.sync[p] = syncProgress{
				NextIndex: req.NextLogIndex,
				End:       endIndex,
			}
		}
	}

	return r.resultMessageAndRecipients(req, recipients)
}

func (r *Reactor) applyHeartbeatTimeout(t time.Time) (Result, error) {
	if r.role != types.RoleLeader || t.Before(r.heartBeatTime) {
		return r.resultEmpty()
	}

	r.heartBeatTime = r.timeSource.Now()

	if r.majority == 1 {
		return r.resultEmpty()
	}

	req, err := r.newAppendEntriesRequest(r.nextLogIndex)
	if err != nil {
		return r.resultError(err)
	}

	recipients := make([]magmatypes.ServerID, 0, len(r.peers))
	for _, p := range r.peers {
		if r.sync[p].NextIndex == r.nextLogIndex && r.sync[p].End == r.nextLogIndex {
			recipients = append(recipients, p)
			r.sync[p] = syncProgress{
				NextIndex: r.nextLogIndex,
				End:       r.nextLogIndex,
			}
		}
	}

	return r.resultMessageAndRecipients(req, recipients)
}

func (r *Reactor) applyElectionTimeout(t time.Time) (Result, error) {
	if r.role == types.RoleLeader || t.Before(r.electionTime) {
		return r.resultEmpty()
	}

	return r.transitionToCandidate()
}

func (r *Reactor) applyPeerConnected(peerID magmatypes.ServerID) (Result, error) {
	if r.role != types.RoleLeader {
		return r.resultEmpty()
	}

	delete(r.matchIndex, peerID)

	req, err := r.newAppendEntriesRequest(r.nextLogIndex)
	if err != nil {
		return r.resultError(err)
	}

	r.sync[peerID] = syncProgress{
		NextIndex: r.nextLogIndex,
		End:       0,
	}

	return r.resultMessageAndRecipient(req, peerID)
}

func (r *Reactor) maybeTransitionToFollower(
	peerID magmatypes.ServerID,
	term types.Term,
	onAppendEntryRequest bool,
) error {
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

	if onAppendEntryRequest {
		r.leaderID = peerID
	}

	return nil
}

func (r *Reactor) transitionToFollower() {
	r.role = types.RoleFollower
	r.leaderID = magmatypes.ZeroServerID
	r.electionTime = r.timeSource.Now()
	r.votedForMe = 0
	clear(r.sync)
	clear(r.matchIndex)
}

func (r *Reactor) transitionToCandidate() (Result, error) {
	if err := r.state.SetCurrentTerm(r.state.CurrentTerm() + 1); err != nil {
		return r.resultError(err)
	}
	granted, err := r.state.VoteFor(r.id)
	if err != nil {
		return r.resultError(err)
	}
	if !granted {
		return r.resultError(errors.New("bug in protocol"))
	}

	r.role = types.RoleCandidate
	r.leaderID = magmatypes.ZeroServerID
	r.votedForMe = 1
	r.electionTime = r.timeSource.Now()
	clear(r.sync)
	clear(r.matchIndex)

	if r.majority == 1 {
		return r.transitionToLeader()
	}

	return r.resultBroadcastMessage(&types.VoteRequest{
		Term:         r.state.CurrentTerm(),
		NextLogIndex: r.nextLogIndex,
		LastLogTerm:  r.lastLogTerm,
	})
}

func (r *Reactor) transitionToLeader() (Result, error) {
	r.role = types.RoleLeader
	r.leaderID = r.id
	clear(r.matchIndex)

	// Add fake item to the log so commit is possible without waiting for a real one.
	var err error
	r.indexTermStarted, err = r.appendData(nil)
	if err != nil {
		return r.resultError(err)
	}

	r.heartBeatTime = r.timeSource.Now()

	if r.majority == 1 {
		r.commitInfo.NextLogIndex = r.nextLogIndex
		return r.resultEmpty()
	}

	req, err := r.newAppendEntriesRequest(r.indexTermStarted)
	if err != nil {
		return r.resultError(err)
	}

	for _, p := range r.peers {
		r.sync[p] = syncProgress{
			NextIndex: r.indexTermStarted,
			End:       0,
		}
	}

	return r.resultBroadcastMessage(req)
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
		LeaderCommit: r.commitInfo.NextLogIndex,
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
		if req.LeaderCommit > r.commitInfo.NextLogIndex {
			r.commitInfo.NextLogIndex = req.LeaderCommit
			if r.commitInfo.NextLogIndex > r.nextLogIndex {
				r.commitInfo.NextLogIndex = r.nextLogIndex
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

func (r *Reactor) updateCommit() {
	// FIXME (wojciech): This is executed frequently and must be optimised.
	indexes := make([]types.Index, 0, len(r.matchIndex))
	for _, index := range r.matchIndex {
		if index > r.commitInfo.NextLogIndex && index > r.indexTermStarted {
			indexes = append(indexes, index)
		}
	}

	if len(indexes) < r.majority {
		return
	}

	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i] > indexes[j]
	})

	r.commitInfo.NextLogIndex = indexes[r.majority-1]
}

func (r *Reactor) appendData(data []byte) (types.Index, error) {
	startIndex := r.nextLogIndex
	var err error
	n := types.Index(varuint64.Put(r.varuint64Buf, uint64(len(data))))
	r.lastLogTerm, r.nextLogIndex, err = r.state.Append(r.nextLogIndex, r.lastLogTerm, r.state.CurrentTerm(),
		r.varuint64Buf[:n])
	if err != nil {
		return 0, err
	}

	if len(data) > 0 {
		r.lastLogTerm, r.nextLogIndex, err = r.state.Append(r.nextLogIndex, r.lastLogTerm, r.state.CurrentTerm(), data)
		if err != nil {
			return 0, err
		}
	}
	if r.nextLogIndex != startIndex+n+types.Index(len(data)) {
		return 0, errors.New("bug in protocol")
	}
	r.matchIndex[r.id] = r.nextLogIndex

	return startIndex, nil
}

func (r *Reactor) resultError(err error) (Result, error) {
	return Result{}, err
}

func (r *Reactor) resultEmpty() (Result, error) {
	return Result{
		Role:       r.role,
		LeaderID:   r.leaderID,
		CommitInfo: r.commitInfo,
	}, nil
}

func (r *Reactor) resultMessageAndRecipient(message any, recipient magmatypes.ServerID) (Result, error) {
	return Result{
		Role:       r.role,
		LeaderID:   r.leaderID,
		CommitInfo: r.commitInfo,
		Messages:   []any{message},
		Recipients: []magmatypes.ServerID{recipient},
	}, nil
}

func (r *Reactor) resultMessageAndRecipients(message any, recipients []magmatypes.ServerID) (Result, error) {
	return Result{
		Role:       r.role,
		LeaderID:   r.leaderID,
		CommitInfo: r.commitInfo,
		Messages:   []any{message},
		Recipients: recipients,
	}, nil
}

func (r *Reactor) resultBroadcastMessage(message any) (Result, error) {
	return Result{
		Role:       r.role,
		LeaderID:   r.leaderID,
		CommitInfo: r.commitInfo,
		Messages:   []any{message},
		Recipients: r.peers,
	}, nil
}
