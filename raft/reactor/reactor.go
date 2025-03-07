package reactor

// FIXME (wojciech): Adding new peers.
// FIXME (wojciech): Preventing server from being a leader.
// FIXME (wojciech): Rebalance reactors across servers.
// FIXME (wojciech): Read and write state and logs.
// FIXME (wojciech): Stop accepting client requests if there are too many uncommitted entries.

import (
	"sort"

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
	maxLogSizePerMessage, maxLogSizeOnWire uint64,
) *Reactor {
	if maxLogSizePerMessage > maxLogSizeOnWire {
		maxLogSizePerMessage = maxLogSizeOnWire
	}

	r := &Reactor{
		id:                   id,
		peers:                peers,
		state:                s,
		varuint64Buf:         make([]byte, varuint64.MaxSize),
		maxLogSizePerMessage: maxLogSizePerMessage,
		maxLogSizeOnWire:     maxLogSizeOnWire,
		majority:             (len(peers)+1)/2 + 1,
		lastLogTerm:          s.LastLogTerm(),
		nextLogIndex:         s.NextLogIndex(),
		sync:                 map[magmatypes.ServerID]*syncProgress{},
		matchIndex:           map[magmatypes.ServerID]types.Index{},
		commitIndexes:        make([]types.Index, len(peers)+1),
	}
	r.transitionToFollower()
	r.ignoreElectionTick = 0

	return r
}

// Reactor implements Raft's state machine.
type Reactor struct {
	id                                     magmatypes.ServerID
	peers                                  []magmatypes.ServerID
	leaderID                               magmatypes.ServerID
	state                                  *state.State
	varuint64Buf                           []byte
	maxLogSizePerMessage, maxLogSizeOnWire uint64

	majority             int
	role                 types.Role
	lastLogTerm          types.Term
	nextLogIndex         types.Index
	syncedCount          types.Index
	leaderCommittedCount types.Index
	commitInfo           types.CommitInfo
	ignoreElectionTick   types.ElectionTick
	ignoreHeartbeatTick  types.HeartbeatTick

	// Follower and candidate specific.
	electionTick types.ElectionTick

	// Candidate specific.
	votedForMe int

	// Leader specific.
	indexTermStarted types.Index
	sync             map[magmatypes.ServerID]*syncProgress
	matchIndex       map[magmatypes.ServerID]types.Index
	commitIndexes    []types.Index
	heartbeatTick    types.HeartbeatTick
}

// Apply applies command to the state machine.
func (r *Reactor) Apply(peerID magmatypes.ServerID, cmd any) (Result, error) {
	switch {
	case peerID == magmatypes.ZeroServerID:
		switch c := cmd.(type) {
		case *types.ClientRequest:
			return r.applyClientRequest(c)
		case types.HeartbeatTick:
			return r.applyHeartbeatTick(c)
		case types.ElectionTick:
			return r.applyElectionTick(c)
		case types.SyncTick:
			return r.applySyncTick()
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

	if err := r.maybeTransitionToFollower(peerID, m.Term, true); err != nil {
		return r.resultError(err)
	}

	resp, err := r.handleAppendEntriesRequest(m)
	if err != nil {
		return r.resultError(err)
	}
	if resp == nil {
		return r.resultEmpty()
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
	if m.SyncLogIndex > m.NextLogIndex {
		return r.resultError(errors.New("bug in protocol"))
	}

	if m.NextLogIndex > r.sync[peerID].NextIndex {
		r.matchIndex[peerID] = m.SyncLogIndex
		if m.SyncLogIndex > r.commitInfo.CommittedCount {
			r.updateLeaderCommit()
		}
	}

	if m.NextLogIndex == r.nextLogIndex {
		pSync := r.sync[peerID]
		pSync.NextIndex = m.NextLogIndex
		pSync.End = m.NextLogIndex

		return r.resultEmpty()
	}

	//nolint:nestif
	if m.NextLogIndex > r.sync[peerID].NextIndex {
		lenToSend := r.maxLogSizeOnWire
		endIndex := m.NextLogIndex
		if r.sync[peerID].End > 0 {
			endIndex = r.sync[peerID].End
			if lenOnWire := uint64(endIndex - m.NextLogIndex); lenOnWire < lenToSend {
				lenToSend -= lenOnWire
			} else {
				lenToSend = 0
			}
		}

		if remaining := uint64(r.nextLogIndex - m.NextLogIndex); remaining < lenToSend {
			lenToSend = remaining
		}

		var reqs []any
		if lenToSend > 0 {
			reqs = make([]any, 0, (lenToSend+r.maxLogSizePerMessage-1)/r.maxLogSizePerMessage)
			for endIndex < r.nextLogIndex && lenToSend > 0 {
				maxSize := r.maxLogSizePerMessage
				if maxSize > lenToSend {
					maxSize = lenToSend
				}

				req, err := r.newAppendEntriesRequest(endIndex, maxSize)
				if err != nil {
					return Result{}, err
				}

				endIndex += types.Index(len(req.Data))
				lenToSend -= uint64(len(req.Data))

				reqs = append(reqs, req)
			}
		}

		pSync := r.sync[peerID]
		pSync.NextIndex = m.NextLogIndex
		pSync.End = endIndex

		if len(reqs) == 0 {
			return r.resultEmpty()
		}

		return r.resultMessagesAndRecipient(reqs, peerID)
	}

	req, err := r.newAppendEntriesRequest(m.NextLogIndex, r.maxLogSizePerMessage)
	if err != nil {
		return Result{}, err
	}

	r.sync[peerID].NextIndex = req.NextLogIndex

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
	if len(m.Data) == 0 {
		return r.resultEmpty()
	}

	newLogIndex, err := r.appendData(m.Data)
	if err != nil {
		return r.resultError(err)
	}

	r.ignoreHeartbeatTick = r.heartbeatTick + 1

	if r.majority == 1 {
		r.commitInfo.CommittedCount = r.nextLogIndex
		return r.resultEmpty()
	}

	req, err := r.newAppendEntriesRequest(newLogIndex, r.maxLogSizePerMessage)
	if err != nil {
		return r.resultError(err)
	}

	recipients := make([]magmatypes.ServerID, 0, len(r.peers))
	endIndex := req.NextLogIndex + types.Index(len(req.Data))
	for _, p := range r.peers {
		pSync := r.sync[p]
		if pSync.NextIndex == req.NextLogIndex && pSync.End == req.NextLogIndex {
			recipients = append(recipients, p)
			pSync.End = endIndex
		}
	}

	return r.resultMessageAndRecipients(req, recipients)
}

func (r *Reactor) applyHeartbeatTick(tick types.HeartbeatTick) (Result, error) {
	r.heartbeatTick = tick
	if r.role != types.RoleLeader || tick <= r.ignoreHeartbeatTick {
		return r.resultEmpty()
	}

	return r.newHeartbeatRequest()
}

func (r *Reactor) applyElectionTick(tick types.ElectionTick) (Result, error) {
	r.electionTick = tick
	if r.role == types.RoleLeader || tick <= r.ignoreElectionTick {
		return r.resultEmpty()
	}

	return r.transitionToCandidate()
}

func (r *Reactor) applySyncTick() (Result, error) {
	if r.nextLogIndex == r.syncedCount {
		return r.resultEmpty()
	}
	if r.nextLogIndex < r.syncedCount {
		return r.resultError(errors.New("bug in protocol"))
	}

	var err error
	r.syncedCount, err = r.state.Sync()
	if err != nil {
		return r.resultError(err)
	}
	if r.syncedCount < r.commitInfo.CommittedCount {
		return r.resultError(errors.New("bug in protocol"))
	}

	if r.role == types.RoleLeader {
		r.matchIndex[r.id] = r.syncedCount
		if r.syncedCount > r.commitInfo.CommittedCount {
			oldCommit := r.commitInfo.CommittedCount
			r.updateLeaderCommit()
			if r.commitInfo.CommittedCount != oldCommit {
				return r.newHeartbeatRequest()
			}
		}
		return r.resultEmpty()
	}

	r.updateFollowerCommit()

	if r.leaderID == magmatypes.ZeroServerID {
		return r.resultEmpty()
	}

	return r.resultMessageAndRecipient(&types.AppendEntriesResponse{
		Term:         r.state.CurrentTerm(),
		NextLogIndex: r.nextLogIndex,
		SyncLogIndex: r.syncedCount,
	}, r.leaderID)
}

func (r *Reactor) applyPeerConnected(peerID magmatypes.ServerID) (Result, error) {
	if r.role != types.RoleLeader {
		return r.resultEmpty()
	}

	delete(r.matchIndex, peerID)

	req, err := r.newAppendEntriesRequest(r.nextLogIndex, r.maxLogSizePerMessage)
	if err != nil {
		return r.resultError(err)
	}

	pSync := r.sync[peerID]
	pSync.NextIndex = r.nextLogIndex
	pSync.End = 0

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
	r.ignoreElectionTick = r.electionTick + 1
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
	r.ignoreElectionTick = r.electionTick + 1
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

var emptyTx = []byte{0x00}

func (r *Reactor) transitionToLeader() (Result, error) {
	r.role = types.RoleLeader
	r.leaderID = r.id
	clear(r.matchIndex)

	// Add fake item to the log so commit is possible without waiting for a real one.
	var err error
	r.indexTermStarted, err = r.appendData(emptyTx)
	if err != nil {
		return r.resultError(err)
	}

	r.ignoreHeartbeatTick = r.heartbeatTick + 1

	if r.majority == 1 {
		r.commitInfo.CommittedCount = r.nextLogIndex
		return r.resultEmpty()
	}

	req, err := r.newAppendEntriesRequest(r.indexTermStarted, r.maxLogSizePerMessage)
	if err != nil {
		return r.resultError(err)
	}

	for _, p := range r.peers {
		r.sync[p] = &syncProgress{
			NextIndex: r.indexTermStarted,
			End:       0,
		}
	}

	return r.resultBroadcastMessage(req)
}

func (r *Reactor) newAppendEntriesRequest(
	nextLogIndex types.Index,
	maxLogSize uint64,
) (*types.AppendEntriesRequest, error) {
	lastLogTerm, nextLogTerm, data, err := r.state.Entries(nextLogIndex, maxLogSize)
	if err != nil {
		return nil, err
	}
	return &types.AppendEntriesRequest{
		Term:         r.state.CurrentTerm(),
		NextLogIndex: nextLogIndex,
		LastLogTerm:  lastLogTerm,
		NextLogTerm:  nextLogTerm,
		Data:         data,
		LeaderCommit: r.commitInfo.CommittedCount,
	}, nil
}

func (r *Reactor) handleAppendEntriesRequest(req *types.AppendEntriesRequest) (*types.AppendEntriesResponse, error) {
	if req.NextLogIndex < r.commitInfo.CommittedCount {
		return nil, errors.New("bug in protocol")
	}
	if req.LeaderCommit < r.commitInfo.CommittedCount {
		return nil, errors.New("bug in protocol")
	}

	resp := &types.AppendEntriesResponse{
		Term:         r.state.CurrentTerm(),
		NextLogIndex: r.nextLogIndex,
		SyncLogIndex: r.syncedCount,
	}
	if req.Term < r.state.CurrentTerm() {
		return resp, nil
	}

	r.ignoreElectionTick = r.electionTick + 1

	var err error
	r.lastLogTerm, r.nextLogIndex, err = r.state.Append(req.NextLogIndex, req.LastLogTerm, req.NextLogTerm, req.Data)
	if err != nil {
		return nil, err
	}

	if r.nextLogIndex < r.syncedCount {
		r.syncedCount = r.nextLogIndex
	}
	if req.NextLogIndex < r.syncedCount {
		r.syncedCount = req.NextLogIndex
	}

	if r.nextLogIndex < req.NextLogIndex {
		resp.SyncLogIndex = r.syncedCount
		resp.NextLogIndex = r.nextLogIndex
		return resp, nil
	}

	r.leaderCommittedCount = req.LeaderCommit
	r.updateFollowerCommit()

	return nil, nil //nolint:nilnil
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
		r.ignoreElectionTick = r.electionTick + 1
	}

	return &types.VoteResponse{
		Term:        r.state.CurrentTerm(),
		VoteGranted: granted,
	}, nil
}

func (r *Reactor) newHeartbeatRequest() (Result, error) {
	if r.majority == 1 {
		return r.resultEmpty()
	}

	recipients := make([]magmatypes.ServerID, 0, len(r.peers))
	for _, p := range r.peers {
		pSync := r.sync[p]
		if pSync.NextIndex == r.nextLogIndex && pSync.End == r.nextLogIndex {
			recipients = append(recipients, p)
		}
	}

	if len(recipients) == 0 {
		return r.resultEmpty()
	}

	req, err := r.newAppendEntriesRequest(r.nextLogIndex, r.maxLogSizePerMessage)
	if err != nil {
		return r.resultError(err)
	}

	return r.resultMessageAndRecipients(req, recipients)
}

func (r *Reactor) updateFollowerCommit() {
	if r.leaderCommittedCount > r.commitInfo.CommittedCount {
		r.commitInfo.CommittedCount = r.leaderCommittedCount
		if r.commitInfo.CommittedCount > r.syncedCount {
			r.commitInfo.CommittedCount = r.syncedCount
		}
	}
}

func (r *Reactor) updateLeaderCommit() bool {
	// FIXME (wojciech): This is executed frequently and must be optimised.
	r.commitIndexes = r.commitIndexes[:0]
	for _, index := range r.matchIndex {
		if index > r.commitInfo.CommittedCount && index > r.indexTermStarted {
			r.commitIndexes = append(r.commitIndexes, index)
		}
	}

	if len(r.commitIndexes) < r.majority {
		return false
	}

	sort.Slice(r.commitIndexes, func(i, j int) bool {
		return r.commitIndexes[i] > r.commitIndexes[j]
	})

	r.commitInfo.CommittedCount = r.commitIndexes[r.majority-1]
	return true
}

func (r *Reactor) appendData(data []byte) (types.Index, error) {
	if len(data) == 0 {
		return 0, errors.New("bug in protocol")
	}

	startIndex := r.nextLogIndex
	var err error
	r.lastLogTerm, r.nextLogIndex, err = r.state.Append(r.nextLogIndex, r.lastLogTerm, r.state.CurrentTerm(), data)
	if err != nil {
		return 0, err
	}
	if r.nextLogIndex != startIndex+types.Index(len(data)) {
		return 0, errors.New("bug in protocol")
	}

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

func (r *Reactor) resultMessagesAndRecipient(messages []any, recipient magmatypes.ServerID) (Result, error) {
	return Result{
		Role:       r.role,
		LeaderID:   r.leaderID,
		CommitInfo: r.commitInfo,
		Messages:   messages,
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
