package reactor

// FIXME (wojciech): Adding new peers.
// FIXME (wojciech): Preventing server from being a leader.
// FIXME (wojciech): Rebalance reactors across servers.
// FIXME (wojciech): Stop accepting client requests if there are too many uncommitted entries.

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/pkg/errors"

	"github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/state"
	magmatypes "github.com/outofforest/magma/types"
)

// Channel defines channel to use for sending the messages.
type Channel uint8

// Available channels.
const (
	ChannelNone Channel = iota
	ChannelP2P
	ChannelL2P
)

// StartTransfer initializes log transfer.
type StartTransfer struct {
	NextLogIndex types.Index
}

// Result is the result of state transition.
type Result struct {
	// Role is the current role.
	Role types.Role
	// LeaderID is the ID of current leader.
	LeaderID magmatypes.ServerID
	// CommitInfo reports latest committed log index.
	CommitInfo types.CommitInfo
	// Channel to use when sending message.
	Channel Channel
	// PeerID is the recipient, if equal to `ZeroServerID` message is broadcasted to all connected peers.
	Recipients []magmatypes.ServerID
	// Messages is the list of messages to send.
	Message any
	Force   bool
}

// New creates new reactor of raft consensus algorithm.
func New(config magmatypes.Config, s *state.State) *Reactor {
	peers := make([]magmatypes.ServerID, 0, len(config.Servers))
	for _, s := range config.Servers {
		if s.ID != config.ServerID {
			peers = append(peers, s.ID)
		}
	}

	r := &Reactor{
		config:      config,
		peers:       peers,
		state:       s,
		majority:    len(config.Servers)/2 + 1,
		lastLogTerm: s.LastLogTerm(),
		commitInfo: types.CommitInfo{
			NextLogIndex: s.NextLogIndex(),
		},
		nextIndex:  map[magmatypes.ServerID]types.Index{},
		matchIndex: map[magmatypes.ServerID]types.Index{},
	}
	r.transitionToFollower()
	r.ignoreElectionTick = 0

	return r
}

// Reactor implements Raft's state machine.
type Reactor struct {
	config   magmatypes.Config
	peers    []magmatypes.ServerID
	leaderID magmatypes.ServerID
	state    *state.State

	majority             int
	role                 types.Role
	lastLogTerm          types.Term
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
	nextIndex        map[magmatypes.ServerID]types.Index
	matchIndex       map[magmatypes.ServerID]types.Index
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
			res, err := r.applyHeartbeatTick(c)
			res.Force = true
			return res, err
		case types.ElectionTick:
			return r.applyElectionTick(c)
		}
	case cmd == nil:
		return r.applyPeerConnected(peerID)
	default:
		switch c := cmd.(type) {
		case []byte:
			return r.applyAppendTx(peerID, c)
		case *types.AppendEntriesRequest:
			return r.applyAppendEntriesRequest(peerID, c)
		case *types.AppendEntriesResponse:
			return r.applyAppendEntriesResponse(peerID, c)
		case *types.AppendEntriesACK:
			return r.applyAppendEntriesACK(peerID, c)
		case *types.Heartbeat:
			return r.applyHeartbeat(peerID, c)
		case *types.VoteRequest:
			return r.applyVoteRequest(peerID, c)
		case *types.VoteResponse:
			return r.applyVoteResponse(c)
		}
	}

	return r.resultError(errors.Errorf("unexpected message type %T", cmd))
}

func (r *Reactor) applyAppendTx(peerID magmatypes.ServerID, tx []byte) (Result, error) {
	if r.leaderID != peerID {
		return r.resultEmpty()
	}

	var err error
	r.lastLogTerm, r.commitInfo.NextLogIndex, err = r.state.Append(tx, true, true)
	if err != nil {
		return r.resultError(err)
	}

	r.ignoreElectionTick = r.electionTick + 1

	return r.resultEmpty()
}

func (r *Reactor) applyAppendEntriesACK(peerID magmatypes.ServerID, m *types.AppendEntriesACK) (Result, error) {
	if err := r.maybeTransitionToFollower(m.Term); err != nil {
		return r.resultError(err)
	}

	if r.role != types.RoleLeader || m.Term != r.state.CurrentTerm() {
		return r.resultEmpty()
	}

	if m.NextLogIndex > r.commitInfo.NextLogIndex {
		return r.resultError(errors.New("bug in protocol"))
	}
	if m.SyncLogIndex > m.NextLogIndex {
		return r.resultError(errors.New("bug in protocol"))
	}

	if m.NextLogIndex >= r.nextIndex[peerID] {
		r.nextIndex[peerID] = m.NextLogIndex
		if m.SyncLogIndex > r.matchIndex[peerID] {
			r.matchIndex[peerID] = m.SyncLogIndex
			r.updateLeaderCommit(m.SyncLogIndex)
		}
	}

	return r.resultEmpty()
}

func (r *Reactor) applyAppendEntriesRequest(peerID magmatypes.ServerID, m *types.AppendEntriesRequest) (Result, error) {
	if r.role == types.RoleLeader && m.Term == r.state.CurrentTerm() {
		return r.resultError(errors.New("bug in protocol"))
	}

	if err := r.maybeTransitionToFollower(m.Term); err != nil {
		return r.resultError(err)
	}

	resp, err := r.handleAppendEntriesRequest(peerID, m)
	if err != nil {
		return r.resultError(err)
	}

	return r.resultMessageAndRecipient(ChannelL2P, resp, peerID)
}

func (r *Reactor) applyAppendEntriesResponse(
	peerID magmatypes.ServerID,
	m *types.AppendEntriesResponse,
) (Result, error) {
	if err := r.maybeTransitionToFollower(m.Term); err != nil {
		return r.resultError(err)
	}

	if r.role != types.RoleLeader || m.Term != r.state.CurrentTerm() {
		return r.resultEmpty()
	}

	if m.NextLogIndex > r.commitInfo.NextLogIndex {
		return r.resultError(errors.New("bug in protocol"))
	}
	if m.SyncLogIndex > m.NextLogIndex {
		return r.resultError(errors.New("bug in protocol"))
	}

	if m.NextLogIndex == r.nextIndex[peerID] {
		r.matchIndex[peerID] = m.SyncLogIndex
		r.updateLeaderCommit(m.SyncLogIndex)

		return r.resultMessageAndRecipient(ChannelL2P, &StartTransfer{
			NextLogIndex: m.NextLogIndex,
		}, peerID)
	}

	r.nextIndex[peerID] = m.NextLogIndex
	req := &types.AppendEntriesRequest{
		Term:         r.state.CurrentTerm(),
		NextLogIndex: m.NextLogIndex,
		LastLogTerm:  r.state.PreviousTerm(m.NextLogIndex),
	}

	// We send no logs until a common point is found.
	return r.resultMessageAndRecipient(ChannelL2P, req, peerID)
}

func (r *Reactor) applyVoteRequest(peerID magmatypes.ServerID, m *types.VoteRequest) (Result, error) {
	if r.role == types.RoleLeader && m.Term == r.state.CurrentTerm() {
		return r.resultError(errors.New("bug in protocol"))
	}

	if err := r.maybeTransitionToFollower(m.Term); err != nil {
		return r.resultError(err)
	}

	resp, err := r.handleVoteRequest(peerID, m)
	if err != nil {
		return r.resultError(err)
	}
	return r.resultMessageAndRecipient(ChannelP2P, resp, peerID)
}

func (r *Reactor) applyVoteResponse(m *types.VoteResponse) (Result, error) {
	if err := r.maybeTransitionToFollower(m.Term); err != nil {
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

func (r *Reactor) applyHeartbeat(peerID magmatypes.ServerID, m *types.Heartbeat) (Result, error) {
	if r.role == types.RoleLeader && m.Term == r.state.CurrentTerm() {
		return r.resultError(errors.New("bug in protocol"))
	}

	if err := r.maybeTransitionToFollower(m.Term); err != nil {
		return r.resultError(err)
	}

	if peerID != r.leaderID || m.Term < r.state.CurrentTerm() {
		return r.resultEmpty()
	}

	if m.LeaderCommit < r.commitInfo.CommittedCount {
		return r.resultError(errors.New("bug in protocol"))
	}

	r.leaderCommittedCount = m.LeaderCommit
	r.updateFollowerCommit()

	r.ignoreElectionTick = r.electionTick + 1

	return r.resultEmpty()
}

func (r *Reactor) applyClientRequest(m *types.ClientRequest) (Result, error) {
	if r.role != types.RoleLeader {
		return r.resultEmpty()
	}
	if len(m.Data) == 0 {
		return r.resultEmpty()
	}

	var err error
	r.lastLogTerm, r.commitInfo.NextLogIndex, err = r.state.Append(m.Data, false, false)
	if err != nil {
		return r.resultError(err)
	}

	return r.resultEmpty()
}

func (r *Reactor) applyHeartbeatTick(tick types.HeartbeatTick) (Result, error) {
	if r.commitInfo.NextLogIndex < r.syncedCount {
		return r.resultError(errors.New("bug in protocol"))
	}

	r.heartbeatTick = tick

	//nolint:nestif
	if r.commitInfo.NextLogIndex > r.syncedCount && tick%5 == 0 {
		var err error
		r.syncedCount, err = r.state.Sync()
		if err != nil {
			return r.resultError(err)
		}
		if r.syncedCount < r.commitInfo.CommittedCount {
			return r.resultError(errors.New("bug in protocol"))
		}

		if r.role == types.RoleLeader {
			r.matchIndex[r.config.ServerID] = r.syncedCount
			if r.updateLeaderCommit(r.syncedCount) {
				return r.newHeartbeatRequest()
			}
		} else {
			r.updateFollowerCommit()

			if r.leaderID == magmatypes.ZeroServerID {
				return r.resultEmpty()
			}

			return r.resultMessageAndRecipient(ChannelP2P, &types.AppendEntriesACK{
				Term:         r.state.CurrentTerm(),
				NextLogIndex: r.commitInfo.NextLogIndex,
				SyncLogIndex: r.syncedCount,
			}, r.leaderID)
		}
	}

	if r.role != types.RoleLeader || tick <= r.ignoreHeartbeatTick || tick%20 != 0 {
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

func (r *Reactor) applyPeerConnected(peerID magmatypes.ServerID) (Result, error) {
	if r.role != types.RoleLeader {
		return r.resultEmpty()
	}

	delete(r.matchIndex, peerID)

	r.nextIndex[peerID] = r.commitInfo.NextLogIndex

	return r.resultMessageAndRecipient(ChannelL2P, r.newAppendEntriesRequest(), peerID)
}

func (r *Reactor) maybeTransitionToFollower(term types.Term) error {
	if term <= r.state.CurrentTerm() {
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
	r.leaderID = magmatypes.ZeroServerID
	r.ignoreElectionTick = r.electionTick + 1
	r.votedForMe = 0
	clear(r.nextIndex)
	clear(r.matchIndex)
}

func (r *Reactor) transitionToCandidate() (Result, error) {
	if err := r.state.SetCurrentTerm(r.state.CurrentTerm() + 1); err != nil {
		return r.resultError(err)
	}
	granted, err := r.state.VoteFor(r.config.ServerID)
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
	clear(r.nextIndex)
	clear(r.matchIndex)

	if r.majority == 1 {
		return r.transitionToLeader()
	}

	return r.resultBroadcastMessage(ChannelP2P, &types.VoteRequest{
		Term:         r.state.CurrentTerm(),
		NextLogIndex: r.commitInfo.NextLogIndex,
		LastLogTerm:  r.lastLogTerm,
	})
}

func (r *Reactor) transitionToLeader() (Result, error) {
	r.role = types.RoleLeader
	r.leaderID = r.config.ServerID
	clear(r.matchIndex)

	r.indexTermStarted = r.commitInfo.NextLogIndex
	var err error
	r.lastLogTerm, r.commitInfo.NextLogIndex, err = r.state.AppendTerm()
	if err != nil {
		return r.resultError(err)
	}

	r.ignoreHeartbeatTick = r.heartbeatTick + 1

	if r.majority == 1 {
		r.commitInfo.CommittedCount = r.commitInfo.NextLogIndex
		return r.resultEmpty()
	}

	for _, p := range r.peers {
		r.nextIndex[p] = r.commitInfo.NextLogIndex
	}

	return r.resultBroadcastMessage(ChannelL2P, r.newAppendEntriesRequest())
}

func (r *Reactor) newAppendEntriesRequest() *types.AppendEntriesRequest {
	return &types.AppendEntriesRequest{
		Term:         r.state.CurrentTerm(),
		NextLogIndex: r.commitInfo.NextLogIndex,
		LastLogTerm:  r.lastLogTerm,
	}
}

func (r *Reactor) handleAppendEntriesRequest(
	peerID magmatypes.ServerID,
	req *types.AppendEntriesRequest,
) (*types.AppendEntriesResponse, error) {
	if req.NextLogIndex < r.commitInfo.CommittedCount {
		return nil, errors.New("bug in protocol")
	}

	resp := &types.AppendEntriesResponse{
		Term:         r.state.CurrentTerm(),
		NextLogIndex: r.commitInfo.NextLogIndex,
		SyncLogIndex: r.syncedCount,
	}
	if req.Term < r.state.CurrentTerm() {
		return resp, nil
	}

	var err error
	r.lastLogTerm, r.commitInfo.NextLogIndex, err = r.state.Validate(req.NextLogIndex, req.LastLogTerm)
	if err != nil {
		return nil, err
	}

	if r.commitInfo.NextLogIndex < r.syncedCount {
		r.syncedCount = r.commitInfo.NextLogIndex
	}
	if req.NextLogIndex < r.syncedCount {
		r.syncedCount = req.NextLogIndex
	}
	if req.NextLogIndex == r.commitInfo.NextLogIndex {
		r.leaderID = peerID
	}

	r.ignoreElectionTick = r.electionTick + 1

	resp.SyncLogIndex = r.syncedCount
	resp.NextLogIndex = r.commitInfo.NextLogIndex
	return resp, nil
}

func (r *Reactor) handleVoteRequest(
	candidateID magmatypes.ServerID,
	req *types.VoteRequest,
) (*types.VoteResponse, error) {
	if req.Term < r.state.CurrentTerm() || r.lastLogTerm > req.LastLogTerm ||
		(r.lastLogTerm == req.LastLogTerm && r.commitInfo.NextLogIndex > req.NextLogIndex) {
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

	return r.resultBroadcastMessage(ChannelP2P, &types.Heartbeat{
		Term:         r.state.CurrentTerm(),
		LeaderCommit: r.commitInfo.CommittedCount,
	})
}

func (r *Reactor) updateFollowerCommit() {
	if r.commitInfo.NextLogIndex > r.commitInfo.CommittedCount && r.leaderCommittedCount > r.commitInfo.CommittedCount {
		r.commitInfo.CommittedCount = r.commitInfo.NextLogIndex
		if r.commitInfo.CommittedCount > r.leaderCommittedCount {
			r.commitInfo.CommittedCount = r.leaderCommittedCount
		}
		fmt.Printf(" %s: %d\n", uuid.UUID(r.config.ServerID), r.commitInfo.CommittedCount)
	}
}

func (r *Reactor) updateLeaderCommit(candidate types.Index) bool {
	if candidate <= r.commitInfo.CommittedCount || candidate <= r.indexTermStarted {
		return false
	}

	var greater int
	nextCommittedCount := candidate
	for _, s := range r.config.Servers {
		index := r.matchIndex[s.ID]
		if index <= r.commitInfo.CommittedCount || index <= r.indexTermStarted {
			continue
		}
		if index < nextCommittedCount {
			nextCommittedCount = index
		}
		greater++
		if greater == r.majority {
			r.commitInfo.CommittedCount = nextCommittedCount
			fmt.Printf("+%s: %d\n", uuid.UUID(r.config.ServerID), r.commitInfo.CommittedCount)
			return true
		}
	}
	return false
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

func (r *Reactor) resultMessageAndRecipient(
	channel Channel,
	message any,
	recipient magmatypes.ServerID,
) (Result, error) {
	return Result{
		Role:       r.role,
		LeaderID:   r.leaderID,
		CommitInfo: r.commitInfo,
		Channel:    channel,
		Message:    message,
		Recipients: []magmatypes.ServerID{recipient},
	}, nil
}

func (r *Reactor) resultBroadcastMessage(channel Channel, message any) (Result, error) {
	return Result{
		Role:       r.role,
		LeaderID:   r.leaderID,
		CommitInfo: r.commitInfo,
		Channel:    channel,
		Message:    message,
		Recipients: r.peers,
	}, nil
}
