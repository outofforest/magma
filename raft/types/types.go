package types

import "github.com/outofforest/magma/types"

// Term represents the term.
type Term uint64

// HeartbeatTick is sent to raft reactor when it's time to send heartbeat to connected peers.
type HeartbeatTick uint64

// ElectionTick is sent to raft reactor when it's time to switch to election phase.
type ElectionTick uint64

// LogSyncRequest is sent by the leader to find common point in log.
type LogSyncRequest struct {
	// Term is the leader's current term.
	Term Term
	// NextLogIndex is the index of the next log entry.
	NextLogIndex types.Index
	// LastLogTerm is the term of the last log entry.
	LastLogTerm Term
}

// LogSyncResponse is sent to leader pointing to current log tail.
type LogSyncResponse struct {
	// Term is the current term of the server receiving the request, for leader to update itself.
	Term Term
	// NextLogIndex is the index of the next log item to receive.
	NextLogIndex types.Index
	// SyncLogIndex is the index synced to persistent storage.
	SyncLogIndex types.Index
}

// LogACK acknowledges log transfer.
type LogACK struct {
	// Term is the current term of the server receiving the request, for leader to update itself.
	Term Term
	// NextLogIndex is the index of the next log item to receive.
	NextLogIndex types.Index
	// SyncLogIndex is the index synced to persistent storage.
	SyncLogIndex types.Index
}

// VoteRequest represents the structure of a request sent by a Raft candidate
// to gather votes from other nodes in the cluster during an election process.
type VoteRequest struct {
	// Term is the candidate's current term.
	Term Term
	// NextLogIndex is the index of the candidate's next log entry.
	NextLogIndex types.Index
	// LastLogTerm is the term of the candidate's last log entry.
	LastLogTerm Term
}

// VoteResponse represents the response sent by a Raft node
// to a candidate after processing a VoteRequest during an election.
type VoteResponse struct {
	// Term is the current term of the server receiving the request, for candidate to update itself.
	Term Term
	// VoteGranted indicates whether the candidate received the vote.
	VoteGranted bool
}

// Heartbeat is sent by the leader to indicate its liveness.
type Heartbeat struct {
	// Term is the leader's current term.
	Term Term
	// LeaderCommit is the leader's commit index.
	LeaderCommit types.Index
}

// ClientRequest represents a client's request to append item to the log.
type ClientRequest struct {
	Data []byte
}

// CommitInfo reports the committed height.
type CommitInfo struct {
	NextLogIndex   types.Index
	CommittedCount types.Index
	HotEndIndex    types.Index
}

// Role is the role of the server.
type Role int

const (
	// RoleFollower represents the follower role.
	RoleFollower Role = iota
	// RoleCandidate represents the candidate role.
	RoleCandidate
	// RoleLeader represents the leader role.
	RoleLeader
)

// Command represents a command executed by state machine.
type Command struct {
	PeerID types.ServerID
	Cmd    any
}
