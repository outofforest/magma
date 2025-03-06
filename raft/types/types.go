package types

import (
	"github.com/outofforest/magma/types"
)

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

type (
	// Term represents the term.
	Term uint64

	// Index represents the index of a log entry.
	Index uint64
)

// Command represents a command executed by state machine.
type Command struct {
	PeerID types.ServerID
	Cmd    any
}

// HeartbeatTick is sent to raft reactor when it's time to send heartbeat to connected peers.
type HeartbeatTick uint64

// ElectionTick is sent to raft reactor when it's time to switch to election phase.
type ElectionTick uint64

// AppendEntriesRequest represents the structure of a request sent by a Raft leader
// to replicate log entries or as a heartbeat.
type AppendEntriesRequest struct {
	// Term is the leader's current term.
	Term Term
	// NextLogIndex is the index of the next log entry.
	NextLogIndex Index
	// NextLogTerm is the term of appended log entries.
	NextLogTerm Term
	// LastLogTerm is the term of the last log entry.
	LastLogTerm Term
	// Data are the bytes to store (empty for a heartbeat).
	Data []byte
	// LeaderCommit is the leader's commit index.
	LeaderCommit Index
}

// AppendEntriesResponse represents the response sent by a Raft follower
// to the leader after processing an AppendEntriesRequest.
type AppendEntriesResponse struct {
	// Term is the current term of the server receiving the request, for leader to update itself.
	Term Term
	// NextLogIndex is the index of the next log item.
	NextLogIndex Index
}

// VoteRequest represents the structure of a request sent by a Raft candidate
// to gather votes from other nodes in the cluster during an election process.
type VoteRequest struct {
	// Term is the candidate's current term.
	Term Term
	// NextLogIndex is the index of the candidate's next log entry.
	NextLogIndex Index
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

// ClientRequest represents a client's request to append item to the log.
type ClientRequest struct {
	Data []byte
}

// CommitInfo reports the committed height.
type CommitInfo struct {
	NextLogIndex Index
}
