package p2p

import (
	"github.com/google/uuid"

	"github.com/outofforest/magma/raft/state"
	"github.com/outofforest/magma/raft/types"
)

// NewMessageID generates a new unique identifier for a message.
func NewMessageID() MessageID {
	return MessageID(uuid.New())
}

// MessageID represents a unique identifier for a message.
type MessageID uuid.UUID

// ZeroMessageID represents uninitialized message ID.
var ZeroMessageID MessageID

// Message represents a structure for P2P messages exchanged between peers.
// It includes a unique message ID, the ID of the peer sending the message,
// and the payload.
type Message struct {
	PeerID types.ServerID
	Msg    any
}

// AppendEntriesRequest represents the structure of a request sent by a Raft leader
// to replicate log entries or as a heartbeat.
type AppendEntriesRequest struct {
	// MessageID is random identifier of the message.
	MessageID MessageID
	// Term is the leader's current term.
	Term types.Term
	// NextLogIndex is the index of the next log entry.
	NextLogIndex types.Index
	// LastLogTerm is the term of the last log entry.
	LastLogTerm types.Term
	// Entries are the log entries to store (empty for a heartbeat).
	Entries []state.LogItem
	// LeaderCommit is the leader's commit index.
	LeaderCommit types.Index
}

// AppendEntriesResponse represents the response sent by a Raft follower
// to the leader after processing an AppendEntriesRequest.
type AppendEntriesResponse struct {
	// MessageID is random identifier of the message.
	MessageID MessageID
	// Term is the current term of the server receiving the request, for leader to update itself.
	Term types.Term
	// NextLogIndex is the index of the next log item.
	NextLogIndex types.Index
	// Success indicates whether the follower contained the matching log entry.
	Success bool
}

// VoteRequest represents the structure of a request sent by a Raft candidate
// to gather votes from other nodes in the cluster during an election process.
type VoteRequest struct {
	// MessageID is random identifier of the message.
	MessageID MessageID
	// Term is the candidate's current term.
	Term types.Term
	// NextLogIndex is the index of the candidate's next log entry.
	NextLogIndex types.Index
	// LastLogTerm is the term of the candidate's last log entry.
	LastLogTerm types.Term
}

// VoteResponse represents the response sent by a Raft node
// to a candidate after processing a VoteRequest during an election.
type VoteResponse struct {
	// MessageID is random identifier of the message.
	MessageID MessageID
	// Term is the current term of the server receiving the request, for candidate to update itself.
	Term types.Term
	// VoteGranted indicates whether the candidate received the vote.
	VoteGranted bool
}
