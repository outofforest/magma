package types

import (
	"time"

	"github.com/google/uuid"
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

	// ServerID represents the unique identifier of a server.
	ServerID uuid.UUID
)

// ZeroServerID represents an uninitialized ServerID with a zero value.
var ZeroServerID ServerID

// HeartbeatTimeout is sent to raft reactor when it's time to send heartbeat to connected peers.
type HeartbeatTimeout struct {
	Time time.Time
}

// ElectionTimeout is sent to raft reactor when it's time to switch to election phase.
type ElectionTimeout struct {
	Time time.Time
}
