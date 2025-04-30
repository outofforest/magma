package types

import "github.com/google/uuid"

// ID is used to define ID field in entities.
type ID uuid.UUID

type idConstraint interface {
	~[16]byte // In go it's not possible to constraint on ID, so this is the best we can do.
}

// NewID generates new ID.
func NewID[T idConstraint]() T {
	return T(uuid.New())
}

// Revision is used to define Revision field in entities.
type Revision uint64

// Index represents the index of a log entry.
type Index uint64

// ServerID represents the unique identifier of a server.
type ServerID string

// ZeroServerID represents an uninitialized ServerID with a zero value.
var ZeroServerID ServerID

// PartitionID represents the partition ID.
type PartitionID string

// Config is the config of magma.
type Config struct {
	ServerID       ServerID
	Servers        []ServerConfig
	MaxMessageSize uint64
}

// ServerConfig stores configuration of server.
type ServerConfig struct {
	ID         ServerID
	P2PAddress string
	Partitions []PartitionID
}
