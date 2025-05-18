package types

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

// Partitions represents the role of server in partitions.
type Partitions map[PartitionID]PartitionRole

// PartitionRole determines if server is the active or passive peer in the partition.
type PartitionRole bool

const (
	// PartitionRoleActive means server is active in the partition.
	PartitionRoleActive PartitionRole = true

	// PartitionRolePassive means server is passive in the partition.
	PartitionRolePassive PartitionRole = false
)

// ServerConfig stores configuration of server.
type ServerConfig struct {
	ID         ServerID
	P2PAddress string
	Partitions Partitions
}
