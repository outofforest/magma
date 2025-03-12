package types

import (
	"github.com/google/uuid"
)

// ServerID represents the unique identifier of a server.
type ServerID uuid.UUID

// ZeroServerID represents an uninitialized ServerID with a zero value.
var ZeroServerID ServerID

// Config is the config of magma.
type Config struct {
	ServerID       ServerID
	Servers        []PeerConfig
	MaxMessageSize uint64
}

// PeerConfig stores configuration of peer connection.
type PeerConfig struct {
	ID         ServerID
	P2PAddress string
}
