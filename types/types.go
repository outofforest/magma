package types

import (
	"github.com/google/uuid"
)

// ServerID represents the unique identifier of a server.
type ServerID uuid.UUID

// Config is the config of magma.
type Config struct {
	ServerID ServerID
	Servers  []PeerConfig
}

// PeerConfig stores configuration of peer connection.
type PeerConfig struct {
	ID         ServerID
	P2PAddress string
}
