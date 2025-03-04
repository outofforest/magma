package types

import (
	"time"

	"github.com/google/uuid"

	"github.com/outofforest/resonance"
)

// ServerID represents the unique identifier of a server.
type ServerID uuid.UUID

// ZeroServerID represents an uninitialized ServerID with a zero value.
var ZeroServerID ServerID

// Config is the config of magma.
type Config struct {
	ServerID             ServerID
	Servers              []PeerConfig
	StateDir             string
	P2P                  resonance.Config
	C2P                  resonance.Config
	MaxLogSizePerMessage uint64
	PassthroughTimeout   time.Duration
}

// PeerConfig stores configuration of peer connection.
type PeerConfig struct {
	ID          ServerID
	P2PAddress  string
	Tx2PAddress string
}
