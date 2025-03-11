package types

import (
	"github.com/google/uuid"

	"github.com/outofforest/resonance"
)

// ServerID represents the unique identifier of a server.
type ServerID uuid.UUID

// ZeroServerID represents an uninitialized ServerID with a zero value.
var ZeroServerID ServerID

// Config is the config of magma.
type Config struct {
	ServerID  ServerID
	Servers   []PeerConfig
	StateDir  string
	P2P       resonance.Config
	L2P       resonance.Config
	Tx2P      resonance.Config
	C2PServer resonance.Config
	C2PClient resonance.Config
}

// PeerConfig stores configuration of peer connection.
type PeerConfig struct {
	ID          ServerID
	P2PAddress  string
	L2PAddress  string
	Tx2PAddress string
}
