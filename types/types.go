package types

import (
	"github.com/google/uuid"
)

// ServerID represents the unique identifier of a server.
type ServerID uuid.UUID

// Config is the config of magma.
type Config struct {
	ServerID          ServerID
	Servers           []ServerID
	ListenAddrPeers   string
	ListenAddrClients string
}
