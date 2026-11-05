package wire

import (
	"github.com/outofforest/magma/raft/types"
	magmatypes "github.com/outofforest/magma/types"
)

// Channel defines channel to use for sending the messages.
type Channel uint8

// Available channels.
const (
	ChannelNone Channel = iota
	ChannelP2P
	ChannelL2P
	ChannelTx2P
)

// Hello is the message exchanged between peers when connected.
type Hello struct {
	ServerID    magmatypes.ServerID
	PartitionID magmatypes.PartitionID
	Channel     Channel
}

// HelloResponse is the response to Hello message.
type HelloResponse struct {
	Error string
}

// StartLogStream indicates beginning of log stream transfer.
type StartLogStream struct {
	Version magmatypes.Version
	Length  uint64
}

// EndOfVersion means sender reached enf of version.
type EndOfVersion struct {
	NextVersion magmatypes.Version
	NextTerm    types.Term
}

// HotEnd indicates that hot end has been reached.
type HotEnd struct{}
