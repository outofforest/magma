package wire

import (
	"github.com/outofforest/magma/types"
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

// Hello is th message exchanged between peers when connected.
type Hello struct {
	ServerID types.ServerID
	Channel  Channel
}
