package wire

import (
	"reflect"

	"github.com/outofforest/magma/types"
	"github.com/outofforest/proton"
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
	ServerID    types.ServerID
	PartitionID types.PartitionID
	Namespace   Namespace
	Channel     Channel
}

// HelloResponse is the response to Hello message.
type HelloResponse struct {
	Error string
}

// StartLogStream indicates beginning of log stream transfer.
type StartLogStream struct {
	Length uint64
}

// HotEnd indicates that hot end has been reached.
type HotEnd struct{}

// Namespace represents namespace of objects stored in partition.
type Namespace string

// NamespaceFromMarshaller converts marshaller to namespace string.
func NamespaceFromMarshaller(m proton.Marshaller) Namespace {
	t := reflect.TypeOf(m)
	return Namespace(t.PkgPath() + "." + t.Name())
}
