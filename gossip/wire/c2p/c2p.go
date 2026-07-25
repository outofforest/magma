package c2p

import (
	"github.com/outofforest/magma/gossip/wire"
	magmatypes "github.com/outofforest/magma/types"
)

// InitRequest initializes tx stream.
type InitRequest struct {
	PartitionID magmatypes.PartitionID
	Namespace   wire.Namespace
	NextIndex   magmatypes.Index
}

// InitResponse is a response to init request.
type InitResponse struct {
	Error string
}
