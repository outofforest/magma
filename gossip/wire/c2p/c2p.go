package c2p

import (
	magmatypes "github.com/outofforest/magma/types"
)

// InitRequest initializes tx stream.
type InitRequest struct {
	PartitionID magmatypes.PartitionID
	NextIndex   magmatypes.Index
}

// InitResponse is a response to init request.
type InitResponse struct{}
