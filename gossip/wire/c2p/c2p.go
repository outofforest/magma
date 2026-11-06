package c2p

import (
	magmatypes "github.com/outofforest/magma/types"
)

// InitRequest initializes tx stream.
type InitRequest struct {
	PartitionID    magmatypes.PartitionID
	DesiredVersion magmatypes.Version
	NextIndex      magmatypes.Index
}

// InitResponse is a response to init request.
type InitResponse struct {
	Error    string
	InitSize uint64
}
