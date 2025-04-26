package c2p

import (
	magmatypes "github.com/outofforest/magma/types"
)

// Init initializes tx stream.
type Init struct {
	PartitionID  magmatypes.PartitionID
	NextLogIndex magmatypes.Index
}
