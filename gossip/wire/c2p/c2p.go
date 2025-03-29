package c2p

import "github.com/outofforest/magma/raft/types"

// Init initializes tx stream.
type Init struct {
	NextLogIndex types.Index
}
