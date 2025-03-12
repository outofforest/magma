package c2p

import "github.com/outofforest/magma/raft/types"

type Init struct {
	NextLogIndex types.Index
}
