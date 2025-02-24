package wire

import "github.com/outofforest/magma/raft/types"

type Hello struct {
	ServerID types.ServerID
}
