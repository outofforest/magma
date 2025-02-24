package wire

import "github.com/outofforest/magma/raft/types"

// Hello is th message exchanged between peers when connected.
type Hello struct {
	ServerID types.ServerID
}
