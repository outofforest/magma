package p2p

import "github.com/outofforest/magma/types"

// Hello is th message exchanged between peers when connected.
type Hello struct {
	ServerID types.ServerID
}
