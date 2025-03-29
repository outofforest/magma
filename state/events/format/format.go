package format

import (
	"github.com/outofforest/magma/raft/types"
	magmatypes "github.com/outofforest/magma/types"
)

// Term stores term.
type Term struct {
	Term types.Term
}

// Vote stores vote.
type Vote struct {
	Candidate magmatypes.ServerID
}
