package format

import (
	"github.com/outofforest/magma/raft/types"
	magmatypes "github.com/outofforest/magma/types"
)

// ChecksumSize is the length of header checksum.
const ChecksumSize = 8

// Header is the file header.
type Header struct {
	PreviousTerm   types.Term
	Term           types.Term
	NextLogIndex   magmatypes.Index
	HeaderChecksum uint64
}
