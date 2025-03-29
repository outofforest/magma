package format

import (
	"github.com/outofforest/magma/raft/types"
)

// ChecksumSize is the length of header checksum.
const ChecksumSize = 8

// Header is the file header.
type Header struct {
	PreviousTerm     types.Term
	PreviousChecksum uint64
	Term             types.Term
	NextLogIndex     types.Index
	NextTxOffset     types.Index
	HeaderChecksum   uint64
}
