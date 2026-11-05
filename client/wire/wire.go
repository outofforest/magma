package wire

import (
	"time"

	"github.com/outofforest/magma/raft/types"
	magmatypes "github.com/outofforest/magma/types"
	"github.com/outofforest/memdb"
)

// InitMetadata stores init metadata.
type InitMetadata struct {
	Version          magmatypes.Version
	Term             types.Term
	PreviousChecksum uint64
	NextIndex        magmatypes.Index
	EntityMetadataID uint64
}

// TxMetadata stores transaction metadata.
type TxMetadata struct {
	ID               memdb.ID
	Time             time.Time
	Service          string
	EntityMetadataID uint64
}

// InitEntityMetadata stores init entity metadata.
type InitEntityMetadata struct {
	ID        memdb.ID
	MessageID uint64
}

// TxEntityMetadata stores tx entity metadata.
type TxEntityMetadata struct {
	ID        memdb.ID
	Revision  magmatypes.Revision
	MessageID uint64
}
