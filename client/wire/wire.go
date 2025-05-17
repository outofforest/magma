package wire

import (
	"time"

	magmatypes "github.com/outofforest/magma/types"
	"github.com/outofforest/memdb"
)

// TxMetadata stores transaction metadata.
type TxMetadata struct {
	ID               memdb.ID
	Time             time.Time
	Service          string
	EntityMetadataID uint64
}

// EntityMetadata stores entity metadata.
type EntityMetadata struct {
	ID        memdb.ID
	Revision  magmatypes.Revision
	MessageID uint64
}
