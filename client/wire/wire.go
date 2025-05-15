package wire

import (
	"time"

	magmatypes "github.com/outofforest/magma/types"
)

// TxMetadata stores transaction metadata.
type TxMetadata struct {
	ID               magmatypes.ID
	Time             time.Time
	Service          string
	EntityMetadataID uint64
}

// EntityMetadata stores entity metadata.
type EntityMetadata struct {
	ID        magmatypes.ID
	Revision  magmatypes.Revision
	MessageID uint64
}
