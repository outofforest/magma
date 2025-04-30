package wire

import "github.com/google/uuid"

// TxMetadata stores transaction metadata.
type TxMetadata struct {
	ID      uuid.UUID
	Service string
}
