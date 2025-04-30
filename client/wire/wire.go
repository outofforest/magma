package wire

import (
	"time"

	"github.com/google/uuid"
)

// TxMetadata stores transaction metadata.
type TxMetadata struct {
	ID      uuid.UUID
	Time    time.Time
	Service string
}
