package entities

import (
	"time"

	"github.com/outofforest/magma/types"
	"github.com/outofforest/memdb"
)

// Fields is used to test indices.
type Fields struct {
	ID       memdb.ID
	Revision types.Revision

	Bool   bool
	Time   time.Time
	Int8   int8
	Int16  int16
	Int32  int32
	Int64  int64
	Uint8  uint8
	Uint16 uint16
	Uint32 uint32
	Uint64 uint64
}
