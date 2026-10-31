package localentities

import (
	magmatypes "github.com/outofforest/magma/types"
	"github.com/outofforest/memdb"
)

type (
	// ID1 is the ID type for testing.
	ID1 memdb.ID

	// ID2 is the ID type for testing.
	ID2 memdb.ID

	// Entity1 is the entity type for testing.
	Entity1 struct {
		ID       ID1
		Revision magmatypes.Revision
		Value    string
	}

	// Entity2 is the entity type for testing.
	Entity2 struct {
		ID       ID2
		Revision magmatypes.Revision
		Value    uint64
	}

	// Invalid is the invalid entity type for testing.
	Invalid struct {
		// ID is missing so using this type should give error.
		Revision magmatypes.Revision
		Value    uint64
	}
)
