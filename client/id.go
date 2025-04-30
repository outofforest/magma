package client

import "github.com/google/uuid"

const idIndex = "id"

type idConstraint interface {
	~[16]byte // In go it's not possible to constraint on ID, so this is the best we can do.
}

// NewID generates new ID.
func NewID[T idConstraint]() T {
	return T(uuid.New())
}
