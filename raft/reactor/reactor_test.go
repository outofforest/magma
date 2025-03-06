package reactor

import (
	"testing"

	"github.com/stretchr/testify/require"

	magmatypes "github.com/outofforest/magma/types"
)

func TestMaxLogSize(t *testing.T) {
	requireT := require.New(t)

	r := New(serverID, peers, newState(), 10, 12)
	requireT.EqualValues(10, r.maxLogSizePerMessage)
	requireT.EqualValues(12, r.maxLogSizeOnWire)

	r = New(serverID, peers, newState(), 12, 10)
	requireT.EqualValues(10, r.maxLogSizePerMessage)
	requireT.EqualValues(10, r.maxLogSizeOnWire)
}

func TestUnknownCommand(t *testing.T) {
	requireT := require.New(t)

	r := New(serverID, peers, newState(), 10, 12)
	result, err := r.Apply(magmatypes.ZeroServerID, "aaa")
	requireT.Error(err)
	requireT.Equal(Result{}, result)
}
