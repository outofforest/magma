package reactor

import (
	"testing"

	"github.com/stretchr/testify/require"

	magmatypes "github.com/outofforest/magma/types"
)

func TestMaxLogSize(t *testing.T) {
	requireT := require.New(t)

	config := config
	config.MaxLogSizePerMessage = 10
	config.MaxLogSizeOnWire = 12

	r := New(config, newState())
	requireT.EqualValues(10, r.config.MaxLogSizePerMessage)
	requireT.EqualValues(12, r.config.MaxLogSizeOnWire)

	config.MaxLogSizePerMessage = 12
	config.MaxLogSizeOnWire = 10

	r = New(config, newState())
	requireT.EqualValues(10, r.config.MaxLogSizePerMessage)
	requireT.EqualValues(10, r.config.MaxLogSizeOnWire)
}

func TestUnknownCommand(t *testing.T) {
	requireT := require.New(t)

	r := New(config, newState())
	result, err := r.Apply(magmatypes.ZeroServerID, "aaa")
	requireT.Error(err)
	requireT.Equal(Result{}, result)
}
