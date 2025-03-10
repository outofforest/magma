package reactor

import (
	"testing"

	"github.com/stretchr/testify/require"

	magmatypes "github.com/outofforest/magma/types"
)

func TestUnknownCommand(t *testing.T) {
	requireT := require.New(t)

	s, _ := newState()
	r := New(config, s)
	result, err := r.Apply(magmatypes.ZeroServerID, "aaa")
	requireT.Error(err)
	requireT.Equal(Result{}, result)
}
