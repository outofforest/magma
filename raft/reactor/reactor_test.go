package reactor

import (
	"testing"

	"github.com/stretchr/testify/require"

	magmatypes "github.com/outofforest/magma/types"
)

func TestUnknownCommand(t *testing.T) {
	requireT := require.New(t)

	servers := make([]magmatypes.ServerID, 0, len(config.Servers))
	for _, s := range config.Servers {
		servers = append(servers, s.ID)
	}

	s, _ := newState(t, "")
	r := New(config.ServerID, servers, s)
	result, err := r.Apply(magmatypes.ZeroServerID, "aaa")
	requireT.Error(err)
	requireT.Equal(Result{}, result)
}
