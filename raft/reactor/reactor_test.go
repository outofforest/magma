package reactor

import (
	"testing"

	"github.com/stretchr/testify/require"

	magmatypes "github.com/outofforest/magma/types"
)

func TestUnknownCommand(t *testing.T) {
	t.Parallel()

	requireT := require.New(t)

	activePeers := make([]magmatypes.ServerID, 0, len(config.Servers))
	for _, s := range config.Servers {
		if s.ID != serverID {
			activePeers = append(activePeers, s.ID)
		}
	}

	s, _ := newState(t, "")
	r := New(config.ServerID, activePeers, []magmatypes.ServerID{passivePeerID}, 4096, s)
	result, err := r.Apply(magmatypes.ZeroServerID, "aaa")
	requireT.Error(err)
	requireT.Equal(Result{}, result)
}
