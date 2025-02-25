package helpers

import "github.com/outofforest/magma/types"

// Peers returns peers of the server by removing server ID from the server set.
func Peers(config types.Config) []types.ServerID {
	peers := make([]types.ServerID, 0, len(config.Servers))
	for _, s := range config.Servers {
		if s.ID != config.ServerID {
			peers = append(peers, s.ID)
		}
	}
	return peers
}
