package helpers

import "github.com/outofforest/magma/types"

// Peers returns peers of the server by removing server ID from the server set.
func Peers(serverID types.ServerID, servers []types.ServerID) []types.ServerID {
	peers := make([]types.ServerID, 0, len(servers))
	for _, s := range servers {
		if s != serverID {
			peers = append(peers, s)
		}
	}
	return peers
}
