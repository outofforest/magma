package timeouts

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/raft/types"
	magmatypes "github.com/outofforest/magma/types"
)

var (
	peer1 = magmatypes.ServerID(uuid.New())
	peer2 = magmatypes.ServerID(uuid.New())
	peer3 = magmatypes.ServerID(uuid.New())
	peer4 = magmatypes.ServerID(uuid.New())

	conn1 = types.NewConnectionID()
	conn2 = types.NewConnectionID()
	conn3 = types.NewConnectionID()
	conn4 = types.NewConnectionID()
)

func TestTickersStoppedOnStartup(t *testing.T) {
	requireT := require.New(t)

	tm := New(3, nil, nil)
	requireT.False(tm.tickerHeartbeat.Ticking())
	requireT.False(tm.tickerElection.Ticking())
}

func TestPeersWhenFollower(t *testing.T) {
	requireT := require.New(t)

	tm := New(3, nil, nil)

	tm.applyRole(types.RoleFollower)

	requireT.False(tm.tickerHeartbeat.Ticking())
	requireT.False(tm.tickerElection.Ticking())

	tm.applyPeerEvent(types.PeerEvent{
		PeerID:       peer1,
		ConnectionID: conn1,
		Connected:    true,
	})

	requireT.False(tm.tickerHeartbeat.Ticking())
	requireT.False(tm.tickerElection.Ticking())

	tm.applyPeerEvent(types.PeerEvent{
		PeerID:       peer2,
		ConnectionID: conn2,
		Connected:    true,
	})

	requireT.False(tm.tickerHeartbeat.Ticking())
	requireT.True(tm.tickerElection.Ticking())

	tm.applyPeerEvent(types.PeerEvent{
		PeerID:       peer3,
		ConnectionID: conn3,
		Connected:    true,
	})

	requireT.False(tm.tickerHeartbeat.Ticking())
	requireT.True(tm.tickerElection.Ticking())

	tm.applyPeerEvent(types.PeerEvent{
		PeerID:       peer4,
		ConnectionID: conn4,
		Connected:    true,
	})

	requireT.False(tm.tickerHeartbeat.Ticking())
	requireT.True(tm.tickerElection.Ticking())

	tm.applyPeerEvent(types.PeerEvent{
		PeerID:       peer4,
		ConnectionID: conn4,
		Connected:    false,
	})

	requireT.False(tm.tickerHeartbeat.Ticking())
	requireT.True(tm.tickerElection.Ticking())

	tm.applyPeerEvent(types.PeerEvent{
		PeerID:       peer3,
		ConnectionID: conn3,
		Connected:    false,
	})

	requireT.False(tm.tickerHeartbeat.Ticking())
	requireT.True(tm.tickerElection.Ticking())

	tm.applyPeerEvent(types.PeerEvent{
		PeerID:       peer2,
		ConnectionID: conn2,
		Connected:    false,
	})

	requireT.False(tm.tickerHeartbeat.Ticking())
	requireT.False(tm.tickerElection.Ticking())

	tm.applyPeerEvent(types.PeerEvent{
		PeerID:       peer1,
		ConnectionID: conn1,
		Connected:    false,
	})

	requireT.False(tm.tickerHeartbeat.Ticking())
	requireT.False(tm.tickerElection.Ticking())
}

func TestPeersWhenCandidate(t *testing.T) {
	requireT := require.New(t)

	tm := New(3, nil, nil)

	tm.role = types.RoleCandidate
	tm.applyRole(types.RoleCandidate)

	requireT.False(tm.tickerHeartbeat.Ticking())
	requireT.False(tm.tickerElection.Ticking())

	tm.applyPeerEvent(types.PeerEvent{
		PeerID:       peer1,
		ConnectionID: conn1,
		Connected:    true,
	})

	requireT.False(tm.tickerHeartbeat.Ticking())
	requireT.False(tm.tickerElection.Ticking())

	tm.applyPeerEvent(types.PeerEvent{
		PeerID:       peer2,
		ConnectionID: conn2,
		Connected:    true,
	})

	requireT.False(tm.tickerHeartbeat.Ticking())
	requireT.True(tm.tickerElection.Ticking())

	tm.applyPeerEvent(types.PeerEvent{
		PeerID:       peer3,
		ConnectionID: conn3,
		Connected:    true,
	})

	requireT.False(tm.tickerHeartbeat.Ticking())
	requireT.True(tm.tickerElection.Ticking())

	tm.applyPeerEvent(types.PeerEvent{
		PeerID:       peer4,
		ConnectionID: conn4,
		Connected:    true,
	})

	requireT.False(tm.tickerHeartbeat.Ticking())
	requireT.True(tm.tickerElection.Ticking())

	tm.applyPeerEvent(types.PeerEvent{
		PeerID:       peer4,
		ConnectionID: conn4,
		Connected:    false,
	})

	requireT.False(tm.tickerHeartbeat.Ticking())
	requireT.True(tm.tickerElection.Ticking())

	tm.applyPeerEvent(types.PeerEvent{
		PeerID:       peer3,
		ConnectionID: conn3,
		Connected:    false,
	})

	requireT.False(tm.tickerHeartbeat.Ticking())
	requireT.True(tm.tickerElection.Ticking())

	tm.applyPeerEvent(types.PeerEvent{
		PeerID:       peer2,
		ConnectionID: conn2,
		Connected:    false,
	})

	requireT.False(tm.tickerHeartbeat.Ticking())
	requireT.False(tm.tickerElection.Ticking())

	tm.applyPeerEvent(types.PeerEvent{
		PeerID:       peer1,
		ConnectionID: conn1,
		Connected:    false,
	})

	requireT.False(tm.tickerHeartbeat.Ticking())
	requireT.False(tm.tickerElection.Ticking())
}

func TestPeersWhenLeader(t *testing.T) {
	requireT := require.New(t)

	tm := New(3, nil, nil)

	tm.role = types.RoleLeader
	tm.applyRole(types.RoleLeader)

	requireT.True(tm.tickerHeartbeat.Ticking())
	requireT.False(tm.tickerElection.Ticking())

	tm.applyPeerEvent(types.PeerEvent{
		PeerID:       peer1,
		ConnectionID: conn1,
		Connected:    true,
	})

	requireT.True(tm.tickerHeartbeat.Ticking())
	requireT.False(tm.tickerElection.Ticking())

	tm.applyPeerEvent(types.PeerEvent{
		PeerID:       peer2,
		ConnectionID: conn2,
		Connected:    true,
	})

	requireT.True(tm.tickerHeartbeat.Ticking())
	requireT.False(tm.tickerElection.Ticking())

	tm.applyPeerEvent(types.PeerEvent{
		PeerID:       peer3,
		ConnectionID: conn3,
		Connected:    true,
	})

	requireT.True(tm.tickerHeartbeat.Ticking())
	requireT.False(tm.tickerElection.Ticking())

	tm.applyPeerEvent(types.PeerEvent{
		PeerID:       peer4,
		ConnectionID: conn4,
		Connected:    true,
	})

	requireT.True(tm.tickerHeartbeat.Ticking())
	requireT.False(tm.tickerElection.Ticking())

	tm.applyPeerEvent(types.PeerEvent{
		PeerID:       peer4,
		ConnectionID: conn4,
		Connected:    false,
	})

	requireT.True(tm.tickerHeartbeat.Ticking())
	requireT.False(tm.tickerElection.Ticking())

	tm.applyPeerEvent(types.PeerEvent{
		PeerID:       peer3,
		ConnectionID: conn3,
		Connected:    false,
	})

	requireT.True(tm.tickerHeartbeat.Ticking())
	requireT.False(tm.tickerElection.Ticking())

	tm.applyPeerEvent(types.PeerEvent{
		PeerID:       peer2,
		ConnectionID: conn2,
		Connected:    false,
	})

	requireT.True(tm.tickerHeartbeat.Ticking())
	requireT.False(tm.tickerElection.Ticking())

	tm.applyPeerEvent(types.PeerEvent{
		PeerID:       peer1,
		ConnectionID: conn1,
		Connected:    false,
	})

	requireT.True(tm.tickerHeartbeat.Ticking())
	requireT.False(tm.tickerElection.Ticking())
}

func TestStaleDisconnection(t *testing.T) {
	requireT := require.New(t)

	tm := New(3, nil, nil)
	tm.connections = map[magmatypes.ServerID]types.ConnectionID{
		peer1: conn1,
	}

	tm.applyPeerEvent(types.PeerEvent{
		PeerID:       peer1,
		ConnectionID: types.NewConnectionID(),
		Connected:    false,
	})

	requireT.Equal(map[magmatypes.ServerID]types.ConnectionID{
		peer1: conn1,
	}, tm.connections)
	requireT.False(tm.tickerHeartbeat.Ticking())
	requireT.False(tm.tickerElection.Ticking())
}

func TestApplyRoleFollower(t *testing.T) {
	requireT := require.New(t)

	tm := New(3, nil, nil)

	tm.connections = map[magmatypes.ServerID]types.ConnectionID{}
	tm.applyRole(types.RoleFollower)
	requireT.False(tm.tickerHeartbeat.Ticking())
	requireT.False(tm.tickerElection.Ticking())

	tm.connections = map[magmatypes.ServerID]types.ConnectionID{
		peer1: conn1,
	}
	tm.applyRole(types.RoleFollower)
	requireT.False(tm.tickerHeartbeat.Ticking())
	requireT.False(tm.tickerElection.Ticking())

	tm.connections = map[magmatypes.ServerID]types.ConnectionID{
		peer1: conn1,
		peer2: conn2,
	}
	tm.applyRole(types.RoleFollower)
	requireT.False(tm.tickerHeartbeat.Ticking())
	requireT.True(tm.tickerElection.Ticking())

	tm.connections = map[magmatypes.ServerID]types.ConnectionID{
		peer1: conn1,
		peer2: conn2,
		peer3: conn3,
	}
	tm.applyRole(types.RoleFollower)
	requireT.False(tm.tickerHeartbeat.Ticking())
	requireT.True(tm.tickerElection.Ticking())

	tm.connections = map[magmatypes.ServerID]types.ConnectionID{
		peer1: conn1,
		peer2: conn2,
		peer3: conn3,
		peer4: conn4,
	}
	tm.applyRole(types.RoleFollower)
	requireT.False(tm.tickerHeartbeat.Ticking())
	requireT.True(tm.tickerElection.Ticking())
}

func TestApplyRoleCandidate(t *testing.T) {
	requireT := require.New(t)

	tm := New(3, nil, nil)

	tm.connections = map[magmatypes.ServerID]types.ConnectionID{}
	tm.applyRole(types.RoleCandidate)
	requireT.False(tm.tickerHeartbeat.Ticking())
	requireT.False(tm.tickerElection.Ticking())

	tm.connections = map[magmatypes.ServerID]types.ConnectionID{
		peer1: conn1,
	}
	tm.applyRole(types.RoleCandidate)
	requireT.False(tm.tickerHeartbeat.Ticking())
	requireT.False(tm.tickerElection.Ticking())

	tm.connections = map[magmatypes.ServerID]types.ConnectionID{
		peer1: conn1,
		peer2: conn2,
	}
	tm.applyRole(types.RoleCandidate)
	requireT.False(tm.tickerHeartbeat.Ticking())
	requireT.True(tm.tickerElection.Ticking())

	tm.connections = map[magmatypes.ServerID]types.ConnectionID{
		peer1: conn1,
		peer2: conn2,
		peer3: conn3,
	}
	tm.applyRole(types.RoleCandidate)
	requireT.False(tm.tickerHeartbeat.Ticking())
	requireT.True(tm.tickerElection.Ticking())

	tm.connections = map[magmatypes.ServerID]types.ConnectionID{
		peer1: conn1,
		peer2: conn2,
		peer3: conn3,
		peer4: conn4,
	}
	tm.applyRole(types.RoleCandidate)
	requireT.False(tm.tickerHeartbeat.Ticking())
	requireT.True(tm.tickerElection.Ticking())
}

func TestApplyRoleLeader(t *testing.T) {
	requireT := require.New(t)

	tm := New(3, nil, nil)

	tm.connections = map[magmatypes.ServerID]types.ConnectionID{}
	tm.applyRole(types.RoleLeader)
	requireT.True(tm.tickerHeartbeat.Ticking())
	requireT.False(tm.tickerElection.Ticking())

	tm.connections = map[magmatypes.ServerID]types.ConnectionID{
		peer1: conn1,
	}
	tm.applyRole(types.RoleLeader)
	requireT.True(tm.tickerHeartbeat.Ticking())
	requireT.False(tm.tickerElection.Ticking())

	tm.connections = map[magmatypes.ServerID]types.ConnectionID{
		peer1: conn1,
		peer2: conn2,
	}
	tm.applyRole(types.RoleLeader)
	requireT.True(tm.tickerHeartbeat.Ticking())
	requireT.False(tm.tickerElection.Ticking())

	tm.connections = map[magmatypes.ServerID]types.ConnectionID{
		peer1: conn1,
		peer2: conn2,
		peer3: conn3,
	}
	tm.applyRole(types.RoleLeader)
	requireT.True(tm.tickerHeartbeat.Ticking())
	requireT.False(tm.tickerElection.Ticking())

	tm.connections = map[magmatypes.ServerID]types.ConnectionID{
		peer1: conn1,
		peer2: conn2,
		peer3: conn3,
		peer4: conn4,
	}
	tm.applyRole(types.RoleLeader)
	requireT.True(tm.tickerHeartbeat.Ticking())
	requireT.False(tm.tickerElection.Ticking())
}
