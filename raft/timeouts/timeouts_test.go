package timeouts

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/raft/types"
	magmatypes "github.com/outofforest/magma/types"
)

func TestTickersStoppedOnStartup(t *testing.T) {
	requireT := require.New(t)

	tm := New(magmatypes.PartitionRoleActive, nil, nil)
	requireT.False(tm.tickerHeartbeat.Ticking())
	requireT.False(tm.tickerElection.Ticking())
}

func TestApplyMajorityWhenFollower(t *testing.T) {
	requireT := require.New(t)

	tm := New(magmatypes.PartitionRoleActive, nil, nil)

	tm.applyRole(types.RoleFollower)

	requireT.False(tm.tickerElection.Ticking())

	tm.applyMajority(false)

	requireT.False(tm.tickerElection.Ticking())

	tm.applyMajority(true)

	requireT.True(tm.tickerElection.Ticking())

	tm.applyMajority(true)

	requireT.True(tm.tickerElection.Ticking())

	tm.applyMajority(false)

	requireT.False(tm.tickerElection.Ticking())

	tm.applyMajority(false)

	requireT.False(tm.tickerElection.Ticking())
}

func TestApplyMajorityWhenPassive(t *testing.T) {
	requireT := require.New(t)

	tm := New(magmatypes.PartitionRolePassive, nil, nil)

	tm.applyRole(types.RoleFollower)

	requireT.False(tm.tickerElection.Ticking())

	tm.applyMajority(false)

	requireT.False(tm.tickerElection.Ticking())

	tm.applyMajority(true)

	requireT.False(tm.tickerElection.Ticking())

	tm.applyMajority(true)

	requireT.False(tm.tickerElection.Ticking())

	tm.applyMajority(false)

	requireT.False(tm.tickerElection.Ticking())

	tm.applyMajority(false)

	requireT.False(tm.tickerElection.Ticking())
}

func TestApplyMajorityWhenCandidate(t *testing.T) {
	requireT := require.New(t)

	tm := New(magmatypes.PartitionRoleActive, nil, nil)

	tm.role = types.RoleCandidate
	tm.applyRole(types.RoleCandidate)

	requireT.False(tm.tickerElection.Ticking())

	tm.applyMajority(false)

	requireT.False(tm.tickerElection.Ticking())

	tm.applyMajority(true)

	requireT.True(tm.tickerElection.Ticking())

	tm.applyMajority(true)

	requireT.True(tm.tickerElection.Ticking())

	tm.applyMajority(false)

	requireT.False(tm.tickerElection.Ticking())

	tm.applyMajority(false)

	requireT.False(tm.tickerElection.Ticking())
}

func TestApplyMajorityWhenLeader(t *testing.T) {
	requireT := require.New(t)

	tm := New(magmatypes.PartitionRoleActive, nil, nil)

	tm.role = types.RoleLeader
	tm.applyRole(types.RoleLeader)

	requireT.False(tm.tickerElection.Ticking())

	tm.applyMajority(false)

	requireT.False(tm.tickerElection.Ticking())

	tm.applyMajority(true)

	requireT.False(tm.tickerElection.Ticking())

	tm.applyMajority(true)

	requireT.False(tm.tickerElection.Ticking())

	tm.applyMajority(false)

	requireT.False(tm.tickerElection.Ticking())

	tm.applyMajority(false)

	requireT.False(tm.tickerElection.Ticking())
}

func TestApplyRoleFollower(t *testing.T) {
	requireT := require.New(t)

	tm := New(magmatypes.PartitionRoleActive, nil, nil)

	tm.majorityPresent = false
	tm.applyRole(types.RoleFollower)
	requireT.False(tm.tickerElection.Ticking())

	tm.majorityPresent = true
	tm.applyRole(types.RoleFollower)
	requireT.True(tm.tickerElection.Ticking())

	tm.majorityPresent = false
	tm.applyRole(types.RoleFollower)
	requireT.False(tm.tickerElection.Ticking())
}

func TestApplyRoleFollowerWhenPassive(t *testing.T) {
	requireT := require.New(t)

	tm := New(magmatypes.PartitionRolePassive, nil, nil)

	tm.majorityPresent = false
	tm.applyRole(types.RoleFollower)
	requireT.False(tm.tickerElection.Ticking())

	tm.majorityPresent = true
	tm.applyRole(types.RoleFollower)
	requireT.False(tm.tickerElection.Ticking())

	tm.majorityPresent = false
	tm.applyRole(types.RoleFollower)
	requireT.False(tm.tickerElection.Ticking())
}

func TestApplyRoleCandidate(t *testing.T) {
	requireT := require.New(t)

	tm := New(magmatypes.PartitionRoleActive, nil, nil)

	tm.majorityPresent = false
	tm.applyRole(types.RoleCandidate)
	requireT.False(tm.tickerElection.Ticking())

	tm.majorityPresent = true
	tm.applyRole(types.RoleCandidate)
	requireT.True(tm.tickerElection.Ticking())

	tm.majorityPresent = false
	tm.applyRole(types.RoleCandidate)
	requireT.False(tm.tickerElection.Ticking())
}

func TestApplyRoleLeader(t *testing.T) {
	requireT := require.New(t)

	tm := New(magmatypes.PartitionRoleActive, nil, nil)

	tm.majorityPresent = false
	tm.applyRole(types.RoleLeader)
	requireT.False(tm.tickerElection.Ticking())

	tm.majorityPresent = true
	tm.applyRole(types.RoleLeader)
	requireT.False(tm.tickerElection.Ticking())

	tm.majorityPresent = false
	tm.applyRole(types.RoleLeader)
	requireT.False(tm.tickerElection.Ticking())
}
