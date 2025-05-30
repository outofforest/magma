package timeouts

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/outofforest/magma/raft/types"
	magmatypes "github.com/outofforest/magma/types"
)

const (
	heartbeatInterval    = 100 * time.Millisecond
	electionBaseInterval = 5 * time.Second
)

// New creates new timeout manager.
func New(partitionRole magmatypes.PartitionRole, roleCh <-chan types.Role, majorityCh <-chan bool) *Timeouts {
	return &Timeouts{
		partitionRole:   partitionRole,
		roleCh:          roleCh,
		majorityCh:      majorityCh,
		tickerHeartbeat: newTicker(),
		tickerElection:  newTicker(),
	}
}

// Timeouts manages timeouts (election and heartbeat) defined by raft protocol.
type Timeouts struct {
	partitionRole magmatypes.PartitionRole
	roleCh        <-chan types.Role
	majorityCh    <-chan bool

	tickerHeartbeat *ticker
	tickerElection  *ticker

	role            types.Role
	majorityPresent bool
}

// Run runs the timeout manager.
func (t *Timeouts) Run(ctx context.Context) error {
	t.tickerHeartbeat.Start(heartbeatInterval)

	defer t.tickerHeartbeat.Stop()
	defer t.tickerElection.Stop()

	for {
		select {
		case majorityPresent, ok := <-t.majorityCh:
			if !ok {
				return errors.WithStack(ctx.Err())
			}
			t.applyMajority(majorityPresent)
		case t.role = <-t.roleCh:
			t.applyRole(t.role)
		}
	}
}

// Heartbeat returns heartbeat ticks.
func (t *Timeouts) Heartbeat() <-chan time.Time {
	return t.tickerHeartbeat.Ticks()
}

// Election returns election ticks.
func (t *Timeouts) Election() <-chan time.Time {
	return t.tickerElection.Ticks()
}

func (t *Timeouts) applyRole(role types.Role) {
	switch role {
	case types.RoleFollower, types.RoleCandidate:
		if t.majorityPresent {
			if t.partitionRole == magmatypes.PartitionRoleActive {
				t.tickerElection.Start(electionBaseInterval)
			}
		} else {
			t.tickerElection.Stop()
		}
	case types.RoleLeader:
		t.tickerElection.Stop()
	}
}

func (t *Timeouts) applyMajority(majorityPresent bool) {
	if majorityPresent == t.majorityPresent {
		return
	}
	t.majorityPresent = majorityPresent
	if t.role == types.RoleLeader {
		return
	}
	if !majorityPresent {
		t.tickerElection.Stop()
		return
	}
	if t.partitionRole == magmatypes.PartitionRoleActive {
		t.tickerElection.Start(electionBaseInterval)
	}
}
