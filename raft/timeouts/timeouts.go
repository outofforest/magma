package timeouts

import (
	"context"
	"math/rand"
	"time"

	"github.com/pkg/errors"

	"github.com/outofforest/magma/raft/types"
)

const (
	heartbeatInterval    = 500 * time.Millisecond
	electionBaseInterval = 2 * time.Second
)

// New creates new timeout manager.
func New(roleCh <-chan types.Role, majorityCh <-chan bool) *Timeouts {
	t := &Timeouts{
		roleCh:          roleCh,
		majorityCh:      majorityCh,
		tickerHeartbeat: newTicker(),
		tickerElection:  newTicker(),
	}
	t.tickerHeartbeat.Stop()
	t.tickerElection.Stop()
	return t
}

// Timeouts manages timeouts (election and heartbeat) defined by raft protocol.
type Timeouts struct {
	roleCh     <-chan types.Role
	majorityCh <-chan bool

	electionInterval time.Duration
	tickerHeartbeat  *ticker
	tickerElection   *ticker

	role            types.Role
	majorityPresent bool
}

// Run runs the timeout manager.
func (t *Timeouts) Run(ctx context.Context) error {
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

// HeartbeatInterval returns interval of the heartbeat timeout.
func (t *Timeouts) HeartbeatInterval() time.Duration {
	return heartbeatInterval
}

// ElectionInterval returns interval of the election timeout.
func (t *Timeouts) ElectionInterval() time.Duration {
	return t.electionInterval
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
		t.tickerHeartbeat.Stop()
		if t.majorityPresent {
			t.electionInterval = electionInterval()
			t.tickerElection.Start(t.electionInterval)
		} else {
			t.tickerElection.Stop()
		}
	case types.RoleLeader:
		t.tickerHeartbeat.Start(heartbeatInterval)
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
	t.electionInterval = electionInterval()
	t.tickerElection.Start(t.electionInterval)
}

func electionInterval() time.Duration {
	return electionBaseInterval + time.Duration(rand.Intn(500))*time.Millisecond
}
