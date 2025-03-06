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
	syncInterval         = 100 * time.Millisecond
)

// New creates new timeout manager.
func New(roleCh <-chan types.Role, majorityCh <-chan bool) *Timeouts {
	return &Timeouts{
		roleCh:          roleCh,
		majorityCh:      majorityCh,
		tickerHeartbeat: newTicker(),
		tickerElection:  newTicker(),
		tickerSync:      newTicker(),
	}
}

// Timeouts manages timeouts (election and heartbeat) defined by raft protocol.
type Timeouts struct {
	roleCh     <-chan types.Role
	majorityCh <-chan bool

	tickerHeartbeat *ticker
	tickerElection  *ticker
	tickerSync      *ticker

	role            types.Role
	majorityPresent bool
}

// Run runs the timeout manager.
func (t *Timeouts) Run(ctx context.Context) error {
	t.tickerSync.Start(syncInterval)

	defer t.tickerHeartbeat.Stop()
	defer t.tickerElection.Stop()
	defer t.tickerSync.Stop()

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

// Sync returns sync ticks.
func (t *Timeouts) Sync() <-chan time.Time {
	return t.tickerSync.Ticks()
}

func (t *Timeouts) applyRole(role types.Role) {
	switch role {
	case types.RoleFollower, types.RoleCandidate:
		t.tickerHeartbeat.Stop()
		if t.majorityPresent {
			t.tickerElection.Start(electionInterval())
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
	t.tickerElection.Start(electionInterval())
}

func electionInterval() time.Duration {
	return electionBaseInterval + time.Duration(rand.Intn(500))*time.Millisecond
}
