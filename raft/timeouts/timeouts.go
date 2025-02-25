package timeouts

import (
	"context"
	"math/rand"
	"time"

	"github.com/pkg/errors"

	"github.com/outofforest/magma/raft/types"
	magmatypes "github.com/outofforest/magma/types"
)

const (
	heartbeatInterval    = 500 * time.Millisecond
	electionBaseInterval = 2 * time.Second
)

// New creates new timeout manager.
func New(majority int, roleCh <-chan types.Role, controlCh <-chan types.PeerEvent) *Timeouts {
	t := &Timeouts{
		majority:        majority,
		roleCh:          roleCh,
		controlCh:       controlCh,
		tickerHeartbeat: newTicker(),
		tickerElection:  newTicker(),
		connections:     map[magmatypes.ServerID]types.ConnectionID{},
	}
	t.tickerHeartbeat.Stop()
	t.tickerElection.Stop()
	return t
}

// Timeouts manages timeouts (election and heartbeat) defined by raft protocol.
type Timeouts struct {
	majority  int
	roleCh    <-chan types.Role
	controlCh <-chan types.PeerEvent

	electionInterval time.Duration
	tickerHeartbeat  *ticker
	tickerElection   *ticker

	role        types.Role
	connections map[magmatypes.ServerID]types.ConnectionID
}

// Run runs the timeout manager.
func (t *Timeouts) Run(ctx context.Context) error {
	defer t.tickerHeartbeat.Stop()
	defer t.tickerElection.Stop()

	for {
		select {
		case peerEvent, ok := <-t.controlCh:
			if !ok {
				return errors.WithStack(ctx.Err())
			}
			t.applyPeerEvent(peerEvent)
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
		if len(t.connections)+1 >= t.majority {
			t.electionInterval = electionInterval()
			t.tickerElection.Start(t.electionInterval)
		}
	case types.RoleLeader:
		t.tickerHeartbeat.Start(heartbeatInterval)
		t.tickerElection.Stop()
	}
}

func (t *Timeouts) applyPeerEvent(e types.PeerEvent) {
	if e.Connected {
		before := len(t.connections[e.PeerID])
		t.connections[e.PeerID] = e.ConnectionID
		if t.role == types.RoleLeader {
			return
		}
		if after := len(t.connections); after != before && after+1 == t.majority {
			t.electionInterval = electionInterval()
			t.tickerElection.Start(t.electionInterval)
		}
		return
	}

	if t.connections[e.PeerID] != e.ConnectionID {
		return
	}
	delete(t.connections, e.PeerID)
	if t.role == types.RoleLeader {
		return
	}
	if len(t.connections)+1 == t.majority-1 {
		t.tickerElection.Stop()
	}
}

func electionInterval() time.Duration {
	return electionBaseInterval + time.Duration(rand.Intn(500))*time.Millisecond
}
