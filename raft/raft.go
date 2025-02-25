package raft

import (
	"context"

	"github.com/pkg/errors"

	"github.com/outofforest/magma/raft/engine"
	"github.com/outofforest/magma/raft/timeouts"
	"github.com/outofforest/magma/raft/types"
	"github.com/outofforest/parallel"
)

const queueCapacity = 100

// GossipFunc is the declaration of the function responsible for gossiping messages between peers.
type GossipFunc func(
	ctx context.Context,
	cmdCh chan<- types.Command,
	sendCh <-chan engine.Send,
	controlCh chan<- types.PeerEvent,
) error

// Run runs Raft processor.
func Run(ctx context.Context, e *engine.Engine, majority int, gossipFunc GossipFunc) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		cmdCh := make(chan types.Command, queueCapacity)
		sendCh := make(chan engine.Send, 1)
		roleCh := make(chan types.Role, 1)
		controlCh := make(chan types.PeerEvent, 1)

		t := timeouts.New(majority, roleCh, controlCh)

		spawn("producers", parallel.Fail, func(ctx context.Context) error {
			defer close(cmdCh)

			return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
				spawn("timeoutProducer", parallel.Fail, t.Run)
				spawn("timeoutConsumer", parallel.Fail, func(ctx context.Context) error {
					return runTimeoutConsumer(ctx, t, cmdCh)
				})
				spawn("gossip", parallel.Fail, func(ctx context.Context) error {
					defer close(controlCh)

					return gossipFunc(ctx, cmdCh, sendCh, controlCh)
				})
				return nil
			})
		})
		spawn("engine", parallel.Fail, func(ctx context.Context) error {
			defer close(sendCh)

			return runEngine(ctx, e, cmdCh, sendCh, roleCh)
		})

		return nil
	})
}

func runTimeoutConsumer(ctx context.Context, t *timeouts.Timeouts, cmdCh chan<- types.Command) error {
	for {
		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		case tm := <-t.Heartbeat():
			// FIXME (wojciech): Using types.Message here is strange.
			cmdCh <- types.Command{Cmd: types.HeartbeatTimeout(tm.Add(-t.HeartbeatInterval()))}
		case tm := <-t.Election():
			// FIXME (wojciech): Using types.Message here is strange.
			cmdCh <- types.Command{Cmd: types.ElectionTimeout(tm.Add(-t.ElectionInterval()))}
		}
	}
}

func runEngine(
	ctx context.Context,
	e *engine.Engine,
	cmdCh <-chan types.Command,
	sendCh chan<- engine.Send,
	roleCh chan types.Role,
) error {
	role := types.RoleFollower
	roleCh <- role

	for cmd := range cmdCh {
		newRole, toSend, err := e.Apply(cmd)
		if err != nil {
			return err
		}

		if newRole != role {
			role = newRole
			select {
			case <-roleCh:
			default:
			}
			roleCh <- role
		}

		if len(toSend.Recipients) > 0 {
			sendCh <- toSend
		}
	}

	return errors.WithStack(ctx.Err())
}
