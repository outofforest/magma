package raft

import (
	"context"
	"math/rand"
	"time"

	"github.com/pkg/errors"

	"github.com/outofforest/magma/raft/engine"
	"github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/raft/wire/p2p"
	"github.com/outofforest/parallel"
)

// Run runs Raft processor.
func Run(ctx context.Context, e *engine.Engine) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		recvCh := make(chan types.Command, 10)
		sendCh := make(chan engine.Send, 10)
		roleCh := make(chan types.Role, 1)

		spawn("producers", parallel.Fail, func(ctx context.Context) error {
			defer close(recvCh)

			return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
				spawn("timeouts", parallel.Fail, func(ctx context.Context) error {
					return runTimeouts(ctx, roleCh, recvCh)
				})
				spawn("peers", parallel.Fail, func(ctx context.Context) error {
					<-ctx.Done()
					return errors.WithStack(ctx.Err())
				})
				return nil
			})
		})
		spawn("consumer", parallel.Fail, func(ctx context.Context) error {
			defer close(sendCh)

			return runEngine(ctx, e, recvCh, sendCh, roleCh)
		})

		return nil
	})
}

func runTimeouts(ctx context.Context, roleCh <-chan types.Role, recvCh chan<- types.Command) error {
	heartbeatDuration := heartbeatTimeout()
	electionDuration := electionTimeout()

	tickerHeartbeat := time.NewTicker(time.Hour)
	defer tickerHeartbeat.Stop()

	tickerElection := time.NewTicker(time.Hour)
	defer tickerElection.Stop()

	tickerHeartbeat.Stop()
	tickerElection.Stop()

	for {
		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		case <-tickerHeartbeat.C:
			// FIXME (wojciech): Using p2p.Message here is strange.
			recvCh <- types.Command{Cmd: types.HeartbeatTimeout(time.Now().Add(-heartbeatDuration))}
		case <-tickerElection.C:
			// FIXME (wojciech): Using p2p.Message here is strange.
			recvCh <- types.Command{Cmd: types.ElectionTimeout(time.Now().Add(-electionDuration))}
		case role := <-roleCh:
			switch role {
			case types.RoleFollower:
				tickerHeartbeat.Stop()
				tickerElection.Reset(electionDuration)
			case types.RoleCandidate:
				tickerHeartbeat.Stop()
				tickerElection.Reset(electionDuration)
			case types.RoleLeader:
				tickerHeartbeat.Reset(heartbeatDuration)
				tickerElection.Stop()
			}
		}
	}
}

func runEngine(
	ctx context.Context,
	e *engine.Engine,
	recvCh <-chan types.Command,
	sendCh chan<- engine.Send,
	roleCh chan types.Role,
) error {
	role := types.RoleFollower
	roleCh <- role

	for cmd := range recvCh {
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

		if toSend.MessageID != p2p.ZeroMessageID {
			sendCh <- toSend
		}
	}

	return errors.WithStack(ctx.Err())
}

func electionTimeout() time.Duration {
	return 2*time.Second + time.Duration(rand.Intn(500))*time.Millisecond
}

func heartbeatTimeout() time.Duration {
	return 200 * time.Millisecond
}
