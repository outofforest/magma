package raft

import (
	"context"
	"math/rand"
	"time"

	"github.com/pkg/errors"

	"github.com/outofforest/magma/raft/reactor"
	"github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/raft/wire/p2p"
	"github.com/outofforest/parallel"
)

// Run starts the main execution flow for the Raft reactor.
func Run(ctx context.Context, reactor *reactor.Reactor) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		recvCh := make(chan p2p.Message, 10)
		sendCh := make(chan []p2p.Message, 10)
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

			return runConsumer(ctx, reactor, recvCh, sendCh, roleCh)
		})

		return nil
	})
}

func runTimeouts(ctx context.Context, roleCh <-chan types.Role, recvCh chan<- p2p.Message) error {
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
			recvCh <- p2p.Message{Msg: types.HeartbeatTimeout{
				Time: time.Now().Add(-heartbeatDuration),
			}}
		case <-tickerElection.C:
			// FIXME (wojciech): Using p2p.Message here is strange.
			recvCh <- p2p.Message{Msg: types.ElectionTimeout{
				Time: time.Now().Add(-electionDuration),
			}}
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

func runConsumer(
	ctx context.Context,
	reactor *reactor.Reactor,
	recvCh <-chan p2p.Message,
	sendCh chan<- []p2p.Message,
	roleCh chan types.Role,
) error {
	role := types.RoleFollower
	roleCh <- role
	for msg := range recvCh {
		newRole, messages, err := reactor.Apply(msg)
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
		if len(messages) > 0 {
			sendCh <- messages
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
