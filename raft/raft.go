package raft

import (
	"context"

	"github.com/pkg/errors"

	"github.com/outofforest/magma/raft/engine"
	"github.com/outofforest/magma/raft/timeouts"
	"github.com/outofforest/magma/raft/types"
	magmatypes "github.com/outofforest/magma/types"
	"github.com/outofforest/parallel"
)

const queueCapacity = 10

// GossipFunc is the declaration of the function responsible for gossiping messages between peers.
type GossipFunc func(
	ctx context.Context,
	cmdP2PCh, cmdC2PCh chan<- types.Command,
	resultCh <-chan engine.Result,
	majorityCh chan<- bool,
) error

// Run runs Raft processor.
func Run(ctx context.Context, e *engine.Engine, gossipFunc GossipFunc) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		cmdP2PCh := make(chan types.Command, queueCapacity)
		cmdC2PCh := make(chan types.Command, queueCapacity)
		resultCh := make(chan engine.Result, 1)
		roleCh := make(chan types.Role, 1)
		majorityCh := make(chan bool, 1)

		t := timeouts.New(roleCh, majorityCh)

		spawn("producers", parallel.Fail, func(ctx context.Context) error {
			defer close(cmdP2PCh)
			defer close(cmdC2PCh)

			return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
				spawn("timeoutProducer", parallel.Fail, t.Run)
				spawn("timeoutConsumer", parallel.Fail, func(ctx context.Context) error {
					return runTimeoutConsumer(ctx, t, cmdP2PCh)
				})
				spawn("gossip", parallel.Fail, func(ctx context.Context) error {
					defer func() {
						spawn("resultChCleaner", parallel.Fail, func(ctx context.Context) error {
							for range resultCh {
							}
							return errors.WithStack(ctx.Err())
						})
					}()
					defer close(majorityCh)

					return gossipFunc(ctx, cmdP2PCh, cmdC2PCh, resultCh, majorityCh)
				})
				return nil
			})
		})
		spawn("engine", parallel.Fail, func(ctx context.Context) error {
			defer func() {
				spawn("cmdP2PChCleaner", parallel.Fail, func(ctx context.Context) error {
					for range cmdP2PCh {
					}
					return errors.WithStack(ctx.Err())
				})
				spawn("cmdC2PChCleaner", parallel.Fail, func(ctx context.Context) error {
					for range cmdC2PCh {
					}
					return errors.WithStack(ctx.Err())
				})
			}()
			defer close(resultCh)

			return runEngine(ctx, e, cmdP2PCh, cmdC2PCh, resultCh, roleCh)
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
			cmdCh <- types.Command{Cmd: types.HeartbeatTimeout(tm.Add(-t.HeartbeatInterval()))}
		case tm := <-t.Election():
			cmdCh <- types.Command{Cmd: types.ElectionTimeout(tm.Add(-t.ElectionInterval()))}
		}
	}
}

func runEngine(
	ctx context.Context,
	e *engine.Engine,
	cmdP2PCh, cmdC2PCh <-chan types.Command,
	resultCh chan<- engine.Result,
	roleCh chan types.Role,
) error {
	role := types.RoleFollower
	roleCh <- role

	var commitInfo types.CommitInfo
	var leaderID magmatypes.ServerID

	for {
		cmd, err := fetchCommand(ctx, cmdP2PCh, cmdC2PCh)
		if err != nil {
			return err
		}

		newRole, result, err := e.Apply(cmd)
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

		if len(result.Recipients) > 0 || result.CommitInfo.NextLogIndex > commitInfo.NextLogIndex ||
			result.LeaderID != leaderID {
			commitInfo = result.CommitInfo
			leaderID = result.LeaderID
			resultCh <- result
		}
		continue
	}
}

func fetchCommand(ctx context.Context, cmdP2PCh, cmdC2PCh <-chan types.Command) (types.Command, error) {
	select {
	case cmd := <-cmdP2PCh:
		return cmd, nil
	default:
		select {
		case <-ctx.Done():
			return types.Command{}, errors.WithStack(ctx.Err())
		case cmd := <-cmdP2PCh:
			return cmd, nil
		case cmd := <-cmdC2PCh:
			return cmd, nil
		}
	}
}
