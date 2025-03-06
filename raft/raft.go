package raft

import (
	"context"

	"github.com/pkg/errors"

	"github.com/outofforest/magma/raft/reactor"
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
	resultCh <-chan reactor.Result,
	majorityCh chan<- bool,
) error

// Run runs Raft processor.
func Run(ctx context.Context, r *reactor.Reactor, gossipFunc GossipFunc) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		cmdP2PCh := make(chan types.Command, queueCapacity)
		cmdC2PCh := make(chan types.Command, queueCapacity)
		resultCh := make(chan reactor.Result, 1)
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
		spawn("reactor", parallel.Fail, func(ctx context.Context) error {
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

			return runReactor(ctx, r, cmdP2PCh, cmdC2PCh, resultCh, roleCh)
		})

		return nil
	})
}

func runTimeoutConsumer(ctx context.Context, t *timeouts.Timeouts, cmdCh chan<- types.Command) error {
	var heartbeatTick types.HeartbeatTick
	var electionTick types.ElectionTick

	for {
		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		case <-t.Heartbeat():
			heartbeatTick++
			cmdCh <- types.Command{Cmd: heartbeatTick}
		case <-t.Election():
			electionTick++
			cmdCh <- types.Command{Cmd: electionTick}
		case <-t.Sync():
			cmdCh <- types.Command{Cmd: types.SyncTick{}}
		}
	}
}

func runReactor(
	ctx context.Context,
	r *reactor.Reactor,
	cmdP2PCh, cmdC2PCh <-chan types.Command,
	resultCh chan<- reactor.Result,
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

		result, err := r.Apply(cmd.PeerID, cmd.Cmd)
		if err != nil {
			return err
		}

		if result.Role != role {
			role = result.Role
			if len(roleCh) > 0 {
				select {
				case <-roleCh:
				default:
				}
			}
			roleCh <- role
		}

		if (len(result.Recipients) > 0 && len(result.Messages) > 0) || result.LeaderID != leaderID ||
			result.CommitInfo.CommittedCount > commitInfo.CommittedCount {
			commitInfo = result.CommitInfo
			leaderID = result.LeaderID
			resultCh <- result
		}
		continue
	}
}

func fetchCommand(ctx context.Context, cmdP2PCh, cmdC2PCh <-chan types.Command) (types.Command, error) {
	if len(cmdP2PCh) > 0 {
		return <-cmdP2PCh, nil
	}

	select {
	case <-ctx.Done():
		return types.Command{}, errors.WithStack(ctx.Err())
	case cmd := <-cmdP2PCh:
		return cmd, nil
	case cmd := <-cmdC2PCh:
		return cmd, nil
	}
}
