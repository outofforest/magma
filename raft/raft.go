package raft

import (
	"context"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/outofforest/logger"
	"github.com/outofforest/magma/gossip"
	"github.com/outofforest/magma/raft/partition"
	"github.com/outofforest/magma/raft/reactor"
	"github.com/outofforest/magma/raft/timeouts"
	"github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/state"
	magmatypes "github.com/outofforest/magma/types"
	"github.com/outofforest/parallel"
)

// Run runs Raft processor.
func Run(
	ctx context.Context,
	partitions map[magmatypes.PartitionID]partition.State,
	g *gossip.Gossip,
) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("producers", parallel.Fail, func(ctx context.Context) error {
			defer func() {
				for _, pState := range partitions {
					close(pState.CmdP2PCh)
					close(pState.CmdC2PCh)
				}
			}()

			return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
				for _, pState := range partitions {
					t := timeouts.New(pState.PartitionRole, pState.RoleCh, pState.MajorityCh)
					spawn("timeoutProducer", parallel.Fail, t.Run)
					spawn("timeoutConsumer", parallel.Fail, func(ctx context.Context) error {
						return runTimeoutConsumer(ctx, t, pState.CmdP2PCh)
					})
				}

				spawn("gossip", parallel.Fail, func(ctx context.Context) error {
					defer func() {
						for _, pState := range partitions {
							close(pState.MajorityCh)
							spawn("resultChCleaner", parallel.Fail, func(ctx context.Context) error {
								for range pState.ResultCh {
								}
								return errors.WithStack(ctx.Err())
							})
						}
					}()

					return g.Run(ctx)
				})
				return nil
			})
		})

		for _, pState := range partitions {
			spawn("reactor", parallel.Fail, func(ctx context.Context) error {
				defer func() {
					spawn("cmdP2PChCleaner", parallel.Fail, func(ctx context.Context) error {
						for range pState.CmdP2PCh {
						}
						return errors.WithStack(ctx.Err())
					})
					spawn("cmdC2PChCleaner", parallel.Fail, func(ctx context.Context) error {
						for range pState.CmdC2PCh {
						}
						return errors.WithStack(ctx.Err())
					})
				}()
				defer close(pState.ResultCh)

				return runReactor(ctx, pState.Reactor, pState.CmdP2PCh, pState.CmdC2PCh, pState.ResultCh, pState.RoleCh)
			})
		}

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
	log := logger.Get(ctx)

	role := types.RoleFollower
	roleCh <- role

	var commitInfo types.CommitInfo
	var leaderID magmatypes.ServerID

	for {
		cmd, err := fetchCommand(ctx, cmdP2PCh, cmdC2PCh)
		if err != nil {
			return err
		}

		res, err := r.Apply(cmd.PeerID, cmd.Cmd)
		if err != nil {
			if cmd.Cmd != nil {
				if _, ok := cmd.Cmd.(*types.ClientRequest); ok && errors.Is(err, state.ErrInvalidTransaction) {
					log.Error("Invalid transaction received from client.", zap.Error(err))
					continue
				}
			}
			return err
		}

		if res.Role != role {
			role = res.Role
			if len(roleCh) > 0 {
				select {
				case <-roleCh:
				default:
				}
			}
			roleCh <- role
		}

		if res.Channel != reactor.ChannelNone || res.LeaderID != leaderID ||
			res.CommitInfo.CommittedCount > commitInfo.CommittedCount ||
			res.CommitInfo.HotEndIndex > commitInfo.HotEndIndex || res.Force {
			commitInfo = res.CommitInfo
			leaderID = res.LeaderID
			resultCh <- res
		}
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
