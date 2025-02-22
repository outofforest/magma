package raft

import (
	"context"
	"math/rand"
	"time"

	"github.com/pkg/errors"

	"github.com/outofforest/magma/raft/reactor"
	"github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/raft/wire/p2c"
	"github.com/outofforest/magma/raft/wire/p2p"
	"github.com/outofforest/parallel"
)

// Run starts the main execution flow for the Raft reactor.
func Run(ctx context.Context, serverID types.ServerID, servers []types.ServerID, reactor *reactor.Reactor) error {
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

			return runConsumer(ctx, serverID, servers, reactor, recvCh, sendCh, roleCh)
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
			recvCh <- p2p.Message{Msg: types.HeartbeatTimeout(time.Now().Add(-heartbeatDuration))}
		case <-tickerElection.C:
			// FIXME (wojciech): Using p2p.Message here is strange.
			recvCh <- p2p.Message{Msg: types.ElectionTimeout(time.Now().Add(-electionDuration))}
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

//nolint:gocyclo
func runConsumer(
	ctx context.Context,
	serverID types.ServerID,
	servers []types.ServerID,
	reactor *reactor.Reactor,
	recvCh <-chan p2p.Message,
	sendCh chan<- []p2p.Message,
	roleCh chan types.Role,
) error {
	peers := make([]types.ServerID, 0, len(servers))
	for _, s := range servers {
		if s != serverID {
			peers = append(peers, s)
		}
	}
	callsInProgress := map[types.ServerID]p2p.MessageID{}

	var leaderID types.ServerID
	role := types.RoleFollower
	roleCh <- role

	var messages []p2p.Message
	for msg := range recvCh {
		switch m := msg.Msg.(type) {
		case p2p.AppendEntriesRequest:
			resp, err := reactor.ApplyAppendEntriesRequest(msg.PeerID, m)
			if err != nil {
				return err
			}
			if resp.MessageID == p2p.ZeroMessageID {
				break
			}
			messages = []p2p.Message{
				{
					PeerID: msg.PeerID,
					Msg:    resp,
				},
			}
		case p2p.AppendEntriesResponse:
			if callsInProgress[msg.PeerID] != m.MessageID {
				continue
			}
			resp, err := reactor.ApplyAppendEntriesResponse(msg.PeerID, m, peers)
			if err != nil {
				return err
			}
			callsInProgress[msg.PeerID] = resp.MessageID
			if resp.MessageID == p2p.ZeroMessageID {
				break
			}
			messages = []p2p.Message{
				{
					PeerID: msg.PeerID,
					Msg:    resp,
				},
			}
		case p2p.VoteRequest:
			resp, err := reactor.ApplyVoteRequest(msg.PeerID, m)
			if err != nil {
				return err
			}
			if resp.MessageID == p2p.ZeroMessageID {
				break
			}
			messages = []p2p.Message{
				{
					PeerID: msg.PeerID,
					Msg:    resp,
				},
			}
		case p2p.VoteResponse:
			if callsInProgress[msg.PeerID] != m.MessageID {
				continue
			}
			resp, err := reactor.ApplyVoteResponse(msg.PeerID, m, peers)
			if err != nil {
				return err
			}
			callsInProgress[msg.PeerID] = p2p.ZeroMessageID
			if resp.MessageID == p2p.ZeroMessageID {
				break
			}
			messages = make([]p2p.Message, 0, len(peers))
			for _, p := range peers {
				if callsInProgress[p] != p2p.ZeroMessageID {
					continue
				}
				callsInProgress[p] = resp.MessageID
				messages = append(messages, p2p.Message{
					PeerID: msg.PeerID,
					Msg:    resp,
				})
			}
		case p2c.ClientRequest:
			if leaderID == types.ZeroServerID {
				continue
			}
			if leaderID == serverID {
				resp, err := reactor.ApplyClientRequest(m, peers)
				if err != nil {
					return err
				}
				if resp.MessageID == p2p.ZeroMessageID {
					break
				}
				messages = make([]p2p.Message, 0, len(peers))
				for _, p := range peers {
					if callsInProgress[p] != p2p.ZeroMessageID {
						continue
					}
					callsInProgress[p] = resp.MessageID
					messages = append(messages, p2p.Message{
						PeerID: msg.PeerID,
						Msg:    resp,
					})
				}
				break
			}
			// We redirect request to leader, but only once, to avoid infinite hops.
			if msg.PeerID != types.ZeroServerID {
				continue
			}
			msg.PeerID = leaderID
			messages = []p2p.Message{
				msg,
			}
		case types.HeartbeatTimeout:
			resp, err := reactor.ApplyHeartbeatTimeout(time.Time(m), peers)
			if err != nil {
				return err
			}
			if resp.MessageID == p2p.ZeroMessageID {
				break
			}
			messages = make([]p2p.Message, 0, len(peers))
			for _, p := range peers {
				if callsInProgress[p] != p2p.ZeroMessageID {
					continue
				}
				callsInProgress[p] = resp.MessageID
				messages = append(messages, p2p.Message{
					PeerID: msg.PeerID,
					Msg:    resp,
				})
			}
		case types.ElectionTimeout:
			resp, err := reactor.ApplyElectionTimeout(time.Time(m), peers)
			if err != nil {
				return err
			}
			if resp.MessageID == p2p.ZeroMessageID {
				break
			}
			messages = make([]p2p.Message, 0, len(peers))
			for _, p := range peers {
				if callsInProgress[p] != p2p.ZeroMessageID {
					continue
				}
				callsInProgress[p] = resp.MessageID
				messages = append(messages, p2p.Message{
					PeerID: msg.PeerID,
					Msg:    resp,
				})
			}
		case types.ServerID:
			resp, err := reactor.ApplyPeerConnected(m)
			if err != nil {
				return err
			}
			if resp.MessageID == p2p.ZeroMessageID {
				break
			}
			callsInProgress[msg.PeerID] = resp.MessageID
			messages = []p2p.Message{
				{
					PeerID: msg.PeerID,
					Msg:    resp,
				},
			}
		default:
			return errors.Errorf("unexpected message type %T", m)
		}

		var newRole types.Role
		newRole, leaderID = reactor.Info()
		if newRole != role {
			clear(callsInProgress)

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
