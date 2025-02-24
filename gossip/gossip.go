package gossip

import (
	"bytes"
	"context"
	"net"
	"time"

	"github.com/pkg/errors"

	"github.com/outofforest/magma/gossip/p2p"
	"github.com/outofforest/magma/helpers"
	"github.com/outofforest/magma/raft"
	"github.com/outofforest/magma/raft/engine"
	rafttypes "github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/parallel"
	"github.com/outofforest/resonance"
)

const (
	helloTimeout   = 2 * time.Second
	sendChCapacity = 50
)

// New returns gossiping function.
func New(config types.Config) raft.GossipFunc {
	p2pConfig := resonance.Config[p2p.Marshaller]{
		MaxMessageSize:    1024 * 1024,
		MarshallerFactory: p2p.NewMarshaller,
	}

	peers := map[types.ServerID]chan any{}
	for _, p := range helpers.Peers(config.ServerID, config.Servers) {
		peers[p] = make(chan any, sendChCapacity)
	}

	return func(ctx context.Context, cmdCh chan<- rafttypes.Command, sendCh <-chan engine.Send) error {
		return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
			spawn("p2pListener", parallel.Fail, func(ctx context.Context) error {
				l, err := net.Listen("tcp", config.ListenAddrPeers)
				if err != nil {
					return errors.WithStack(err)
				}
				defer l.Close()

				return resonance.RunServer(ctx, l, p2pConfig,
					func(ctx context.Context, recvCh <-chan any, c *resonance.Connection[p2p.Marshaller]) error {
						return handlePeer(ctx, config.ServerID, peers, false, cmdCh, recvCh, c)
					},
				)
			})
			for peerID := range peers {
				if !initConnection(config.ServerID, peerID) {
					continue
				}
				spawn("p2pConnector", parallel.Fail, func(ctx context.Context) error {
					for {
						err := resonance.RunClient[p2p.Marshaller](ctx, "", p2pConfig,
							func(ctx context.Context, recvCh <-chan any, c *resonance.Connection[p2p.Marshaller]) error {
								return handlePeer(ctx, config.ServerID, peers, true, cmdCh, recvCh, c)
							},
						)
						if errors.Is(err, ctx.Err()) {
							return errors.WithStack(err)
						}
					}
				})
			}
			spawn("p2pSender", parallel.Fail, func(ctx context.Context) error {
				for {
					select {
					case <-ctx.Done():
						return errors.WithStack(ctx.Err())
					case toSend := <-sendCh:
						for _, peerID := range toSend.Recipients {
							select {
							case peers[peerID] <- toSend.Message:
							default:
								select {
								case <-peers[peerID]:
								default:
								}

								peers[peerID] <- toSend.Message
							}
						}
					}
				}
			})
			return nil
		})
	}
}

func handlePeer(
	ctx context.Context,
	serverID types.ServerID,
	validPeers map[types.ServerID]chan any,
	startedByMe bool,
	cmdCh chan<- rafttypes.Command,
	recvCh <-chan any,
	c *resonance.Connection[p2p.Marshaller],
) error {
	if sent, _ := c.SendIfPossible(&p2p.Hello{
		ServerID: serverID,
	}); !sent {
		return errors.WithStack(ctx.Err())
	}

	var peerID types.ServerID
	var sendCh <-chan any
	select {
	case <-time.After(helloTimeout):
		return errors.WithStack(ctx.Err())
	case m, ok := <-recvCh:
		if !ok {
			return errors.WithStack(ctx.Err())
		}

		h, ok := m.(*p2p.Hello)
		if !ok {
			return errors.WithStack(ctx.Err())
		}

		sendCh = validPeers[h.ServerID]
		if sendCh == nil {
			return errors.WithStack(ctx.Err())
		}

		if !startedByMe && initConnection(serverID, h.ServerID) {
			return errors.WithStack(ctx.Err())
		}

		peerID = h.ServerID
	}

	select {
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case cmdCh <- rafttypes.Command{
		PeerID: peerID,
	}:
	}

	for {
		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		case m, ok := <-recvCh:
			if !ok {
				return errors.WithStack(ctx.Err())
			}
			if _, ok := m.(*p2p.Hello); ok {
				return errors.WithStack(ctx.Err())
			}

			select {
			case <-ctx.Done():
				return errors.WithStack(ctx.Err())
			case cmdCh <- rafttypes.Command{
				PeerID: peerID,
				Cmd:    m,
			}:
			}
		case m := <-sendCh:
			c.SendIfPossible(m)
		}
	}
}

func initConnection(serverID, peerID types.ServerID) bool {
	return bytes.Compare(serverID[:], peerID[:]) >= 0
}
