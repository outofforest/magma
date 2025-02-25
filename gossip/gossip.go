package gossip

import (
	"bytes"
	"context"
	"net"
	"time"

	"github.com/pkg/errors"

	"github.com/outofforest/magma/gossip/p2c"
	"github.com/outofforest/magma/gossip/p2p"
	"github.com/outofforest/magma/raft"
	"github.com/outofforest/magma/raft/engine"
	rafttypes "github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/parallel"
	"github.com/outofforest/resonance"
)

const (
	helloTimeout  = time.Second
	queueCapacity = 100
)

var (
	// P2CConfig is the config of client connection.
	P2CConfig = resonance.Config[p2c.Marshaller]{
		MaxMessageSize:    4 * 1024,
		MarshallerFactory: p2c.NewMarshaller,
	}

	// P2PConfig is the config of peer connection.
	P2PConfig = resonance.Config[p2p.Marshaller]{
		MaxMessageSize:    1024 * 1024,
		MarshallerFactory: p2p.NewMarshaller,
	}
)

// New returns gossiping function.
func New(config types.Config, p2pListener, p2cListener net.Listener) raft.GossipFunc {
	peerAddrs := map[types.ServerID]string{}
	peerChs := map[types.ServerID]chan any{}
	for _, p := range config.Servers {
		if p.ID != config.ServerID {
			peerChs[p.ID] = make(chan any, queueCapacity)
			peerAddrs[p.ID] = p.P2PAddress
		}
	}

	return func(ctx context.Context, cmdCh chan<- rafttypes.Command, sendCh <-chan engine.Send) error {
		return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
			spawn("p2cListener", parallel.Fail, func(ctx context.Context) error {
				return resonance.RunServer(ctx, p2cListener, P2CConfig,
					func(ctx context.Context, recvCh <-chan any, c *resonance.Connection[p2c.Marshaller]) error {
						return handleClient(ctx, cmdCh, recvCh)
					},
				)
			})
			spawn("p2pListener", parallel.Fail, func(ctx context.Context) error {
				return resonance.RunServer(ctx, p2pListener, P2PConfig,
					func(ctx context.Context, recvCh <-chan any, c *resonance.Connection[p2p.Marshaller]) error {
						return handlePeer(ctx, config.ServerID, peerChs, false, cmdCh, recvCh, c)
					},
				)
			})
			for peerID, peerAddr := range peerAddrs {
				if !initConnection(config.ServerID, peerID) {
					continue
				}
				spawn("p2pConnector", parallel.Fail, func(ctx context.Context) error {
					for {
						err := resonance.RunClient[p2p.Marshaller](ctx, peerAddr, P2PConfig,
							func(ctx context.Context, recvCh <-chan any, c *resonance.Connection[p2p.Marshaller]) error {
								return handlePeer(ctx, config.ServerID, peerChs, true, cmdCh, recvCh, c)
							},
						)
						if err != nil && errors.Is(err, ctx.Err()) {
							return err
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
							case peerChs[peerID] <- toSend.Message:
							default:
								select {
								case <-peerChs[peerID]:
								default:
								}

								peerChs[peerID] <- toSend.Message
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
	peers map[types.ServerID]chan any,
	startedByMe bool,
	cmdCh chan<- rafttypes.Command,
	recvCh <-chan any,
	c *resonance.Connection[p2p.Marshaller],
) error {
	if sent := c.Send(&p2p.Hello{
		ServerID: serverID,
	}); !sent {
		return errors.New("sending hello failed")
	}

	var peerID types.ServerID
	var sendCh <-chan any
	select {
	case <-time.After(helloTimeout):
		return errors.New("timeout waiting for hello")
	case m, ok := <-recvCh:
		if !ok {
			return errors.WithStack(ctx.Err())
		}

		h, ok := m.(*p2p.Hello)
		if !ok {
			return errors.New("expected hello, got sth else")
		}

		sendCh = peers[h.ServerID]
		if sendCh == nil {
			return errors.New("unknown peer")
		}

		if !startedByMe && initConnection(serverID, h.ServerID) {
			return errors.New("peer should wait for my connection")
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
				return errors.New("unexpected hello")
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
			c.Send(m)
		}
	}
}

func handleClient(
	ctx context.Context,
	cmdCh chan<- rafttypes.Command,
	recvCh <-chan any,
) error {
	for {
		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		case m, ok := <-recvCh:
			if !ok {
				return errors.WithStack(ctx.Err())
			}

			req, ok := m.(*rafttypes.ClientRequest)
			if !ok {
				return errors.New("unknown message")
			}

			select {
			case <-ctx.Done():
				return errors.WithStack(ctx.Err())
			case cmdCh <- rafttypes.Command{
				Cmd: req,
			}:
			}
		}
	}
}

func initConnection(serverID, peerID types.ServerID) bool {
	return bytes.Compare(serverID[:], peerID[:]) >= 0
}
