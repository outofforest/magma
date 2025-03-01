package gossip

import (
	"bytes"
	"context"
	"net"
	"sync"

	"github.com/pkg/errors"

	"github.com/outofforest/magma/gossip/p2p"
	"github.com/outofforest/magma/raft"
	"github.com/outofforest/magma/raft/engine"
	rafttypes "github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/parallel"
	"github.com/outofforest/resonance"
)

const queueCapacity = 100

var (
	// P2CConfig is the config of client connection.
	P2CConfig = resonance.Config{
		MaxMessageSize: 4 * 1024,
	}

	// P2PConfig is the config of peer connection.
	P2PConfig = resonance.Config{
		MaxMessageSize: 1024 * 1024,
	}
)

// New returns gossiping function.
func New(config types.Config, p2pListener, p2cListener net.Listener) raft.GossipFunc {
	peerChs := map[types.ServerID]chan any{}
	for _, p := range config.Servers {
		if p.ID != config.ServerID {
			peerChs[p.ID] = make(chan any, queueCapacity)
		}
	}

	return (&gossip{
		config:        config,
		p2pListener:   p2pListener,
		p2cListener:   p2cListener,
		p2pMarshaller: p2p.NewMarshaller(),
		peerChs:       peerChs,
		activeConns:   map[types.ServerID]*context.CancelFunc{},
	}).Run
}

type gossip struct {
	config                   types.Config
	p2pListener, p2cListener net.Listener

	p2pMarshaller p2p.Marshaller
	peerChs       map[types.ServerID]chan any

	mu          sync.Mutex
	activeConns map[types.ServerID]*context.CancelFunc
}

func (g *gossip) Run(
	ctx context.Context,
	cmdCh chan<- rafttypes.Command,
	sendCh <-chan engine.Send,
	controlCh chan<- rafttypes.PeerEvent,
) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("p2cListener", parallel.Fail, func(ctx context.Context) error {
			return resonance.RunServer(ctx, g.p2cListener, P2CConfig,
				func(ctx context.Context, c *resonance.Connection) error {
					return g.handleClient(ctx, cmdCh, c)
				},
			)
		})
		spawn("p2pListener", parallel.Fail, func(ctx context.Context) error {
			return resonance.RunServer(ctx, g.p2pListener, P2PConfig,
				func(ctx context.Context, c *resonance.Connection) error {
					return g.handlePeer(ctx, types.ZeroServerID, cmdCh, controlCh, c)
				},
			)
		})

		for _, s := range g.config.Servers {
			if s.ID == g.config.ServerID || !initConnection(g.config.ServerID, s.ID) {
				continue
			}
			spawn("p2pConnector", parallel.Fail, func(ctx context.Context) error {
				for {
					err := resonance.RunClient(ctx, s.P2PAddress, P2PConfig,
						func(ctx context.Context, c *resonance.Connection) error {
							return g.handlePeer(ctx, s.ID, cmdCh, controlCh, c)
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
						case g.peerChs[peerID] <- toSend.Message:
						default:
							select {
							case <-g.peerChs[peerID]:
							default:
							}

							g.peerChs[peerID] <- toSend.Message
						}
					}
				}
			}
		})
		return nil
	})
}

func (g *gossip) handlePeer(
	ctx context.Context,
	expectedPeerID types.ServerID,
	cmdCh chan<- rafttypes.Command,
	controlCh chan<- rafttypes.PeerEvent,
	c *resonance.Connection,
) error {
	if sent := c.SendProton(&p2p.Hello{
		ServerID: g.config.ServerID,
	}, g.p2pMarshaller); !sent {
		return errors.New("sending hello failed")
	}

	var peerID types.ServerID
	var sendCh <-chan any

	m, err := c.ReceiveProton(g.p2pMarshaller)
	if err != nil {
		return err
	}

	h, ok := m.(*p2p.Hello)
	if !ok {
		return errors.New("expected hello, got sth else")
	}

	sendCh = g.peerChs[h.ServerID]
	if sendCh == nil {
		return errors.New("unknown peer")
	}

	switch {
	case expectedPeerID == types.ZeroServerID:
		if initConnection(g.config.ServerID, h.ServerID) {
			return errors.New("peer should wait for my connection")
		}
	case h.ServerID != expectedPeerID:
		return errors.New("unexpected peer")
	}

	peerID = h.ServerID

	ctx, cancel := context.WithCancel(ctx)
	defer g.closeConnection(peerID, &cancel)

	g.replaceConnection(peerID, &cancel)

	cmdCh <- rafttypes.Command{
		PeerID: peerID,
	}

	connectionID := rafttypes.NewConnectionID()
	controlCh <- rafttypes.PeerEvent{
		PeerID:       peerID,
		ConnectionID: connectionID,
		Connected:    true,
	}
	defer func() {
		controlCh <- rafttypes.PeerEvent{
			PeerID:       peerID,
			ConnectionID: connectionID,
		}
	}()

	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("receiver", parallel.Fail, func(ctx context.Context) error {
			for {
				m, err := c.ReceiveProton(g.p2pMarshaller)
				if err != nil {
					return err
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
			}
		})
		spawn("sender", parallel.Fail, func(ctx context.Context) error {
			for {
				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case m := <-sendCh:
					c.SendProton(m, g.p2pMarshaller)
				}
			}
		})

		return nil
	})
}

func (g *gossip) handleClient(
	ctx context.Context,
	cmdCh chan<- rafttypes.Command,
	c *resonance.Connection,
) error {
	for {
		tx, err := c.ReceiveBytes()
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		case cmdCh <- rafttypes.Command{
			Cmd: &rafttypes.ClientRequest{
				Data: tx,
			},
		}:
		}
	}
}

func (g *gossip) replaceConnection(peerID types.ServerID, closer *context.CancelFunc) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if cancel := g.activeConns[peerID]; cancel != nil {
		(*cancel)()
	}

	g.activeConns[peerID] = closer
}

func (g *gossip) closeConnection(peerID types.ServerID, closer *context.CancelFunc) {
	defer (*closer)()

	g.mu.Lock()
	defer g.mu.Unlock()

	if cancel := g.activeConns[peerID]; cancel != closer {
		return
	}

	g.activeConns[peerID] = nil
}

func initConnection(serverID, peerID types.ServerID) bool {
	return bytes.Compare(serverID[:], peerID[:]) >= 0
}
