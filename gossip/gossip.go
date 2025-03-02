package gossip

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"path/filepath"

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

const queueCapacity = 10

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

type peer struct {
	ID        types.ServerID
	Ch        chan any
	Connected bool
}

// New returns gossiping function.
func New(config types.Config, p2pListener, p2cListener net.Listener, stateDir string) raft.GossipFunc {
	validPeers := map[types.ServerID]struct{}{}
	for _, p := range config.Servers {
		if p.ID != config.ServerID {
			validPeers[p.ID] = struct{}{}
		}
	}
	return (&gossip{
		config:        config,
		p2pListener:   p2pListener,
		p2cListener:   p2cListener,
		stateDir:      stateDir,
		validPeers:    validPeers,
		minority:      len(config.Servers) / 2,
		p2pMarshaller: p2p.NewMarshaller(),
		p2cMarshaller: p2c.NewMarshaller(),
	}).Run
}

type gossip struct {
	config                   types.Config
	p2pListener, p2cListener net.Listener
	stateDir                 string
	validPeers               map[types.ServerID]struct{}
	minority                 int

	p2pMarshaller p2p.Marshaller
	p2cMarshaller p2c.Marshaller
}

//nolint:gocyclo
func (g *gossip) Run(
	ctx context.Context,
	cmdCh chan<- rafttypes.Command,
	sendCh <-chan engine.Send,
	majorityCh chan<- bool,
) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		peerCh := make(chan peer)
		clientCh := make(chan chan rafttypes.CommitInfo)

		spawn("p2cListener", parallel.Fail, func(ctx context.Context) error {
			return resonance.RunServer(ctx, g.p2cListener, P2CConfig,
				func(ctx context.Context, c *resonance.Connection) error {
					commitCh := make(chan rafttypes.CommitInfo, 1)

					select {
					case <-ctx.Done():
						return errors.WithStack(ctx.Err())
					case clientCh <- commitCh:
					}

					defer func() {
						select {
						case <-ctx.Done():
						case clientCh <- commitCh:
						}
					}()

					return g.handleClient(ctx, commitCh, cmdCh, c)
				},
			)
		})
		spawn("p2pListener", parallel.Fail, func(ctx context.Context) error {
			return resonance.RunServer(ctx, g.p2pListener, P2PConfig,
				func(ctx context.Context, c *resonance.Connection) error {
					return g.handlePeer(ctx, types.ZeroServerID, peerCh, cmdCh, c)
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
							return g.handlePeer(ctx, s.ID, peerCh, cmdCh, c)
						},
					)
					if err != nil && errors.Is(err, ctx.Err()) {
						return err
					}
				}
			})
		}
		spawn("sender", parallel.Fail, func(ctx context.Context) error {
			peers := map[types.ServerID]chan any{}
			clientChs := map[chan rafttypes.CommitInfo]struct{}{}
			var commitInfo rafttypes.CommitInfo

			if g.minority == 0 {
				majorityCh <- true
			}

			for {
				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case toSend := <-sendCh:
					for _, peerID := range toSend.Recipients {
						ch := peers[peerID]
						if ch == nil {
							continue
						}

						select {
						case ch <- toSend.Message:
						default:
							select {
							case <-ch:
							default:
							}

							ch <- toSend.Message
						}
					}
					if toSend.CommitInfo.NextLogIndex > commitInfo.NextLogIndex {
						commitInfo = toSend.CommitInfo
						for ch := range clientChs {
							select {
							case <-ch:
							default:
							}
							ch <- commitInfo
						}
					}
				case p := <-peerCh:
					switch {
					case p.Connected:
						ch := peers[p.ID]
						if ch != nil {
							close(ch)
						}
						peers[p.ID] = p.Ch
						if ch == nil && len(peers) == g.minority {
							majorityCh <- true
						}
					case peers[p.ID] == p.Ch:
						delete(peers, p.ID)
						if len(peers)+1 == g.minority {
							majorityCh <- false
						}
					}
				case ch := <-clientCh:
					if _, exists := clientChs[ch]; exists {
						delete(clientChs, ch)
					} else {
						if commitInfo.NextLogIndex > 0 {
							ch <- commitInfo
						}
						clientChs[ch] = struct{}{}
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
	peerCh chan<- peer,
	cmdCh chan<- rafttypes.Command,
	c *resonance.Connection,
) error {
	if sent := c.SendProton(&p2p.Hello{
		ServerID: g.config.ServerID,
	}, g.p2pMarshaller); !sent {
		return errors.New("sending hello failed")
	}

	var peerID types.ServerID

	m, err := c.ReceiveProton(g.p2pMarshaller)
	if err != nil {
		return err
	}

	h, ok := m.(*p2p.Hello)
	if !ok {
		return errors.New("expected hello, got sth else")
	}

	if _, exists := g.validPeers[h.ServerID]; !exists {
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

	cmdCh <- rafttypes.Command{
		PeerID: peerID,
	}

	ch := make(chan any, queueCapacity)
	p := peer{
		ID:        peerID,
		Ch:        ch,
		Connected: true,
	}
	select {
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case peerCh <- p:
	}

	defer func() {
		p.Connected = false
		select {
		case <-ctx.Done():
		case peerCh <- p:
		}
	}()

	var sendCh <-chan any = ch

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
			defer c.Close()

			for {
				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case m, ok := <-sendCh:
					if !ok {
						return errors.WithStack(err)
					}
					c.SendProton(m, g.p2pMarshaller)
				}
			}
		})

		return nil
	})
}

func (g *gossip) handleClient(
	ctx context.Context,
	commitCh <-chan rafttypes.CommitInfo,
	cmdCh chan<- rafttypes.Command,
	c *resonance.Connection,
) error {
	msg, err := c.ReceiveProton(g.p2cMarshaller)
	if err != nil {
		return err
	}

	msgCommitInfo, ok := msg.(*rafttypes.CommitInfo)
	if !ok {
		return errors.New("expected init")
	}

	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("receive", parallel.Fail, func(ctx context.Context) error {
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
		})
		spawn("send", parallel.Fail, func(ctx context.Context) error {
			nextLogIndex := msgCommitInfo.NextLogIndex
			var logF *os.File
			for {
				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case newCommitInfo := <-commitCh:
					if logF == nil {
						var err error
						logF, err = os.Open(filepath.Join(g.stateDir, "log"))
						if err != nil {
							return errors.WithStack(err)
						}
						if nextLogIndex > 0 {
							if _, err := logF.Seek(int64(nextLogIndex), io.SeekStart); err != nil {
								return errors.WithStack(err)
							}
						}
					}

					if newCommitInfo.NextLogIndex > nextLogIndex {
						toSend := uint64(newCommitInfo.NextLogIndex - nextLogIndex)
						if !c.SendStream(io.LimitReader(logF, int64(toSend))) {
							return errors.WithStack(ctx.Err())
						}
						nextLogIndex += rafttypes.Index(toSend)
					}
				}
			}
		})

		return nil
	})
}

func initConnection(serverID, peerID types.ServerID) bool {
	return bytes.Compare(serverID[:], peerID[:]) >= 0
}
