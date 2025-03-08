package gossip

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/outofforest/logger"
	"github.com/outofforest/magma/gossip/wire"
	"github.com/outofforest/magma/gossip/wire/c2p"
	"github.com/outofforest/magma/gossip/wire/hello"
	"github.com/outofforest/magma/gossip/wire/l2p"
	"github.com/outofforest/magma/gossip/wire/p2p"
	"github.com/outofforest/magma/raft"
	"github.com/outofforest/magma/raft/reactor"
	rafttypes "github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/parallel"
	"github.com/outofforest/resonance"
)

const queueCapacity = 10

type peerP2P struct {
	ID          types.ServerID
	SendCh      chan []any
	InstalledCh chan struct{}
	Connected   bool
}

type peerL2P struct {
	ID          types.ServerID
	SendCh      chan []any
	InstalledCh chan struct{}
	Connected   bool
}

type peerTx2P struct {
	ID         types.ServerID
	LeaderCh   chan peerTx2P
	Connection *resonance.Connection
	Connected  bool
}

type client struct {
	CommitCh chan rafttypes.CommitInfo
	LeaderCh chan peerTx2P
}

// New returns gossiping function.
func New(config types.Config, p2pListener, l2pListener, tx2pListener, c2pListener net.Listener) raft.GossipFunc {
	validPeers := map[types.ServerID]struct{}{}
	for _, p := range config.Servers {
		if p.ID != config.ServerID {
			validPeers[p.ID] = struct{}{}
		}
	}

	g := &gossip{
		config:          config,
		p2pListener:     p2pListener,
		l2pListener:     l2pListener,
		tx2pListener:    tx2pListener,
		c2pListener:     c2pListener,
		minority:        len(config.Servers) / 2,
		validPeers:      validPeers,
		helloMarshaller: hello.NewMarshaller(),
		p2pMarshaller:   p2p.NewMarshaller(),
		l2pMarshaller:   l2p.NewMarshaller(),
		c2pMarshaller:   c2p.NewMarshaller(),
	}
	return g.Run
}

type gossip struct {
	config                                              types.Config
	p2pListener, l2pListener, tx2pListener, c2pListener net.Listener
	minority                                            int
	validPeers                                          map[types.ServerID]struct{}

	helloMarshaller hello.Marshaller
	p2pMarshaller   p2p.Marshaller
	l2pMarshaller   l2p.Marshaller
	c2pMarshaller   c2p.Marshaller
}

func (g *gossip) Run(
	ctx context.Context,
	cmdP2PCh, cmdC2PCh chan<- rafttypes.Command,
	resultCh <-chan reactor.Result,
	majorityCh chan<- bool,
) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		peerP2PCh := make(chan peerP2P)
		peerL2PCh := make(chan peerL2P)
		peerTx2PCh := make(chan peerTx2P)
		clientCh := make(chan client)

		spawn("network", parallel.Fail, func(ctx context.Context) error {
			defer close(peerP2PCh)
			defer close(peerL2PCh)
			defer close(peerTx2PCh)
			defer close(clientCh)

			return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
				spawn("p2pListener", parallel.Fail, func(ctx context.Context) error {
					return resonance.RunServer(ctx, g.p2pListener, g.config.P2P,
						func(ctx context.Context, c *resonance.Connection) error {
							return g.p2pHandler(ctx, types.ZeroServerID, peerP2PCh, cmdP2PCh, c)
						},
					)
				})
				spawn("l2pListener", parallel.Fail, func(ctx context.Context) error {
					return resonance.RunServer(ctx, g.l2pListener, g.config.P2P,
						func(ctx context.Context, c *resonance.Connection) error {
							return g.l2pHandler(ctx, types.ZeroServerID, peerL2PCh, cmdP2PCh, c)
						},
					)
				})
				spawn("tx2pListener", parallel.Fail, func(ctx context.Context) error {
					return resonance.RunServer(ctx, g.tx2pListener, g.config.C2P,
						func(ctx context.Context, c *resonance.Connection) error {
							return g.tx2pHandler(types.ZeroServerID, peerTx2PCh, cmdC2PCh, c)
						},
					)
				})
				spawn("c2pListener", parallel.Fail, func(ctx context.Context) error {
					return resonance.RunServer(ctx, g.c2pListener, g.config.C2P,
						func(ctx context.Context, c *resonance.Connection) error {
							return g.c2pHandler(ctx, clientCh, cmdC2PCh, c)
						},
					)
				})

				for _, s := range g.config.Servers {
					if s.ID == g.config.ServerID || !initConnection(g.config.ServerID, s.ID) {
						continue
					}
					spawn("p2pConnector", parallel.Fail, func(ctx context.Context) error {
						log := logger.Get(ctx)
						for {
							err := resonance.RunClient(ctx, s.P2PAddress, g.config.P2P,
								func(ctx context.Context, c *resonance.Connection) error {
									return g.p2pHandler(ctx, s.ID, peerP2PCh, cmdP2PCh, c)
								},
							)
							if ctx.Err() != nil {
								return errors.WithStack(ctx.Err())
							}

							log.Error("Connection failed", zap.Error(err))
						}
					})
					spawn("l2pConnector", parallel.Fail, func(ctx context.Context) error {
						log := logger.Get(ctx)
						for {
							err := resonance.RunClient(ctx, s.L2PAddress, g.config.P2P,
								func(ctx context.Context, c *resonance.Connection) error {
									return g.l2pHandler(ctx, s.ID, peerL2PCh, cmdP2PCh, c)
								},
							)
							if ctx.Err() != nil {
								return errors.WithStack(ctx.Err())
							}

							log.Error("Connection failed", zap.Error(err))
						}
					})
					spawn("tx2pConnector", parallel.Fail, func(ctx context.Context) error {
						log := logger.Get(ctx)

						for {
							err := resonance.RunClient(ctx, s.Tx2PAddress, g.config.C2P,
								func(ctx context.Context, c *resonance.Connection) error {
									return g.tx2pHandler(s.ID, peerTx2PCh, cmdC2PCh, c)
								},
							)
							if ctx.Err() != nil {
								return errors.WithStack(ctx.Err())
							}

							log.Error("Connection failed", zap.Error(err))
						}
					})
				}
				return nil
			})
		})
		spawn("supervisor", parallel.Fail, func(ctx context.Context) error {
			return g.runSupervisor(ctx, peerP2PCh, peerL2PCh, peerTx2PCh, clientCh, resultCh, majorityCh)
		})
		return nil
	})
}

//nolint:gocyclo
func (g *gossip) runSupervisor(
	ctx context.Context,
	peerP2PCh <-chan peerP2P,
	peerL2PCh <-chan peerL2P,
	peerTx2PCh <-chan peerTx2P,
	clientCh <-chan client,
	resultCh <-chan reactor.Result,
	majorityCh chan<- bool,
) error {
	peersP2P := map[types.ServerID]peerP2P{}
	peersL2P := map[types.ServerID]peerL2P{}
	peersTx2P := map[types.ServerID]peerTx2P{}
	clients := map[chan rafttypes.CommitInfo]chan peerTx2P{}
	var commitInfo rafttypes.CommitInfo

	if g.minority == 0 {
		majorityCh <- true
	}

	var pLeader peerTx2P
	var leaderID types.ServerID

	for {
		select {
		case result, ok := <-resultCh:
			if !ok {
				resultCh = nil
				continue
			}
			//nolint:nestif
			if leaderID != result.LeaderID {
				leaderID = result.LeaderID
				if leaderID == g.config.ServerID {
					pLeader = peerTx2P{
						ID: g.config.ServerID,
					}
				} else {
					newPLeader := peersTx2P[leaderID]
					if newPLeader.ID == pLeader.ID {
						continue
					}
					pLeader = newPLeader
				}

				for _, p := range peersTx2P {
					if len(p.LeaderCh) > 0 {
						select {
						case <-p.LeaderCh:
						default:
						}
					}
					p.LeaderCh <- pLeader
				}
				for _, cLCh := range clients {
					if len(cLCh) > 0 {
						select {
						case <-cLCh:
						default:
						}
					}
					cLCh <- pLeader
				}
			}
			for _, peerID := range result.Recipients {
				p, exists := peersP2P[peerID]
				if !exists {
					continue
				}

				p.SendCh <- result.Messages
			}
			if result.CommitInfo.CommittedCount > commitInfo.CommittedCount {
				commitInfo = result.CommitInfo
				for commitCh := range clients {
					if len(commitCh) > 0 {
						select {
						case <-commitCh:
						default:
						}
					}
					commitCh <- commitInfo
				}
			}
		case p, ok := <-peerP2PCh:
			if !ok {
				peerP2PCh = nil
				if peerP2PCh == nil && peerL2PCh == nil && peerTx2PCh == nil && clientCh == nil {
					return errors.WithStack(ctx.Err())
				}
				continue
			}
			switch {
			case p.Connected:
				pOld, exists := peersP2P[p.ID]
				if exists {
					close(pOld.SendCh)
				}
				peersP2P[p.ID] = p
				close(p.InstalledCh)
				if !exists && len(peersP2P) == g.minority {
					majorityCh <- true
				}
			case peersP2P[p.ID].SendCh == p.SendCh:
				delete(peersP2P, p.ID)
				close(p.SendCh)
				if len(peersP2P)+1 == g.minority {
					majorityCh <- false
				}
			}
		case p, ok := <-peerL2PCh:
			if !ok {
				peerL2PCh = nil
				if peerP2PCh == nil && peerL2PCh == nil && peerTx2PCh == nil && clientCh == nil {
					return errors.WithStack(ctx.Err())
				}
				continue
			}
			switch {
			case p.Connected:
				pOld, exists := peersL2P[p.ID]
				if exists {
					close(pOld.SendCh)
				}
				peersL2P[p.ID] = p
				close(p.InstalledCh)
			case peersL2P[p.ID].SendCh == p.SendCh:
				delete(peersL2P, p.ID)
				close(p.SendCh)
			}
		case p, ok := <-peerTx2PCh:
			if !ok {
				peerTx2PCh = nil
				if peerP2PCh == nil && peerL2PCh == nil && peerTx2PCh == nil && clientCh == nil {
					return errors.WithStack(ctx.Err())
				}
				continue
			}
			switch {
			case p.Connected:
				pOld, exists := peersTx2P[p.ID]
				if exists {
					close(pOld.LeaderCh)
				}
				peersTx2P[p.ID] = p

				switch {
				case p.ID == leaderID:
					pLeader = p
					for _, p := range peersTx2P {
						if len(p.LeaderCh) > 0 {
							select {
							case <-p.LeaderCh:
							default:
							}
						}
						p.LeaderCh <- pLeader
					}
					for _, cLCh := range clients {
						if len(cLCh) > 0 {
							select {
							case <-cLCh:
							default:
							}
						}
						cLCh <- pLeader
					}
				case pLeader.ID != types.ZeroServerID:
					p.LeaderCh <- pLeader
				}
			case peersTx2P[p.ID].LeaderCh == p.LeaderCh:
				delete(peersTx2P, p.ID)
				close(p.LeaderCh)
			}
		case c, ok := <-clientCh:
			if !ok {
				clientCh = nil
				if peerP2PCh == nil && peerL2PCh == nil && peerTx2PCh == nil && clientCh == nil {
					return errors.WithStack(ctx.Err())
				}
				continue
			}
			if _, exists := clients[c.CommitCh]; exists {
				close(c.CommitCh)
				close(c.LeaderCh)
				delete(clients, c.CommitCh)
			} else {
				if commitInfo.CommittedCount > 0 {
					c.CommitCh <- commitInfo
				}
				if pLeader.ID != types.ZeroServerID {
					c.LeaderCh <- pLeader
				}
				clients[c.CommitCh] = c.LeaderCh
			}
		}
	}
}

func (g *gossip) p2pHandler(
	ctx context.Context,
	expectedPeerID types.ServerID,
	peerCh chan<- peerP2P,
	cmdCh chan<- rafttypes.Command,
	c *resonance.Connection,
) error {
	peerID, err := g.hello(c, expectedPeerID)
	if err != nil {
		return err
	}

	ch := make(chan []any, queueCapacity)
	var sendCh <-chan []any = ch

	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("receive", parallel.Fail, func(ctx context.Context) error {
			p := peerP2P{
				ID:          peerID,
				SendCh:      ch,
				InstalledCh: make(chan struct{}),
				Connected:   true,
			}
			peerCh <- p
			defer func() {
				p.Connected = false
				peerCh <- p
			}()

			<-p.InstalledCh

			cmdCh <- rafttypes.Command{
				PeerID: peerID,
			}

			for {
				m, err := c.ReceiveProton(g.p2pMarshaller)
				if err != nil {
					return err
				}

				cmdCh <- rafttypes.Command{
					PeerID: peerID,
					Cmd:    m,
				}
			}
		})
		spawn("send", parallel.Fail, func(ctx context.Context) error {
			defer func() {
				c.Close()
				for range sendCh {
				}
			}()

			for ms := range sendCh {
				for _, m := range ms {
					if err := c.SendProton(m, g.p2pMarshaller); err != nil {
						return errors.WithStack(err)
					}
				}
			}

			return errors.WithStack(ctx.Err())
		})

		return nil
	})
}

func (g *gossip) l2pHandler(
	ctx context.Context,
	expectedPeerID types.ServerID,
	peerCh chan<- peerL2P,
	cmdCh chan<- rafttypes.Command,
	c *resonance.Connection,
) error {
	peerID, err := g.hello(c, expectedPeerID)
	if err != nil {
		return err
	}

	ch := make(chan []any, queueCapacity)
	var sendCh <-chan []any = ch

	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("receive", parallel.Fail, func(ctx context.Context) error {
			p := peerL2P{
				ID:          peerID,
				SendCh:      ch,
				InstalledCh: make(chan struct{}),
				Connected:   true,
			}
			peerCh <- p
			defer func() {
				p.Connected = false
				peerCh <- p
			}()

			<-p.InstalledCh

			for {
				m, err := c.ReceiveProton(g.l2pMarshaller)
				if err != nil {
					return err
				}

				cmdCh <- rafttypes.Command{
					PeerID: peerID,
					Cmd:    m,
				}
			}
		})
		spawn("send", parallel.Fail, func(ctx context.Context) error {
			defer func() {
				c.Close()
				for range sendCh {
				}
			}()

			for ms := range sendCh {
				for _, m := range ms {
					if err := c.SendProton(m, g.p2pMarshaller); err != nil {
						return errors.WithStack(err)
					}
				}
			}

			return errors.WithStack(ctx.Err())
		})

		return nil
	})
}

func (g *gossip) tx2pHandler(
	expectedPeerID types.ServerID,
	peerCh chan<- peerTx2P,
	cmdCh chan<- rafttypes.Command,
	c *resonance.Connection,
) error {
	peerID, err := g.hello(c, expectedPeerID)
	if err != nil {
		return err
	}

	ch := make(chan peerTx2P, 1)
	var leaderCh <-chan peerTx2P = ch
	p := peerTx2P{
		ID:         peerID,
		LeaderCh:   ch,
		Connection: c,
		Connected:  true,
	}
	peerCh <- p
	defer func() {
		p.Connected = false
		peerCh <- p
	}()

	var leader peerTx2P
	for {
		tx, err := c.ReceiveRawBytes()
		if err != nil {
			return err
		}

		if len(leaderCh) > 0 {
			leader = <-leaderCh
		}

		if leader.ID == g.config.ServerID {
			cmdCh <- rafttypes.Command{
				Cmd: &rafttypes.ClientRequest{
					Data: tx,
				},
			}
			continue
		}
	}
}

func (g *gossip) c2pHandler(
	ctx context.Context,
	clientCh chan<- client,
	cmdCh chan<- rafttypes.Command,
	c *resonance.Connection,
) error {
	msg, err := c.ReceiveProton(g.c2pMarshaller)
	if err != nil {
		return err
	}

	msgCommitInfo, ok := msg.(*rafttypes.CommitInfo)
	if !ok {
		return errors.New("expected init")
	}

	ch := make(chan rafttypes.CommitInfo, 1)
	var commitCh <-chan rafttypes.CommitInfo = ch

	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("receive", parallel.Fail, func(ctx context.Context) error {
			ch2 := make(chan peerTx2P, 1)
			var leaderCh <-chan peerTx2P = ch2
			cl := client{
				CommitCh: ch,
				LeaderCh: ch2,
			}
			clientCh <- cl
			defer func() {
				clientCh <- cl
			}()

			var leader peerTx2P
			for {
				tx, err := c.ReceiveRawBytes()
				if err != nil {
					return err
				}

				if len(leaderCh) > 0 {
					leader = <-leaderCh
				}

				switch leader.ID {
				case g.config.ServerID:
					cmdCh <- rafttypes.Command{
						Cmd: &rafttypes.ClientRequest{
							Data: tx,
						},
					}
				case types.ZeroServerID:
				default:
					if leader.Connection.SendRawBytes(tx) != nil {
						leader = peerTx2P{}
					}
				}
			}
		})
		spawn("send", parallel.Fail, func(ctx context.Context) error {
			defer c.Close()

			nextLogIndex := msgCommitInfo.CommittedCount
			var logF *os.File
			for newCommitInfo := range commitCh {
				if logF == nil {
					var err error
					logF, err = os.Open(filepath.Join(g.config.StateDir, "log"))
					if err != nil {
						return errors.WithStack(err)
					}
					if nextLogIndex > 0 {
						if _, err := logF.Seek(int64(nextLogIndex), io.SeekStart); err != nil {
							return errors.WithStack(err)
						}
					}
				}

				if newCommitInfo.CommittedCount > nextLogIndex {
					toSend := uint64(newCommitInfo.CommittedCount - nextLogIndex)
					if err := c.SendStream(io.LimitReader(logF, int64(toSend))); err != nil {
						return errors.WithStack(err)
					}
					nextLogIndex += rafttypes.Index(toSend)
				}
			}

			return errors.WithStack(ctx.Err())
		})

		return nil
	})
}

func (g *gossip) hello(c *resonance.Connection, expectedPeerID types.ServerID) (types.ServerID, error) {
	if err := c.SendProton(&wire.Hello{
		ServerID: g.config.ServerID,
	}, g.helloMarshaller); err != nil {
		return types.ZeroServerID, err
	}

	m, err := c.ReceiveProton(g.helloMarshaller)
	if err != nil {
		return types.ZeroServerID, err
	}

	h, ok := m.(*wire.Hello)
	if !ok {
		return types.ZeroServerID, errors.New("expected hello, got sth else")
	}

	if _, exists := g.validPeers[h.ServerID]; !exists {
		return types.ZeroServerID, errors.New("unknown peer")
	}

	switch {
	case expectedPeerID == types.ZeroServerID:
		if initConnection(g.config.ServerID, h.ServerID) {
			return types.ZeroServerID, errors.New("peer should wait for my connection")
		}
	case h.ServerID != expectedPeerID:
		return types.ZeroServerID, errors.New("unexpected peer")
	}

	return h.ServerID, nil
}

func initConnection(serverID, peerID types.ServerID) bool {
	return bytes.Compare(serverID[:], peerID[:]) >= 0
}
