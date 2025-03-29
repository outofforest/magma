package gossip

import (
	"bytes"
	"context"
	"net"

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
	"github.com/outofforest/magma/state/repository"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/parallel"
	"github.com/outofforest/resonance"
	"github.com/outofforest/varuint64"
)

const queueCapacity = 10

type peerP2P struct {
	ID          types.ServerID
	SendCh      chan any
	InstalledCh chan struct{}
	Connected   bool
}

type peerL2P struct {
	ID          types.ServerID
	SendCh      chan any
	InstalledCh chan struct{}
	Iterator    *repository.Iterator
	Connected   bool
}

type peerTx2P struct {
	ID         types.ServerID
	LeaderCh   chan peerTx2P
	Connection *resonance.Connection
	Connected  bool
}

type client struct {
	Iterator *repository.Iterator
	LeaderCh chan peerTx2P
}

// New returns gossiping function.
func New(
	config types.Config,
	p2pListener, c2pListener net.Listener,
	repo *repository.Repository,
) raft.GossipFunc {
	validPeers := map[types.ServerID]struct{}{}
	for _, p := range config.Servers {
		if p.ID != config.ServerID {
			validPeers[p.ID] = struct{}{}
		}
	}

	g := &gossip{
		config:          config,
		p2pListener:     p2pListener,
		c2pListener:     c2pListener,
		repo:            repo,
		minority:        len(config.Servers) / 2,
		validPeers:      validPeers,
		helloMarshaller: hello.NewMarshaller(),
		p2pMarshaller:   p2p.NewMarshaller(),
		l2pMarshaller:   l2p.NewMarshaller(),
		c2pMarshaller:   c2p.NewMarshaller(),
		providerPeers:   repository.NewTailProvider(),
		providerClients: repository.NewTailProvider(),
	}
	return g.Run
}

type gossip struct {
	config                   types.Config
	p2pListener, c2pListener net.Listener
	repo                     *repository.Repository
	minority                 int
	validPeers               map[types.ServerID]struct{}

	helloMarshaller hello.Marshaller
	p2pMarshaller   p2p.Marshaller
	l2pMarshaller   l2p.Marshaller
	c2pMarshaller   c2p.Marshaller

	providerPeers   *repository.TailProvider
	providerClients *repository.TailProvider
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

			resConfig := resonance.Config{MaxMessageSize: g.config.MaxMessageSize}

			return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
				spawn("p2pListener", parallel.Fail, func(ctx context.Context) error {
					return resonance.RunServer(ctx, g.p2pListener, resConfig,
						func(ctx context.Context, c *resonance.Connection) error {
							return g.peerHandler(ctx, types.ZeroServerID, wire.ChannelNone, peerP2PCh, peerL2PCh,
								peerTx2PCh, cmdP2PCh, c)
						},
					)
				})
				spawn("c2pListener", parallel.Fail, func(ctx context.Context) error {
					return resonance.RunServer(ctx, g.c2pListener, resConfig,
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
							err := resonance.RunClient(ctx, s.P2PAddress, resConfig,
								func(ctx context.Context, c *resonance.Connection) error {
									return g.peerHandler(ctx, s.ID, wire.ChannelP2P, peerP2PCh, peerL2PCh, peerTx2PCh,
										cmdP2PCh, c)
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
							err := resonance.RunClient(ctx, s.P2PAddress, resConfig,
								func(ctx context.Context, c *resonance.Connection) error {
									return g.peerHandler(ctx, s.ID, wire.ChannelL2P, peerP2PCh, peerL2PCh, peerTx2PCh,
										cmdP2PCh, c)
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
							err := resonance.RunClient(ctx, s.P2PAddress, resConfig,
								func(ctx context.Context, c *resonance.Connection) error {
									return g.peerHandler(ctx, s.ID, wire.ChannelTx2P, peerP2PCh, peerL2PCh, peerTx2PCh,
										cmdP2PCh, c)
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
	clients := map[*repository.Iterator]chan peerTx2P{}
	var commitInfo rafttypes.CommitInfo

	if g.minority == 0 {
		majorityCh <- true
	}

	var pLeader peerTx2P
	var leaderID types.ServerID

	for {
		select {
		case res, ok := <-resultCh:
			if !ok {
				resultCh = nil
				continue
			}

			//nolint:nestif
			if leaderID != res.LeaderID {
				leaderID = res.LeaderID
				if leaderID == g.config.ServerID {
					pLeader = peerTx2P{
						ID: g.config.ServerID,
					}
				} else {
					newPLeader := peersTx2P[leaderID]
					if newPLeader.ID != pLeader.ID {
						pLeader = newPLeader
					}
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
			switch res.Channel {
			case reactor.ChannelP2P:
				for _, peerID := range res.Recipients {
					if p := peersP2P[peerID]; p.SendCh != nil {
						p.SendCh <- res.Message
					}
				}
			case reactor.ChannelL2P:
				switch m := res.Message.(type) {
				case *reactor.StartTransfer:
					for _, peerID := range res.Recipients {
						if p := peersL2P[peerID]; p.SendCh != nil {
							p.Iterator = repository.NewIterator(g.providerPeers, g.repo.Iterator(m.NextLogIndex), m.NextLogIndex)
							peersL2P[peerID] = p
							p.SendCh <- p.Iterator
						}
					}
				default:
					for _, peerID := range res.Recipients {
						if p := peersL2P[peerID]; p.SendCh != nil {
							p.SendCh <- res.Message
						}
					}
				}
			}
			if res.CommitInfo.NextLogIndex > commitInfo.NextLogIndex {
				g.providerPeers.SetTail(res.CommitInfo.NextLogIndex)
			}
			if res.CommitInfo.CommittedCount > commitInfo.CommittedCount {
				g.providerClients.SetTail(res.CommitInfo.CommittedCount)
			}
			commitInfo = res.CommitInfo
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
					if pOld.Iterator != nil {
						if err := pOld.Iterator.Close(); err != nil {
							return err
						}
					}
				}
				peersL2P[p.ID] = p
				close(p.InstalledCh)
			case peersL2P[p.ID].SendCh == p.SendCh:
				it := peersL2P[p.ID].Iterator
				delete(peersL2P, p.ID)
				close(p.SendCh)
				if it != nil {
					if err := it.Close(); err != nil {
						return err
					}
				}
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
			if _, exists := clients[c.Iterator]; exists {
				close(c.LeaderCh)
				delete(clients, c.Iterator)
				if err := c.Iterator.Close(); err != nil {
					return err
				}
			} else {
				if pLeader.ID != types.ZeroServerID {
					c.LeaderCh <- pLeader
				}
				clients[c.Iterator] = c.LeaderCh
			}
		}
	}
}

func (g *gossip) peerHandler(
	ctx context.Context,
	expectedPeerID types.ServerID,
	channel wire.Channel,
	p2pPeerCh chan<- peerP2P,
	l2pPeerCh chan<- peerL2P,
	tx2pPeerCh chan<- peerTx2P,
	cmdCh chan<- rafttypes.Command,
	c *resonance.Connection,
) error {
	peerID, channel, err := g.hello(c, expectedPeerID, channel)
	if err != nil {
		return err
	}

	switch channel {
	case wire.ChannelP2P:
		return g.p2pHandler(ctx, peerID, p2pPeerCh, cmdCh, c)
	case wire.ChannelL2P:
		return g.l2pHandler(ctx, peerID, l2pPeerCh, cmdCh, c)
	case wire.ChannelTx2P:
		return g.tx2pHandler(peerID, tx2pPeerCh, cmdCh, c)
	default:
		return errors.Errorf("unexpected channel %d", channel)
	}
}

func (g *gossip) p2pHandler(
	ctx context.Context,
	peerID types.ServerID,
	peerCh chan<- peerP2P,
	cmdCh chan<- rafttypes.Command,
	c *resonance.Connection,
) error {
	c.BufferReads()
	c.BufferWrites()

	ch := make(chan any, queueCapacity)
	var sendCh <-chan any = ch

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

			for m := range sendCh {
				if err := c.SendProton(m, g.p2pMarshaller); err != nil {
					return err
				}
			}

			return errors.WithStack(ctx.Err())
		})

		return nil
	})
}

func (g *gossip) l2pHandler(
	ctx context.Context,
	peerID types.ServerID,
	peerCh chan<- peerL2P,
	cmdCh chan<- rafttypes.Command,
	c *resonance.Connection,
) error {
	c.BufferReads()

	ch := make(chan any, queueCapacity)
	var sendCh <-chan any = ch

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

			cmdCh <- rafttypes.Command{
				PeerID: peerID,
			}

			for {
				m, err := c.ReceiveProton(g.l2pMarshaller)
				if err != nil {
					return err
				}

				switch m.(type) {
				case *l2p.StartTransfer:
					for {
						m, err := c.ReceiveRawBytes()
						if err != nil {
							return err
						}

						cmdCh <- rafttypes.Command{
							PeerID: peerID,
							Cmd:    m,
						}
					}
				default:
					cmdCh <- rafttypes.Command{
						PeerID: peerID,
						Cmd:    m,
					}
				}
			}
		})
		spawn("send", parallel.Fail, func(ctx context.Context) error {
			defer func() {
				c.Close()
				for range sendCh {
				}
			}()

			for msg := range sendCh {
				switch m := msg.(type) {
				case *repository.Iterator:
					startMsg := &l2p.StartTransfer{}
					id, err := g.l2pMarshaller.ID(startMsg)
					if err != nil {
						return err
					}
					msgSize, err := g.l2pMarshaller.Size(startMsg)
					if err != nil {
						return err
					}
					startMsgSize := varuint64.Size(id) + msgSize
					startMsgSize += varuint64.Size(startMsgSize)

					initSentBytes := c.BytesSent() + startMsgSize
					if err := c.SendProton(startMsg, g.l2pMarshaller); err != nil {
						return err
					}
					for {
						if sentBytes := c.BytesSent(); sentBytes > initSentBytes {
							if err := m.Acknowledge(rafttypes.Index(sentBytes - initSentBytes)); err != nil {
								return err
							}
						}
						r, err := m.Reader()
						if err != nil {
							return err
						}
						if err := c.SendStream(r); err != nil {
							return err
						}
					}
				default:
					if err := c.SendProton(m, g.l2pMarshaller); err != nil {
						return err
					}
				}
			}

			return errors.WithStack(ctx.Err())
		})

		return nil
	})
}

func (g *gossip) tx2pHandler(
	peerID types.ServerID,
	peerCh chan<- peerTx2P,
	cmdCh chan<- rafttypes.Command,
	c *resonance.Connection,
) error {
	c.BufferReads()
	c.BufferWrites()

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
	c.BufferReads()

	msg, err := c.ReceiveProton(g.c2pMarshaller)
	if err != nil {
		return err
	}

	msgInit, ok := msg.(*c2p.Init)
	if !ok {
		return errors.New("expected init")
	}

	it := repository.NewIterator(g.providerClients, g.repo.Iterator(msgInit.NextLogIndex), msgInit.NextLogIndex)

	ch2 := make(chan peerTx2P, 1)
	var leaderCh <-chan peerTx2P = ch2
	cl := client{
		Iterator: it,
		LeaderCh: ch2,
	}
	clientCh <- cl

	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("receive", parallel.Fail, func(ctx context.Context) error {
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

			initSentBytes := c.BytesSent()
			for {
				if sentBytes := c.BytesSent(); sentBytes > initSentBytes {
					if err := it.Acknowledge(rafttypes.Index(sentBytes - initSentBytes)); err != nil {
						return err
					}
				}

				r, err := it.Reader()
				if err != nil {
					return err
				}
				if err := c.SendStream(r); err != nil {
					return err
				}
			}
		})

		return nil
	})
}

func (g *gossip) hello(
	c *resonance.Connection,
	expectedPeerID types.ServerID,
	channel wire.Channel,
) (types.ServerID, wire.Channel, error) {
	if expectedPeerID == types.ZeroServerID && channel != wire.ChannelNone {
		return types.ZeroServerID, wire.ChannelNone, errors.New("channel must not be provided on incoming peer")
	}

	if err := c.SendProton(&wire.Hello{
		ServerID: g.config.ServerID,
		Channel:  channel,
	}, g.helloMarshaller); err != nil {
		return types.ZeroServerID, wire.ChannelNone, err
	}

	m, err := c.ReceiveProton(g.helloMarshaller)
	if err != nil {
		return types.ZeroServerID, wire.ChannelNone, err
	}

	h, ok := m.(*wire.Hello)
	if !ok {
		return types.ZeroServerID, wire.ChannelNone, errors.New("expected hello, got sth else")
	}

	if _, exists := g.validPeers[h.ServerID]; !exists {
		return types.ZeroServerID, wire.ChannelNone, errors.New("unknown peer")
	}

	switch {
	case expectedPeerID == types.ZeroServerID:
		if initConnection(g.config.ServerID, h.ServerID) {
			return types.ZeroServerID, wire.ChannelNone, errors.New("peer should wait for my connection")
		}
		switch h.Channel {
		case wire.ChannelP2P, wire.ChannelL2P, wire.ChannelTx2P:
			channel = h.Channel
		default:
			return types.ZeroServerID, wire.ChannelNone, errors.Errorf("invalid channel %d", h.Channel)
		}
	case h.ServerID != expectedPeerID:
		return types.ZeroServerID, wire.ChannelNone, errors.New("unexpected peer")
	case h.Channel != wire.ChannelNone:
		return types.ZeroServerID, wire.ChannelNone, errors.New("peer must not announce requested channel")
	}

	return h.ServerID, channel, nil
}

func initConnection(serverID, peerID types.ServerID) bool {
	return bytes.Compare(serverID[:], peerID[:]) >= 0
}
