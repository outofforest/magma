package gossip

import (
	"context"
	"net"
	"strings"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/outofforest/logger"
	"github.com/outofforest/magma/gossip/wire"
	"github.com/outofforest/magma/gossip/wire/c2p"
	"github.com/outofforest/magma/gossip/wire/hello"
	"github.com/outofforest/magma/gossip/wire/l2p"
	"github.com/outofforest/magma/gossip/wire/p2p"
	"github.com/outofforest/magma/raft/partition"
	"github.com/outofforest/magma/raft/reactor"
	rafttypes "github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/state/repository"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/parallel"
	"github.com/outofforest/resonance"
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
	Connection  *resonance.Connection
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
	serverID types.ServerID,
	maxMessageSize uint64,
	p2pListener,
	c2pListener net.Listener,
	partitions map[types.PartitionID]partition.State) *Gossip {
	pStates := map[types.PartitionID]partitionState{}
	for id, p := range partitions {
		validPeers := map[types.ServerID]struct{}{}
		for _, peer := range p.Peers {
			validPeers[peer.ID] = struct{}{}
		}

		pStates[id] = partitionState{
			State: p,

			minority:        (len(p.Peers) + 1) / 2,
			validPeers:      validPeers,
			providerPeers:   repository.NewTailProvider(),
			providerClients: repository.NewTailProvider(),

			PeerP2PCh:  make(chan peerP2P),
			PeerL2PCh:  make(chan peerL2P),
			PeerTx2PCh: make(chan peerTx2P),
			ClientCh:   make(chan client),
		}
	}

	return &Gossip{
		serverID:        serverID,
		maxMessageSize:  maxMessageSize,
		p2pListener:     p2pListener,
		c2pListener:     c2pListener,
		partitions:      pStates,
		helloMarshaller: hello.NewMarshaller(),
		p2pMarshaller:   p2p.NewMarshaller(),
		l2pMarshaller:   l2p.NewMarshaller(),
		c2pMarshaller:   c2p.NewMarshaller(),
	}
}

type partitionState struct {
	partition.State

	minority        int
	validPeers      map[types.ServerID]struct{}
	providerPeers   *repository.TailProvider
	providerClients *repository.TailProvider

	PeerP2PCh  chan peerP2P
	PeerL2PCh  chan peerL2P
	PeerTx2PCh chan peerTx2P
	ClientCh   chan client
}

// Gossip is responsible for gossiping messages between peers and clients.
type Gossip struct {
	serverID                 types.ServerID
	maxMessageSize           uint64
	p2pListener, c2pListener net.Listener
	partitions               map[types.PartitionID]partitionState

	helloMarshaller hello.Marshaller
	p2pMarshaller   p2p.Marshaller
	l2pMarshaller   l2p.Marshaller
	c2pMarshaller   c2p.Marshaller
}

// Run runs the gossiper.
func (g *Gossip) Run(ctx context.Context) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("network", parallel.Fail, func(ctx context.Context) error {
			defer func() {
				for _, pState := range g.partitions {
					close(pState.ClientCh)
					close(pState.PeerTx2PCh)
					close(pState.PeerL2PCh)
					close(pState.PeerP2PCh)
				}
			}()

			resConfig := resonance.Config{MaxMessageSize: g.maxMessageSize}

			return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
				spawn("p2pListener", parallel.Fail, func(ctx context.Context) error {
					return resonance.RunServer(ctx, g.p2pListener, resConfig,
						func(ctx context.Context, c *resonance.Connection) error {
							return g.peerHandler(ctx, connProperties{}, c)
						},
					)
				})
				spawn("c2pListener", parallel.Fail, func(ctx context.Context) error {
					return resonance.RunServer(ctx, g.c2pListener, resConfig,
						func(ctx context.Context, c *resonance.Connection) error {
							return g.c2pHandler(ctx, c)
						},
					)
				})

				for pID, pState := range g.partitions {
					for _, s := range pState.Peers {
						if !initConnection(g.serverID, s.ID) {
							continue
						}

						spawn("p2pConnector", parallel.Fail, func(ctx context.Context) error {
							log := logger.Get(ctx)
							for {
								err := resonance.RunClient(ctx, s.P2PAddress, resConfig,
									func(ctx context.Context, c *resonance.Connection) error {
										return g.peerHandler(ctx, connProperties{
											PeerID:      s.ID,
											PartitionID: pID,
											Channel:     wire.ChannelP2P,
										}, c)
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
										return g.peerHandler(ctx, connProperties{
											PeerID:      s.ID,
											PartitionID: pID,
											Channel:     wire.ChannelL2P,
										}, c)
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
										return g.peerHandler(ctx, connProperties{
											PeerID:      s.ID,
											PartitionID: pID,
											Channel:     wire.ChannelTx2P,
										}, c)
									},
								)
								if ctx.Err() != nil {
									return errors.WithStack(ctx.Err())
								}

								log.Error("Connection failed", zap.Error(err))
							}
						})
					}
				}
				return nil
			})
		})
		for _, pState := range g.partitions {
			spawn("supervisor", parallel.Fail, func(ctx context.Context) error {
				return g.runSupervisor(ctx, pState)
			})
		}
		return nil
	})
}

//nolint:gocyclo
func (g *Gossip) runSupervisor(ctx context.Context, pState partitionState) error {
	peersP2P := map[types.ServerID]peerP2P{}
	peersL2P := map[types.ServerID]peerL2P{}
	peersTx2P := map[types.ServerID]peerTx2P{}
	clients := map[*repository.Iterator]chan peerTx2P{}
	var commitInfo rafttypes.CommitInfo

	if pState.minority == 0 {
		pState.MajorityCh <- true
	}

	var pLeader peerTx2P
	var leaderID types.ServerID

	for {
		select {
		case res, ok := <-pState.ResultCh:
			if !ok {
				pState.ResultCh = nil
				continue
			}

			//nolint:nestif
			if leaderID != res.LeaderID {
				if peer, exists := peersL2P[leaderID]; exists {
					peer.Connection.Close()
				}

				leaderID = res.LeaderID
				if leaderID == g.serverID {
					pLeader = peerTx2P{
						ID: g.serverID,
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
						if p := peersL2P[peerID]; p.SendCh != nil && p.Iterator == nil {
							p.Iterator = pState.Repo.Iterator(pState.providerPeers, m.NextIndex)
							peersL2P[peerID] = p
							p.SendCh <- p.Iterator
						}
					}
				default:
					for _, peerID := range res.Recipients {
						if p := peersL2P[peerID]; p.SendCh != nil && p.Iterator == nil {
							p.SendCh <- res.Message
						}
					}
				}
			}
			if res.CommitInfo.NextIndex != commitInfo.NextIndex && leaderID == g.serverID {
				pState.providerPeers.SetTail(res.CommitInfo.NextIndex, 0)
			}
			if res.CommitInfo.CommittedCount > commitInfo.CommittedCount ||
				res.CommitInfo.HotEndIndex > commitInfo.HotEndIndex {
				pState.providerClients.SetTail(res.CommitInfo.CommittedCount, res.CommitInfo.HotEndIndex)
			}
			commitInfo = res.CommitInfo
		case p, ok := <-pState.PeerP2PCh:
			if !ok {
				pState.PeerP2PCh = nil
				if pState.PeerP2PCh == nil && pState.PeerL2PCh == nil && pState.PeerTx2PCh == nil &&
					pState.ClientCh == nil {
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
				if !exists && len(peersP2P) == pState.minority {
					pState.MajorityCh <- true
				}
			case peersP2P[p.ID].SendCh == p.SendCh:
				delete(peersP2P, p.ID)
				close(p.SendCh)
				if len(peersP2P)+1 == pState.minority {
					pState.MajorityCh <- false
				}
			}
		case p, ok := <-pState.PeerL2PCh:
			if !ok {
				pState.PeerL2PCh = nil
				if pState.PeerP2PCh == nil && pState.PeerL2PCh == nil && pState.PeerTx2PCh == nil &&
					pState.ClientCh == nil {
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
		case p, ok := <-pState.PeerTx2PCh:
			if !ok {
				pState.PeerTx2PCh = nil
				if pState.PeerP2PCh == nil && pState.PeerL2PCh == nil && pState.PeerTx2PCh == nil &&
					pState.ClientCh == nil {
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
		case c, ok := <-pState.ClientCh:
			if !ok {
				pState.ClientCh = nil
				if pState.PeerP2PCh == nil && pState.PeerL2PCh == nil && pState.PeerTx2PCh == nil &&
					pState.ClientCh == nil {
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

type connProperties struct {
	PeerID      types.ServerID
	PartitionID types.PartitionID
	Channel     wire.Channel
}

func (g *Gossip) peerHandler(ctx context.Context, prop connProperties, c *resonance.Connection) error {
	prop, pState, err := g.hello(c, prop)
	if err != nil {
		return err
	}

	switch prop.Channel {
	case wire.ChannelP2P:
		return g.p2pHandler(ctx, prop.PeerID, pState.PeerP2PCh, pState.CmdP2PCh, c)
	case wire.ChannelL2P:
		return g.l2pHandler(ctx, prop.PeerID, pState.PeerL2PCh, pState.CmdP2PCh, c)
	case wire.ChannelTx2P:
		return g.tx2pHandler(prop.PeerID, pState.PeerTx2PCh, pState.CmdP2PCh, c)
	default:
		return errors.Errorf("unexpected channel %d", prop.Channel)
	}
}

func (g *Gossip) p2pHandler(
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

func (g *Gossip) l2pHandler(
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
				Connection:  c,
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

				switch msg := m.(type) {
				case *wire.StartLogStream:
					var length uint64
					for length < msg.Length {
						m, err := c.ReceiveRawBytes()
						if err != nil {
							return err
						}

						length += uint64(len(m))

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
					for {
						r, length, err := m.Reader(false)
						if err != nil {
							return err
						}

						if r == nil {
							if err := c.SendProton(&wire.HotEnd{}, g.l2pMarshaller); err != nil {
								return err
							}

							r, length, err = m.Reader(true)
							if err != nil {
								return err
							}
						}

						if err := c.SendProton(&wire.StartLogStream{
							Length: length,
						}, g.l2pMarshaller); err != nil {
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

func (g *Gossip) tx2pHandler(
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

		if leader.ID == g.serverID {
			cmdCh <- rafttypes.Command{
				Cmd: &rafttypes.ClientRequest{
					Data: tx,
				},
			}
			continue
		}
	}
}

func (g *Gossip) c2pHandler(ctx context.Context, c *resonance.Connection) error {
	c.BufferReads()

	msg, err := c.ReceiveProton(g.c2pMarshaller)
	if err != nil {
		return err
	}

	msgInit, ok := msg.(*c2p.InitRequest)
	if !ok {
		return errors.Errorf("expected init request, got: %T", msg)
	}

	pState, exists := g.partitions[msgInit.PartitionID]
	if !exists {
		return errors.Errorf("partition %s is not defined", msgInit.PartitionID)
	}

	if err := c.SendProton(&c2p.InitResponse{}, g.c2pMarshaller); err != nil {
		return err
	}

	it := pState.Repo.Iterator(pState.providerClients, msgInit.NextIndex)

	ch2 := make(chan peerTx2P, 1)
	var leaderCh <-chan peerTx2P = ch2
	cl := client{
		Iterator: it,
		LeaderCh: ch2,
	}
	pState.ClientCh <- cl

	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("receive", parallel.Fail, func(ctx context.Context) error {
			defer func() {
				pState.ClientCh <- cl
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
				case g.serverID:
					pState.CmdC2PCh <- rafttypes.Command{
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

			for {
				r, length, err := it.Reader(true)
				if err != nil {
					return err
				}

				if r == nil {
					if err := c.SendProton(&wire.HotEnd{}, g.c2pMarshaller); err != nil {
						return err
					}

					continue
				}

				if err := c.SendProton(&wire.StartLogStream{
					Length: length,
				}, g.c2pMarshaller); err != nil {
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

func (g *Gossip) hello(
	c *resonance.Connection,
	prop connProperties,
) (connProperties, partitionState, error) {
	if err := c.SendProton(&wire.Hello{
		ServerID:    g.serverID,
		PartitionID: prop.PartitionID,
		Channel:     prop.Channel,
	}, g.helloMarshaller); err != nil {
		return connProperties{}, partitionState{}, err
	}

	m, err := c.ReceiveProton(g.helloMarshaller)
	if err != nil {
		return connProperties{}, partitionState{}, err
	}

	h, ok := m.(*wire.Hello)
	if !ok {
		return connProperties{}, partitionState{}, errors.New("expected hello, got sth else")
	}

	switch {
	case prop == connProperties{}:
		if initConnection(g.serverID, h.ServerID) {
			return connProperties{}, partitionState{}, errors.New("peer should wait for my connection")
		}
		switch h.Channel {
		case wire.ChannelP2P, wire.ChannelL2P, wire.ChannelTx2P:
			prop.Channel = h.Channel
		default:
			return connProperties{}, partitionState{}, errors.Errorf("invalid channel %d", h.Channel)
		}

		if _, exists := g.partitions[h.PartitionID]; !exists {
			return connProperties{}, partitionState{}, errors.Errorf("invalid partition %s", h.PartitionID)
		}

		prop.PartitionID = h.PartitionID
	case h.ServerID != prop.PeerID:
		return connProperties{}, partitionState{}, errors.New("unexpected peer")
	case h.Channel != wire.ChannelNone:
		return connProperties{}, partitionState{}, errors.New("peer must not announce requested channel")
	case h.PartitionID != "":
		return connProperties{}, partitionState{}, errors.New("peer must not announce partition")
	}

	prop.PeerID = h.ServerID

	pState, exists := g.partitions[prop.PartitionID]
	if !exists {
		return connProperties{}, partitionState{}, errors.Errorf("partition %s is not defined", h.PartitionID)
	}

	if _, exists := pState.validPeers[prop.PeerID]; !exists {
		return connProperties{}, partitionState{}, errors.New("unknown peer")
	}

	return prop, pState, nil
}

func initConnection(serverID, peerID types.ServerID) bool {
	return strings.Compare(string(serverID), string(peerID)) >= 0
}
