package gossip

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/outofforest/logger"
	"github.com/outofforest/magma/gossip/wire"
	"github.com/outofforest/magma/gossip/wire/c2p"
	"github.com/outofforest/magma/gossip/wire/p2p"
	"github.com/outofforest/magma/gossip/wire/tx2p"
	"github.com/outofforest/magma/raft"
	"github.com/outofforest/magma/raft/reactor"
	rafttypes "github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/parallel"
	"github.com/outofforest/resonance"
)

const queueCapacity = 10

type peerP2P struct {
	ID        types.ServerID
	SendCh    chan []any
	Connected bool
}

type peerTx2P struct {
	ID        types.ServerID
	LeaderCh  chan chan<- []byte
	Connected bool
}

type client struct {
	CommitCh chan rafttypes.CommitInfo
	LeaderCh chan chan<- []byte
}

// New returns gossiping function.
func New(
	config types.Config,
	p2pListener, tx2pListener, c2pListener net.Listener,
	stateDir string,
) raft.GossipFunc {
	tx2PChs := map[types.ServerID]chan []byte{}
	for _, p := range config.Servers {
		if p.ID != config.ServerID {
			tx2PChs[p.ID] = make(chan []byte)
		}
	}

	g := &gossip{
		config:                   config,
		p2pListener:              p2pListener,
		tx2pListener:             tx2pListener,
		c2pListener:              c2pListener,
		stateDir:                 stateDir,
		tx2PChs:                  tx2PChs,
		minority:                 len(config.Servers) / 2,
		p2pMarshaller:            p2p.NewMarshaller(),
		tx2pMarshaller:           tx2p.NewMarshaller(),
		c2pMarshaller:            c2p.NewMarshaller(),
		passthroughTimeoutTicker: time.NewTicker(time.Hour),
	}
	g.passthroughTimeoutTicker.Stop()
	return g.Run
}

type gossip struct {
	config                                 types.Config
	p2pListener, tx2pListener, c2pListener net.Listener
	stateDir                               string
	tx2PChs                                map[types.ServerID]chan []byte
	minority                               int

	p2pMarshaller  p2p.Marshaller
	tx2pMarshaller tx2p.Marshaller
	c2pMarshaller  c2p.Marshaller

	passthroughTimeoutTicker *time.Ticker
}

func (g *gossip) Run(
	ctx context.Context,
	cmdP2PCh, cmdC2PCh chan<- rafttypes.Command,
	resultCh <-chan reactor.Result,
	majorityCh chan<- bool,
) error {
	g.passthroughTimeoutTicker.Reset(g.config.PassthroughTimeout)
	defer g.passthroughTimeoutTicker.Stop()

	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		peerP2PCh := make(chan peerP2P)
		peerTx2PCh := make(chan peerTx2P)
		clientCh := make(chan client)

		spawn("network", parallel.Fail, func(ctx context.Context) error {
			defer close(peerP2PCh)
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
				spawn("tx2pListener", parallel.Fail, func(ctx context.Context) error {
					return resonance.RunServer(ctx, g.tx2pListener, g.config.C2P,
						func(ctx context.Context, c *resonance.Connection) error {
							return g.tx2pHandler(ctx, types.ZeroServerID, peerTx2PCh, cmdC2PCh, c)
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
					spawn("tx2pConnector", parallel.Fail, func(ctx context.Context) error {
						log := logger.Get(ctx)

						for {
							err := resonance.RunClient(ctx, s.Tx2PAddress, g.config.C2P,
								func(ctx context.Context, c *resonance.Connection) error {
									return g.tx2pHandler(ctx, s.ID, peerTx2PCh, cmdC2PCh, c)
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
			return g.runSupervisor(ctx, peerP2PCh, peerTx2PCh, clientCh, resultCh, majorityCh)
		})
		return nil
	})
}

//nolint:gocyclo
func (g *gossip) runSupervisor(
	ctx context.Context,
	peerP2PCh <-chan peerP2P,
	peerTx2PCh <-chan peerTx2P,
	clientCh <-chan client,
	resultCh <-chan reactor.Result,
	majorityCh chan<- bool,
) error {
	peersP2P := map[types.ServerID]peerP2P{}
	peersTx2P := map[types.ServerID]peerTx2P{}
	clients := map[chan rafttypes.CommitInfo]chan chan<- []byte{}
	var commitInfo rafttypes.CommitInfo

	if g.minority == 0 {
		majorityCh <- true
	}

	var leaderID types.ServerID
	var lCh chan<- []byte

	for {
		select {
		case result, ok := <-resultCh:
			if !ok {
				resultCh = nil
				continue
			}
			if leaderID != result.LeaderID {
				leaderID = result.LeaderID
				lCh = g.tx2PChs[leaderID]
				for _, p := range peersTx2P {
					select {
					case <-p.LeaderCh:
					default:
					}
					p.LeaderCh <- lCh
				}
				for _, cLCh := range clients {
					select {
					case <-cLCh:
					default:
					}
					cLCh <- lCh
				}
			}
			for _, peerID := range result.Recipients {
				p, exists := peersP2P[peerID]
				if !exists {
					continue
				}

				p.SendCh <- result.Messages
			}
			if result.CommitInfo.NextLogIndex > commitInfo.NextLogIndex {
				commitInfo = result.CommitInfo
				for commitCh := range clients {
					select {
					case <-commitCh:
					default:
					}
					commitCh <- commitInfo
				}
			}
		case p, ok := <-peerP2PCh:
			if !ok {
				peerP2PCh = nil
				if peerP2PCh == nil && peerTx2PCh == nil && clientCh == nil {
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
		case p, ok := <-peerTx2PCh:
			if !ok {
				peerTx2PCh = nil
				if peerP2PCh == nil && peerTx2PCh == nil && clientCh == nil {
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
				if lCh != nil {
					p.LeaderCh <- lCh
				}
			case peersTx2P[p.ID].LeaderCh == p.LeaderCh:
				delete(peersTx2P, p.ID)
				close(p.LeaderCh)
			}
		case c, ok := <-clientCh:
			if !ok {
				clientCh = nil
				if peerP2PCh == nil && peerTx2PCh == nil && clientCh == nil {
					return errors.WithStack(ctx.Err())
				}
				continue
			}
			if _, exists := clients[c.CommitCh]; exists {
				close(c.CommitCh)
				close(c.LeaderCh)
				delete(clients, c.CommitCh)
			} else {
				if commitInfo.NextLogIndex > 0 {
					c.CommitCh <- commitInfo
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
	if err := c.SendProton(&wire.Hello{
		ServerID: g.config.ServerID,
	}, g.p2pMarshaller); err != nil {
		return err
	}

	m, err := c.ReceiveProton(g.p2pMarshaller)
	if err != nil {
		return err
	}

	h, ok := m.(*wire.Hello)
	if !ok {
		return errors.New("expected hello, got sth else")
	}

	if g.tx2PChs[h.ServerID] == nil {
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

	ch := make(chan []any, queueCapacity)
	var sendCh <-chan []any = ch

	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("receive", parallel.Fail, func(ctx context.Context) error {
			p := peerP2P{
				ID:        h.ServerID,
				SendCh:    ch,
				Connected: true,
			}
			peerCh <- p
			defer func() {
				p.Connected = false
				peerCh <- p
			}()

			cmdCh <- rafttypes.Command{
				PeerID: h.ServerID,
			}

			for {
				m, err := c.ReceiveProton(g.p2pMarshaller)
				if err != nil {
					return err
				}

				switch m.(type) {
				case *wire.Hello:
					return errors.New("unexpected hello")
				default:
					cmdCh <- rafttypes.Command{
						PeerID: h.ServerID,
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
	ctx context.Context,
	expectedPeerID types.ServerID,
	peerCh chan<- peerTx2P,
	cmdCh chan<- rafttypes.Command,
	c *resonance.Connection,
) error {
	meCh := g.tx2PChs[g.config.ServerID]

	if err := c.SendProton(&wire.Hello{
		ServerID: g.config.ServerID,
	}, g.tx2pMarshaller); err != nil {
		return err
	}

	m, err := c.ReceiveProton(g.tx2pMarshaller)
	if err != nil {
		return err
	}

	h, ok := m.(*wire.Hello)
	if !ok {
		return errors.New("expected hello, got sth else")
	}

	txCh := g.tx2PChs[h.ServerID]
	if txCh == nil {
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

	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("receive", parallel.Fail, func(ctx context.Context) error {
			ch := make(chan chan<- []byte, 1)
			var leaderCh <-chan chan<- []byte = ch
			p := peerTx2P{
				ID:        h.ServerID,
				LeaderCh:  ch,
				Connected: true,
			}
			peerCh <- p
			defer func() {
				p.Connected = false
				peerCh <- p
			}()

			var lCh chan<- []byte
			for {
				tx, err := c.ReceiveBytes()
				if err != nil {
					return err
				}

				select {
				case lCh = <-leaderCh:
				default:
				}

				if lCh == meCh {
					cmdCh <- rafttypes.Command{
						Cmd: &rafttypes.ClientRequest{
							Data: tx,
						},
					}
					continue
				}
			}
		})
		spawn("send", parallel.Fail, func(ctx context.Context) error {
			for {
				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case tx := <-txCh:
					if err := c.SendBytes(tx); err != nil {
						return errors.WithStack(err)
					}
				}
			}
		})

		return nil
	})
}

func (g *gossip) c2pHandler(
	ctx context.Context,
	clientCh chan<- client,
	cmdCh chan<- rafttypes.Command,
	c *resonance.Connection,
) error {
	meCh := g.tx2PChs[g.config.ServerID]

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
			ch2 := make(chan chan<- []byte, 1)
			var leaderCh <-chan chan<- []byte = ch2
			cl := client{
				CommitCh: ch,
				LeaderCh: ch2,
			}
			clientCh <- cl
			defer func() {
				clientCh <- cl
			}()

			var lCh chan<- []byte
			for {
				tx, err := c.ReceiveBytes()
				if err != nil {
					return err
				}

				select {
				case lCh = <-leaderCh:
				default:
				}

				if lCh == meCh {
					cmdCh <- rafttypes.Command{
						Cmd: &rafttypes.ClientRequest{
							Data: tx,
						},
					}
					continue
				}

			loop:
				for range 2 {
					select {
					case <-ctx.Done():
						return errors.WithStack(ctx.Err())
					case <-g.passthroughTimeoutTicker.C:
					case lCh = <-leaderCh:
						if lCh == meCh {
							cmdCh <- rafttypes.Command{
								Cmd: &rafttypes.ClientRequest{
									Data: tx,
								},
							}
							break loop
						}
						if lCh != nil {
							select {
							case <-ctx.Done():
								return errors.WithStack(ctx.Err())
							case <-g.passthroughTimeoutTicker.C:
							case lCh <- tx:
								break loop
							}
						}
					case lCh <- tx:
						break loop
					}
				}
			}
		})
		spawn("send", parallel.Fail, func(ctx context.Context) error {
			defer c.Close()

			nextLogIndex := msgCommitInfo.NextLogIndex
			var logF *os.File
			for newCommitInfo := range commitCh {
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

func initConnection(serverID, peerID types.ServerID) bool {
	return bytes.Compare(serverID[:], peerID[:]) >= 0
}
