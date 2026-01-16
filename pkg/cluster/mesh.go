package cluster

import (
	"context"
	"net"
	"sync"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/outofforest/logger"
	"github.com/outofforest/magma/gossip/wire"
	"github.com/outofforest/magma/gossip/wire/hello"
	"github.com/outofforest/magma/gossip/wire/p2p"
	"github.com/outofforest/magma/raft/types"
	"github.com/outofforest/parallel"
	"github.com/outofforest/resonance"
	"github.com/outofforest/varuint64"
)

// Link stores peers connected by link.
type Link struct {
	SrcPeer *Peer
	DstPeer *Peer
}

// Pair stores information about connected pair of peers.
type Pair struct {
	Link        Link
	SrcListener net.Listener
	DstListener net.Listener
}

// newMesh creates new mesh.
func newMesh() *mesh {
	return &mesh{
		listeners: map[*Peer]net.Listener{},
		links:     map[Link]*Pair{},
		mHello:    hello.NewMarshaller(),
		mP2P:      p2p.NewMarshaller(),
		ch:        make(chan any),
	}
}

// mesh maintains connection mesh between peers.
type mesh struct {
	mHello hello.Marshaller
	mP2P   p2p.Marshaller

	mu           sync.RWMutex
	listeners    map[*Peer]net.Listener
	links        map[Link]*Pair
	forcedLeader *Peer

	ch chan any
}

type startPair struct {
	Pair *Pair
}

type enablePair struct {
	Pair *Pair
	Done chan struct{}
}

type disablePair struct {
	Pair *Pair
	Done chan struct{}
}

type connection struct {
	Conn net.Conn
	Pair *Pair
}

// Listener returns listener for peer.
func (m *mesh) Listener(peer *Peer) (net.Listener, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.listener(peer)
}

// Pair returns a pair of connected endpoints.
func (m *mesh) Pair(ctx context.Context, srcPeer, dstPeer *Peer) (*Pair, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	lnk := Link{SrcPeer: srcPeer, DstPeer: dstPeer}
	p, exists := m.links[lnk]
	if !exists {
		dstL, err := m.listener(dstPeer)
		if err != nil {
			return nil, err
		}
		srcL, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			return nil, errors.WithStack(err)
		}

		p = &Pair{
			Link:        lnk,
			SrcListener: srcL,
			DstListener: dstL,
		}
		m.links[lnk] = p

		cmd := startPair{
			Pair: p,
		}
		select {
		case <-ctx.Done():
		case m.ch <- cmd:
		}
	}

	return p, nil
}

// ForceLeader sets the peer which should be a leader after next voting.
func (m *mesh) ForceLeader(peer *Peer) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.forcedLeader = peer
}

// EnableLink enables pair connection.
func (m *mesh) EnableLink(ctx context.Context, srcPeer, dstPeer *Peer) error {
	pair, err := m.Pair(ctx, srcPeer, dstPeer)
	if err != nil {
		return err
	}

	cmd := enablePair{
		Pair: pair,
		Done: make(chan struct{}),
	}

	select {
	case <-ctx.Done():
	case m.ch <- cmd:
		select {
		case <-ctx.Done():
		case <-cmd.Done:
		}
	}

	return nil
}

// DisableLink disables pair connection.
func (m *mesh) DisableLink(ctx context.Context, srcPeer, dstPeer *Peer) error {
	pair, err := m.Pair(ctx, srcPeer, dstPeer)
	if err != nil {
		return err
	}

	cmd := disablePair{
		Pair: pair,
		Done: make(chan struct{}),
	}

	select {
	case <-ctx.Done():
	case m.ch <- cmd:
		select {
		case <-ctx.Done():

		case <-cmd.Done:
		}
	}

	return nil
}

func (m *mesh) run(ctx context.Context) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("supervisor", parallel.Fail, func(ctx context.Context) error {
			runningPairs := map[*Pair]*parallel.Group{}

			for {
				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case cmd := <-m.ch:
					switch cmd := cmd.(type) {
					case connection:
						group := runningPairs[cmd.Pair]
						if group == nil {
							continue
						}
						group.Spawn("conn", parallel.Continue, func(ctx context.Context) error {
							return m.runConn(ctx, cmd.Conn, cmd.Pair)
						})
					case startPair:
						if _, exists := runningPairs[cmd.Pair]; exists {
							continue
						}
						group := parallel.NewSubgroup(spawn, "connections", parallel.Continue)
						runningPairs[cmd.Pair] = group

						spawn("pair", parallel.Continue, func(ctx context.Context) error {
							return m.runForwarder(ctx, cmd.Pair)
						})
					case enablePair:
						func() {
							defer close(cmd.Done)

							group, exists := runningPairs[cmd.Pair]
							if !exists || group != nil {
								return
							}

							runningPairs[cmd.Pair] = parallel.NewSubgroup(spawn, "connections", parallel.Continue)
						}()
					case disablePair:
						err := func() error {
							defer close(cmd.Done)

							group, exists := runningPairs[cmd.Pair]

							if !exists || group == nil {
								return nil
							}

							group.Exit(nil)
							if err := group.Wait(); err != nil {
								return err
							}
							runningPairs[cmd.Pair] = nil
							return nil
						}()
						if err != nil {
							return err
						}
					}
				}
			}
		})

		return nil
	})
}

func (m *mesh) runForwarder(ctx context.Context, pair *Pair) error {
	err := parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("listener", parallel.Fail, func(ctx context.Context) error {
			for {
				conn, err := pair.SrcListener.Accept()
				if ctx.Err() != nil {
					if err == nil {
						_ = conn.Close()
					}
					return errors.WithStack(ctx.Err())
				}
				if err != nil {
					return err
				}

				select {
				case <-ctx.Done():
					return errors.WithStack(err)
				case m.ch <- connection{
					Conn: conn,
					Pair: pair,
				}:
				}
			}
		})
		spawn("watchdog", parallel.Fail, func(ctx context.Context) error {
			defer pair.SrcListener.Close()

			<-ctx.Done()
			return errors.WithStack(ctx.Err())
		})

		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		logger.Get(ctx).Error("Listener failed", zap.Error(err))
	}
	return err
}

func (m *mesh) runConn(ctx context.Context, conn net.Conn, pair *Pair) error {
	_ = parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		conn2, err := net.Dial("tcp", pair.DstListener.Addr().String())
		if err != nil {
			_ = conn.Close()
			return err
		}

		spawn("watchdog", parallel.Fail, func(ctx context.Context) error {
			defer conn.Close()
			defer conn2.Close()

			<-ctx.Done()
			return errors.WithStack(ctx.Err())
		})

		config := resonance.Config{
			MaxMessageSize: MaxMsgSize,
		}
		c1 := resonance.NewConnection(conn, config)
		c2 := resonance.NewConnection(conn2, config)

		spawn("c1", parallel.Fail, c1.Run)
		spawn("c2", parallel.Fail, c2.Run)

		c1.BufferReads()
		c1.BufferWrites()
		c2.BufferReads()
		c2.BufferWrites()

		helloMsg1, err := m.interceptHello(c2, c1)
		if err != nil {
			return err
		}

		helloMsg2, err := m.interceptHello(c1, c2)
		if err != nil {
			return err
		}

		channel := helloMsg1.Channel
		if channel == wire.ChannelNone {
			channel = helloMsg2.Channel
		}

		spawn("copy1", parallel.Exit, func(ctx context.Context) error {
			return m.interceptChannel(channel, c2, c1, pair.Link.SrcPeer)
		})
		spawn("copy2", parallel.Exit, func(ctx context.Context) error {
			return m.interceptChannel(channel, c1, c2, pair.Link.DstPeer)
		})

		return nil
	})
	return nil
}

func (m *mesh) interceptChannel(channel wire.Channel, dstC, srcC *resonance.Connection, srcPeer *Peer) error {
	for {
		msg, _, err := srcC.ReceiveRawBytes()
		if err != nil {
			return err
		}

		//nolint:nestif
		if channel == wire.ChannelP2P {
			_, n := varuint64.Parse(msg)
			msgID, n2 := varuint64.Parse(msg[n:])

			p2pMsg, _, err := m.mP2P.Unmarshal(msgID, msg[n+n2:])
			if err != nil {
				return err
			}

			if v, ok := p2pMsg.(*types.VoteRequest); ok && m.forcedLeader != nil {
				if m.forcedLeader == srcPeer {
					if _, err := srcC.SendProton(&types.VoteResponse{
						Term:        v.Term,
						VoteGranted: true,
					}, m.mP2P); err != nil {
						return err
					}
				}
				continue
			}
		}

		if _, err := dstC.SendRawBytes(msg); err != nil {
			return err
		}
	}
}

func (m *mesh) interceptHello(dstC, srcC *resonance.Connection) (*wire.Hello, error) {
	msg, _, err := srcC.ReceiveProton(m.mHello)
	if err != nil {
		return nil, err
	}

	helloMsg, ok := msg.(*wire.Hello)
	if !ok {
		return nil, errors.Errorf("hello expected, got: %T", msg)
	}

	if _, err := dstC.SendProton(helloMsg, m.mHello); err != nil {
		return nil, err
	}

	return helloMsg, nil
}

func (m *mesh) listener(peer *Peer) (net.Listener, error) {
	l, exists := m.listeners[peer]
	if !exists {
		var err error
		l, err = net.Listen("tcp", "localhost:0")
		if err != nil {
			return nil, errors.WithStack(err)
		}
		m.listeners[peer] = l
	}
	return l, nil
}
