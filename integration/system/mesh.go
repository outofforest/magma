package system

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/outofforest/logger"
	"github.com/outofforest/magma/gossip/wire"
	"github.com/outofforest/magma/gossip/wire/hello"
	"github.com/outofforest/magma/gossip/wire/p2p"
	"github.com/outofforest/magma/raft/types"
	"github.com/outofforest/parallel"
	"github.com/outofforest/resonance"
)

type link struct {
	SrcPeer *Peer
	DstPeer *Peer
}

// pair stores information about connected pair of peers.
type pair struct {
	SrcListener net.Listener
	DstListener net.Listener

	Mu    sync.Mutex
	Group *parallel.Group
}

// newMesh creates new mesh.
func newMesh(t *testing.T, group *parallel.Group) *mesh {
	return &mesh{
		requireT:  require.New(t),
		listeners: map[*Peer]net.Listener{},
		links:     map[link]*pair{},
		group:     group,
		mHello:    hello.NewMarshaller(),
		mP2P:      p2p.NewMarshaller(),
	}
}

// mesh maintains connection mesh between peers.
type mesh struct {
	requireT  *require.Assertions
	listeners map[*Peer]net.Listener
	links     map[link]*pair
	group     *parallel.Group
	mHello    hello.Marshaller
	mP2P      p2p.Marshaller

	mu           sync.RWMutex
	forcedLeader *Peer
}

// Listener returns listener for peer.
func (m *mesh) Listener(peer *Peer) net.Listener {
	l, exists := m.listeners[peer]
	if !exists {
		var err error
		l, err = net.Listen("tcp", "localhost:0")
		m.requireT.NoError(err)
		m.listeners[peer] = l
	}
	return l
}

// Pair returns a pair of connected endpoints.
func (m *mesh) Pair(srcPeer, dstPeer *Peer) *pair {
	lnk := link{SrcPeer: srcPeer, DstPeer: dstPeer}
	p, exists := m.links[lnk]
	if !exists {
		dstL := m.Listener(dstPeer)
		srcL, err := net.Listen("tcp", "localhost:0")
		m.requireT.NoError(err)
		p = &pair{
			SrcListener: srcL,
			DstListener: dstL,
			Group:       parallel.NewSubgroup(m.group.Spawn, "clients", parallel.Continue),
		}
		m.links[lnk] = p

		m.startForwarder(lnk, p)
	}

	return p
}

// ForceLeader sets the peer which should be a leader after next voting.
func (m *mesh) ForceLeader(peer *Peer) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.forcedLeader = peer
}

// EnableLink enables pair connection.
func (m *mesh) EnableLink(srcPeer, dstPeer *Peer) {
	pair := m.Pair(srcPeer, dstPeer)

	pair.Mu.Lock()
	defer pair.Mu.Unlock()

	if pair.Group != nil {
		return
	}

	pair.Group = parallel.NewSubgroup(m.group.Spawn, "clients", parallel.Continue)
}

// DisableLink disables pair connection.
func (m *mesh) DisableLink(srcPeer, dstPeer *Peer) {
	pair := m.Pair(srcPeer, dstPeer)

	pair.Mu.Lock()
	defer pair.Mu.Unlock()

	if pair.Group == nil {
		return
	}

	pair.Group.Exit(nil)
	m.requireT.NoError(pair.Group.Wait())
	pair.Group = nil
}

func (m *mesh) startForwarder(lnk link, pair *pair) {
	m.group.Spawn("forwarder", parallel.Fail, func(ctx context.Context) error {
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
					m.handleConn(conn, lnk, pair)
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
	})
}

func (m *mesh) handleConn(conn net.Conn, lnk link, pair *pair) {
	pair.Mu.Lock()
	defer pair.Mu.Unlock()

	if pair.Group == nil {
		_ = conn.Close()
		return
	}

	pair.Group.Spawn("conn", parallel.Continue, func(ctx context.Context) error {
		_ = parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
			conn2, err := net.Dial("tcp", pair.DstListener.Addr().String())
			if err != nil {
				return err
			}

			spawn("handshake", parallel.Continue, func(ctx context.Context) error {
				config := resonance.Config{
					MaxMessageSize: maxMsgSize,
				}
				c1 := resonance.NewConnection(conn, config)
				c2 := resonance.NewConnection(conn2, config)

				helloMsg1, err := m.interceptHello(c2, c1)
				if err != nil {
					return err
				}

				helloMsg2, err := m.interceptHello(c1, c2)
				if err != nil {
					return err
				}

				isP2PChannel := helloMsg1.Channel == wire.ChannelP2P || helloMsg2.Channel == wire.ChannelP2P

				spawn("copy1", parallel.Exit, func(ctx context.Context) error {
					if !isP2PChannel {
						_, err := io.Copy(conn2, conn)
						return err
					}
					return m.interceptVoteRequests(c2, c1, lnk.SrcPeer)
				})
				spawn("copy2", parallel.Exit, func(ctx context.Context) error {
					if !isP2PChannel {
						_, err := io.Copy(conn, conn2)
						return err
					}
					return m.interceptVoteRequests(c1, c2, lnk.DstPeer)
				})

				return nil
			})
			spawn("watchdog", parallel.Fail, func(ctx context.Context) error {
				defer conn.Close()
				defer conn2.Close()

				<-ctx.Done()
				return errors.WithStack(ctx.Err())
			})
			return nil
		})

		return nil
	})
}

func (m *mesh) interceptVoteRequests(dstC, srcC *resonance.Connection, srcPeer *Peer) error {
	for {
		msg, err := srcC.ReceiveProton(m.mP2P)
		if err != nil {
			return err
		}

		if _, ok := msg.(*types.VoteRequest); ok && m.isVoteRequestBlocked(srcPeer) {
			continue
		}

		if err := dstC.SendProton(msg, m.mP2P); err != nil {
			return err
		}
	}
}

func (m *mesh) isVoteRequestBlocked(srcPeer *Peer) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.forcedLeader != nil && m.forcedLeader != srcPeer
}

func (m *mesh) interceptHello(dstC, srcC *resonance.Connection) (*wire.Hello, error) {
	msg, err := srcC.ReceiveProton(m.mHello)
	if err != nil {
		return nil, err
	}

	helloMsg, ok := msg.(*wire.Hello)
	if !ok {
		return nil, errors.Errorf("hello expected, got: %T", msg)
	}

	if err := dstC.SendProton(helloMsg, m.mHello); err != nil {
		return nil, err
	}

	return helloMsg, nil
}
