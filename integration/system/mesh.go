package system

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/parallel"
)

type link struct {
	SrcPeer *Peer
	DstPeer *Peer
}

// Pair stores information about connected pair of peers.
type Pair struct {
	SrcListener net.Listener
	DstListener net.Listener

	Mu    sync.Mutex
	Group *parallel.Group
}

// NewMesh creates new mesh.
func NewMesh(ctx context.Context, t *testing.T) *Mesh {
	m := &Mesh{
		requireT:  require.New(t),
		listeners: map[*Peer]net.Listener{},
		links:     map[link]*Pair{},
		group:     parallel.NewGroup(ctx),
	}
	t.Cleanup(m.close)
	return m
}

// Mesh maintains connection mesh between peers.
type Mesh struct {
	requireT  *require.Assertions
	listeners map[*Peer]net.Listener
	links     map[link]*Pair
	group     *parallel.Group
}

// Listener returns listener for peer.
func (m *Mesh) Listener(peer *Peer) net.Listener {
	l, exists := m.listeners[peer]
	if !exists {
		var err error
		l, err = net.Listen("tcp", ":0")
		m.requireT.NoError(err)
		m.listeners[peer] = l
	}
	return l
}

// Pair returns a pair of connected endpoints.
func (m *Mesh) Pair(srcPeer, dstPeer *Peer) *Pair {
	lnk := link{SrcPeer: srcPeer, DstPeer: dstPeer}
	pair, exists := m.links[lnk]
	if !exists {
		dstL := m.Listener(dstPeer)
		srcL, err := net.Listen("tcp", ":0")
		m.requireT.NoError(err)
		pair = &Pair{
			SrcListener: srcL,
			DstListener: dstL,
			Group:       parallel.NewSubgroup(m.group.Spawn, "clients", parallel.Continue),
		}
		m.links[lnk] = pair

		m.startForwarder(pair)
	}

	return pair
}

// Enable enables pair connection.
func (m *Mesh) Enable(srcPeer, dstPeer *Peer) {
	pair := m.Pair(srcPeer, dstPeer)

	pair.Mu.Lock()
	defer pair.Mu.Unlock()

	if pair.Group != nil {
		return
	}

	pair.Group = parallel.NewSubgroup(m.group.Spawn, "clients", parallel.Continue)
}

// Disable disables pair connection.
func (m *Mesh) Disable(srcPeer, dstPeer *Peer) {
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

func (m *Mesh) startForwarder(pair *Pair) {
	m.group.Spawn("forwarder", parallel.Fail, func(ctx context.Context) error {
		return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
			spawn("listener", parallel.Fail, func(ctx context.Context) error {
				for {
					conn, err := pair.SrcListener.Accept()
					if err != nil {
						if ctx.Err() != nil {
							return errors.WithStack(ctx.Err())
						}
						continue
					}
					m.handleConn(conn, pair)
				}
			})
			spawn("watchdog", parallel.Fail, func(ctx context.Context) error {
				defer pair.SrcListener.Close()

				<-ctx.Done()
				return errors.WithStack(ctx.Err())
			})

			return nil
		})
	})
}

func (m *Mesh) handleConn(conn net.Conn, pair *Pair) {
	pair.Mu.Lock()
	defer pair.Mu.Unlock()

	if pair.Group == nil {
		_ = conn.Close()
		return
	}

	pair.Group.Spawn("conn", parallel.Continue, func(ctx context.Context) error {
		conn2, err := net.Dial("tcp", pair.DstListener.Addr().String())
		if err != nil {
			return nil //nolint:nilerr
		}

		return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
			spawn("copy1", parallel.Exit, func(ctx context.Context) error {
				_, _ = io.Copy(conn2, conn)
				return nil
			})
			spawn("copy2", parallel.Exit, func(ctx context.Context) error {
				_, _ = io.Copy(conn, conn2)
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
	})
}

func (m *Mesh) close() {
	m.group.Exit(nil)
	if err := m.group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		m.requireT.NoError(err)
	}
}
