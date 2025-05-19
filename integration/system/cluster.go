package system

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/outofforest/logger"
	"github.com/outofforest/magma"
	"github.com/outofforest/magma/client"
	"github.com/outofforest/magma/integration/entities"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/memdb"
	"github.com/outofforest/parallel"
)

// NewPeer creates new peer.
func NewPeer(t *testing.T, peerID types.ServerID, partitions types.Partitions) *Peer {
	requireT := require.New(t)

	c2pListener, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)

	p := &Peer{
		id:          peerID,
		dir:         t.TempDir(),
		c2pListener: c2pListener.(*net.TCPListener),
		partitions:  partitions,
		requireT:    requireT,
	}
	t.Cleanup(p.close)
	return p
}

// Peer is used to run peers.
type Peer struct {
	id          types.ServerID
	partitions  types.Partitions
	dir         string
	c2pListener *net.TCPListener
	requireT    *require.Assertions
	group       *parallel.Group
}

// DropData drops data directory of the peer.
func (p *Peer) DropData() {
	if err := os.RemoveAll(p.dir); err != nil && !errors.Is(err, os.ErrNotExist) {
		p.requireT.NoError(err)
	}
}

func (p *Peer) start(group *parallel.Group, config types.Config, p2pListener net.Listener) {
	if p.group != nil {
		p.requireT.Fail("peer is already running")
	}

	p.group = parallel.NewSubgroup(group.Spawn, string(p.id), parallel.Continue)
	p.group.Spawn("peer", parallel.Exit, func(ctx context.Context) error {
		err := magma.Run(ctx, config, p2pListener, p.c2pListener, p.dir, uint64(os.Getpagesize()))
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			logger.Get(ctx).Error("Peer failed", zap.Error(err))
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		return nil
	})
}

func (p *Peer) stop() {
	if p.group != nil {
		p.group.Exit(nil)
		p.requireT.NoError(p.group.Wait())
		p.group = nil
	}
}

func (p *Peer) close() {
	_ = p.c2pListener.Close()
}

// NewClient returns new client.
func NewClient(
	t *testing.T,
	peer *Peer,
	name string,
	partitionID types.PartitionID,
	triggerFunc func(context.Context, *client.View) error,
	indices ...memdb.Index,
) *Client {
	requireT := require.New(t)

	c, err := client.New(client.Config{
		Service:          name,
		PeerAddress:      peer.c2pListener.Addr().String(),
		PartitionID:      partitionID,
		MaxMessageSize:   maxMsgSize,
		BroadcastTimeout: time.Second,
		AwaitTimeout:     5 * time.Second,
		Marshaller:       entities.NewMarshaller(),
		Indices:          indices,
		TriggerFunc:      triggerFunc,
	})
	requireT.NoError(err)

	return &Client{
		name:     name,
		client:   c,
		requireT: requireT,
	}
}

// Client is used tor un clients.
type Client struct {
	name     string
	client   *client.Client
	requireT *require.Assertions
	group    *parallel.Group
}

// View returns current db view.
func (c *Client) View() *client.View {
	return c.client.View()
}

// NewTransactor returns new transactor.
func (c *Client) NewTransactor() *client.Transactor {
	return c.client.NewTransactor()
}

func (c *Client) start(group *parallel.Group) {
	if c.group != nil {
		c.requireT.Fail("client is already running")
	}

	c.group = parallel.NewSubgroup(group.Spawn, c.name, parallel.Continue)
	c.group.Spawn("client", parallel.Exit, func(ctx context.Context) error {
		err := c.client.Run(ctx)
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			logger.Get(ctx).Error("Client failed", zap.Error(err))
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		return nil
	})
}

func (c *Client) warmUp(ctx context.Context) {
	if c.group == nil {
		c.requireT.Fail("client is not running")
	}

	if err := c.client.WarmUp(ctx); err != nil && !errors.Is(err, context.Canceled) {
		c.requireT.NoError(err)
	}
}

func (c *Client) stop() {
	if c.group != nil {
		c.group.Exit(nil)
		c.requireT.NoError(c.group.Wait())
	}
}

const maxMsgSize = 4 * 1024

// NewCluster creates new cluster.
func NewCluster(t *testing.T, peers ...*Peer) (Cluster, context.Context) {
	group := parallel.NewGroup(newContext(t))
	c := Cluster{
		group: group,
		mesh:  newMesh(t, parallel.NewSubgroup(group.Spawn, "mesh", parallel.Fail)),
		peers: peers,
	}
	t.Cleanup(func() {
		group.Exit(nil)
		if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			require.NoError(t, err)
		}
	})
	return c, group.Context()
}

// Cluster runs peers and clients.
type Cluster struct {
	group *parallel.Group
	mesh  *mesh
	peers []*Peer
}

// StartPeers starts peers.
func (c Cluster) StartPeers(peers ...*Peer) {
	for _, p := range peers {
		p.start(c.group, c.newConfig(p), c.mesh.Listener(p))
	}
}

// StopPeers stops peers.
func (c Cluster) StopPeers(peers ...*Peer) {
	for _, p := range peers {
		p.stop()
	}
}

// StartClients starts clients.
func (c Cluster) StartClients(clients ...*Client) {
	for _, cl := range clients {
		cl.start(c.group)
	}
	for _, cl := range clients {
		cl.warmUp(c.group.Context())
	}
}

// StopClients stops clients.
func (c Cluster) StopClients(clients ...*Client) {
	for _, c := range clients {
		c.stop()
	}
}

// ForceLeader sets the peer which should be a leader after next voting.
func (c Cluster) ForceLeader(peer *Peer) {
	c.mesh.ForceLeader(peer)
}

// EnableLink enables link between peers.
func (c Cluster) EnableLink(peer1, peer2 *Peer) {
	c.mesh.EnableLink(peer1, peer2)
	c.mesh.EnableLink(peer2, peer1)
}

// DisableLink disables link between peers.
func (c Cluster) DisableLink(peer1, peer2 *Peer) {
	c.mesh.DisableLink(peer1, peer2)
	c.mesh.DisableLink(peer2, peer1)
}

func (c Cluster) newConfig(peer *Peer) types.Config {
	config := types.Config{
		ServerID:       peer.id,
		MaxMessageSize: maxMsgSize,
	}

	for _, p := range c.peers {
		var p2pAddress string
		if p != peer {
			p2pAddress = c.mesh.Pair(peer, p).SrcListener.Addr().String()
		}

		config.Servers = append(config.Servers, types.ServerConfig{
			ID:         p.id,
			P2PAddress: p2pAddress,
			Partitions: p.partitions,
		})
	}

	return config
}
