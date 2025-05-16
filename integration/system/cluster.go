package system

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma"
	"github.com/outofforest/magma/client"
	"github.com/outofforest/magma/client/indices"
	"github.com/outofforest/magma/integration/entities"
	"github.com/outofforest/magma/types"
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

func (p *Peer) start(ctx context.Context, config types.Config, p2pListener net.Listener) {
	if p.group != nil {
		p.requireT.Fail("peer is already running")
	}

	p.group = parallel.NewGroup(ctx)
	p.group.Spawn(string(p.id), parallel.Fail, func(ctx context.Context) error {
		return magma.Run(ctx, config, p2pListener, p.c2pListener, p.dir, uint64(os.Getpagesize()))
	})
}

func (p *Peer) stop() {
	if p.group != nil {
		p.group.Exit(nil)
		if err := p.group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			p.requireT.NoError(err)
		}
		p.group = nil
	}
}

func (p *Peer) close() {
	p.stop()

	_ = p.c2pListener.Close()
}

// NewClient returns new client.
func NewClient(
	t *testing.T,
	peer *Peer,
	name string,
	partitionID types.PartitionID,
	triggerFunc func(context.Context, *client.View) error,
	indices ...indices.Index,
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

	cl := &Client{
		name:     name,
		client:   c,
		requireT: requireT,
	}
	t.Cleanup(cl.stop)
	return cl
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

func (c *Client) start(ctx context.Context) {
	if c.group != nil {
		c.requireT.Fail("client is already running")
	}

	c.group = parallel.NewGroup(ctx)
	c.group.Spawn(c.name, parallel.Fail, c.client.Run)
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
		if err := c.group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			c.requireT.NoError(err)
		}
	}
}

const maxMsgSize = 4 * 1024

// NewCluster creates new cluster.
func NewCluster(ctx context.Context, t *testing.T, peers ...*Peer) Cluster {
	return Cluster{
		ctx:   ctx,
		mesh:  NewMesh(ctx, t),
		peers: peers,
	}
}

// Cluster runs peers and clients.
type Cluster struct {
	ctx   context.Context //nolint:containedctx
	mesh  *Mesh
	peers []*Peer
}

// StartPeers starts peers.
func (c Cluster) StartPeers(peers ...*Peer) {
	for _, p := range peers {
		p.start(c.ctx, c.newConfig(p), c.mesh.Listener(p))
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
		cl.start(c.ctx)
	}
	for _, cl := range clients {
		cl.warmUp(c.ctx)
	}
}

// StopClients stops clients.
func (c Cluster) StopClients(clients ...*Client) {
	for _, c := range clients {
		c.stop()
	}
}

// EnableLink enables link between peers.
func (c Cluster) EnableLink(peer1, peer2 *Peer) {
	c.mesh.Enable(peer1, peer2)
	c.mesh.Enable(peer2, peer1)
}

// DisableLink disables link between peers.
func (c Cluster) DisableLink(peer1, peer2 *Peer) {
	c.mesh.Disable(peer1, peer2)
	c.mesh.Disable(peer2, peer1)
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
