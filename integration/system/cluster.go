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

	p2pListener, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)

	c2pListener, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)

	p := &Peer{
		id:          peerID,
		dir:         t.TempDir(),
		p2pListener: p2pListener.(*net.TCPListener),
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
	p2pListener *net.TCPListener
	c2pListener *net.TCPListener
	requireT    *require.Assertions
	group       *parallel.Group
}

func (p *Peer) start(ctx context.Context, config types.Config) {
	if p.group != nil {
		p.requireT.Fail("peer is already running")
	}

	config.ServerID = p.id

	p.group = parallel.NewGroup(ctx)
	p.group.Spawn(string(p.id), parallel.Fail, func(ctx context.Context) error {
		return magma.Run(ctx, config, p.p2pListener, p.c2pListener, p.dir, uint64(os.Getpagesize()))
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

	_ = p.p2pListener.Close()
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

func (c *Client) start(ctx context.Context) {
	if c.group != nil {
		c.requireT.Fail("client is already running")
	}

	c.group = parallel.NewGroup(ctx)
	c.group.Spawn(c.name, parallel.Fail, c.client.Run)
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
		c.group = nil
	}
}

const maxMsgSize = 1024 * 1024

// NewCluster creates new cluster.
func NewCluster(peers ...*Peer) Cluster {
	config := types.Config{
		MaxMessageSize: maxMsgSize,
	}

	for _, p := range peers {
		config.Servers = append(config.Servers, types.ServerConfig{
			ID:         p.id,
			P2PAddress: p.p2pListener.Addr().String(),
			Partitions: p.partitions,
		})
	}

	return Cluster{
		config: config,
	}
}

// Cluster runs peers and clients.
type Cluster struct {
	config types.Config
}

// StartPeers starts peers.
func (c Cluster) StartPeers(ctx context.Context, peers ...*Peer) {
	for _, p := range peers {
		p.start(ctx, c.config)
	}
}

// StopPeers stops peers.
func (c Cluster) StopPeers(peers ...*Peer) {
	for _, p := range peers {
		p.stop()
	}
}

// StartClients starts clients.
func (c Cluster) StartClients(ctx context.Context, clients ...*Client) {
	for _, c := range clients {
		c.start(ctx)
	}
}

// StopClients stops clients.
func (c Cluster) StopClients(clients ...*Client) {
	for _, c := range clients {
		c.stop()
	}
}
