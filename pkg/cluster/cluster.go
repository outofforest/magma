package cluster

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/outofforest/logger"
	"github.com/outofforest/magma"
	"github.com/outofforest/magma/client"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/memdb"
	"github.com/outofforest/parallel"
	"github.com/outofforest/proton"
)

// Peer is used to run peers.
type Peer struct {
	id          types.ServerID
	partitions  types.Partitions
	dir         string
	c2pListener *net.TCPListener
}

// ClientListenAddr returns address of client endpoint listener.
func (p *Peer) ClientListenAddr() string {
	return p.c2pListener.Addr().String()
}

// DropData drops data directory of the peer.
func (p *Peer) DropData() error {
	if err := os.RemoveAll(p.dir); err != nil && !errors.Is(err, os.ErrNotExist) {
		return errors.WithStack(err)
	}
	return nil
}

func (p *Peer) run(ctx context.Context, config types.Config, p2pListener net.Listener) error {
	err := magma.Run(ctx, config, p2pListener, p.c2pListener, p.dir, uint64(os.Getpagesize()))
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		logger.Get(ctx).Error("Peer failed", zap.Error(err))
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func (p *Peer) close() {
	_ = p.c2pListener.Close()
}

// Client is used tor un clients.
type Client struct {
	client *client.Client
}

// View returns current db view.
func (c *Client) View() *client.View {
	return c.client.View()
}

// NewTransactor returns new transactor.
func (c *Client) NewTransactor() client.Transactor {
	return c.client.NewTransactor()
}

func (c *Client) run(ctx context.Context) error {
	err := c.client.Run(ctx)
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		logger.Get(ctx).Error("Client failed", zap.Error(err))
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func (c *Client) warmUp(ctx context.Context) error {
	return c.client.WarmUp(ctx)
}

const (
	maxMsgSize        = 4 * 1024
	maxUncommittedLog = 2048
)

// New creates new cluster.
func New(dir string) *Cluster {
	return &Cluster{
		dir:  dir,
		mesh: newMesh(),
		ch:   make(chan any),
	}
}

// Cluster runs peers and clients.
type Cluster struct {
	dir   string
	mesh  *mesh
	peers []*Peer

	ch chan any
}

type startPeers struct {
	Peers []*Peer
	Done  chan struct{}
}

type stopPeers struct {
	Peers []*Peer
	Done  chan struct{}
}

type startClients struct {
	Clients []*Client
	Done    chan struct{}
}

type stopClients struct {
	Clients []*Client
	Done    chan struct{}
}

// Run runs cluster.
func (c *Cluster) Run(ctx context.Context) error {
	defer func() {
		for _, peer := range c.peers {
			peer.close()
		}
	}()

	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("mesh", parallel.Fail, c.mesh.run)
		spawn("supervisor", parallel.Fail, func(ctx context.Context) error {
			runningPeers := map[*Peer]*parallel.Group{}
			runningClients := map[*Client]*parallel.Group{}

			for {
				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case cmd := <-c.ch:
					switch cmd := cmd.(type) {
					case startPeers:
						func() {
							defer close(cmd.Done)

							for _, p := range cmd.Peers {
								if runningPeers[p] != nil {
									continue
								}
								group := parallel.NewSubgroup(spawn, "peer", parallel.Continue)
								runningPeers[p] = group
								group.Spawn("peer", parallel.Continue, func(ctx context.Context) error {
									listener, err := c.mesh.Listener(p)
									if err != nil {
										return err
									}
									config, err := c.newPeerConfig(ctx, p)
									if err != nil {
										return err
									}

									return p.run(ctx, config, listener)
								})
							}
						}()
					case stopPeers:
						err := func() error {
							defer close(cmd.Done)

							for _, p := range cmd.Peers {
								group := runningPeers[p]
								if group == nil {
									continue
								}

								group.Exit(nil)
								if err := group.Wait(); err != nil {
									return err
								}

								delete(runningPeers, p)
							}
							return nil
						}()
						if err != nil {
							return err
						}
					case startClients:
						err := func() error {
							defer close(cmd.Done)

							for _, c := range cmd.Clients {
								if runningClients[c] != nil {
									continue
								}
								group := parallel.NewSubgroup(spawn, "client", parallel.Continue)
								runningClients[c] = group
								group.Spawn("client", parallel.Continue, c.run)
							}
							for _, cl := range cmd.Clients {
								if err := cl.warmUp(ctx); err != nil {
									return err
								}
							}
							return nil
						}()
						if err != nil {
							return err
						}
					case stopClients:
						err := func() error {
							defer close(cmd.Done)

							for _, c := range cmd.Clients {
								group := runningClients[c]
								if group == nil {
									continue
								}

								group.Exit(nil)
								if err := group.Wait(); err != nil {
									return err
								}

								delete(runningClients, c)
							}
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

// NewPeer creates new peer.
func (c *Cluster) NewPeer(peerID types.ServerID, partitions types.Partitions) (*Peer, error) {
	c2pListener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return nil, errors.WithStack(err)
	}

	peer := &Peer{
		id:          peerID,
		dir:         filepath.Join(c.dir, string(peerID)),
		c2pListener: c2pListener.(*net.TCPListener),
		partitions:  partitions,
	}

	c.peers = append(c.peers, peer)

	return peer, nil
}

// NewClient returns new client.
func (c *Cluster) NewClient(
	peer *Peer,
	name string,
	marshaller proton.Marshaller,
	partitionID types.PartitionID,
	triggerFunc func(context.Context, *client.View) error,
	indices ...memdb.Index,
) (*Client, error) {
	cl, err := client.New(client.Config{
		Service:          name,
		PeerAddress:      peer.c2pListener.Addr().String(),
		PartitionID:      partitionID,
		MaxMessageSize:   maxMsgSize,
		BroadcastTimeout: time.Second,
		AwaitTimeout:     5 * time.Second,
		Marshaller:       marshaller,
		Indices:          indices,
		TriggerFunc:      triggerFunc,
	})
	if err != nil {
		return nil, err
	}

	return &Client{
		client: cl,
	}, nil
}

// StartPeers starts peers.
func (c *Cluster) StartPeers(ctx context.Context, peers ...*Peer) {
	cmd := startPeers{
		Peers: peers,
		Done:  make(chan struct{}),
	}
	select {
	case <-ctx.Done():
	case c.ch <- cmd:
		select {
		case <-ctx.Done():
		case <-cmd.Done:
		}
	}
}

// StopPeers stops peers.
func (c *Cluster) StopPeers(ctx context.Context, peers ...*Peer) {
	cmd := stopPeers{
		Peers: peers,
		Done:  make(chan struct{}),
	}
	select {
	case <-ctx.Done():
	case c.ch <- cmd:
		select {
		case <-ctx.Done():
		case <-cmd.Done:
		}
	}
}

// DropData drops data stored by peer.
func (c *Cluster) DropData(p *Peer) error {
	return p.DropData()
}

// StartClients starts clients.
func (c *Cluster) StartClients(ctx context.Context, clients ...*Client) {
	cmd := startClients{
		Clients: clients,
		Done:    make(chan struct{}),
	}
	select {
	case <-ctx.Done():
	case c.ch <- cmd:
		select {
		case <-ctx.Done():
		case <-cmd.Done:
		}
	}
}

// StopClients stops clients.
func (c *Cluster) StopClients(ctx context.Context, clients ...*Client) {
	cmd := stopClients{
		Clients: clients,
		Done:    make(chan struct{}),
	}
	select {
	case <-ctx.Done():
	case c.ch <- cmd:
		select {
		case <-ctx.Done():
		case <-cmd.Done:
		}
	}
}

// ForceLeader sets the peer which should be a leader after next voting.
func (c *Cluster) ForceLeader(peer *Peer) {
	c.mesh.ForceLeader(peer)
}

// EnableLink enables link between peers.
func (c *Cluster) EnableLink(ctx context.Context, peer1, peer2 *Peer) error {
	if err := c.mesh.EnableLink(ctx, peer1, peer2); err != nil {
		return err
	}
	return c.mesh.EnableLink(ctx, peer2, peer1)
}

// DisableLink disables link between peers.
func (c *Cluster) DisableLink(ctx context.Context, peer1, peer2 *Peer) error {
	if err := c.mesh.DisableLink(ctx, peer1, peer2); err != nil {
		return err
	}
	return c.mesh.DisableLink(ctx, peer2, peer1)
}

func (c *Cluster) newPeerConfig(ctx context.Context, peer *Peer) (types.Config, error) {
	config := types.Config{
		ServerID:          peer.id,
		MaxMessageSize:    maxMsgSize,
		MaxUncommittedLog: maxUncommittedLog,
	}

	for _, p := range c.peers {
		var p2pAddress string
		if p != peer {
			pair, err := c.mesh.Pair(ctx, peer, p)
			if err != nil {
				return types.Config{}, err
			}
			p2pAddress = pair.SrcListener.Addr().String()
		}

		config.Servers = append(config.Servers, types.ServerConfig{
			ID:         p.id,
			P2PAddress: p2pAddress,
			Partitions: p.partitions,
		})
	}

	return config, nil
}
