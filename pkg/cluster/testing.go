package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/client"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/memdb"
	"github.com/outofforest/parallel"
	"github.com/outofforest/proton"
)

// TestingCluster is a set of helper around cluster for testing.
type TestingCluster struct {
	cluster  *Cluster
	ctx      context.Context //nolint:containedctx
	requireT *require.Assertions
}

// NewTesting returns new testing cluster wrapper.
func NewTesting(group *parallel.Group, t *testing.T) TestingCluster {
	tc := TestingCluster{
		cluster:  New(t.TempDir()),
		ctx:      group.Context(),
		requireT: require.New(t),
	}
	group.Spawn("cluster", parallel.Fail, tc.cluster.Run)
	return tc
}

// NewPeer creates new peer.
func (c TestingCluster) NewPeer(peerID types.ServerID, partitions types.Partitions) *Peer {
	peer, err := c.cluster.NewPeer(peerID, partitions)
	c.requireT.NoError(err)
	return peer
}

// NewClient returns new client.
func (c TestingCluster) NewClient(
	peer *Peer,
	name string,
	marshaller proton.Marshaller,
	partitionID types.PartitionID,
	triggerFunc func(context.Context, *client.View) error,
	indices ...memdb.Index,
) *Client {
	cl, err := c.cluster.NewClient(peer, name, marshaller, partitionID, triggerFunc, indices...)
	c.requireT.NoError(err)
	return cl
}

// StartPeers starts peers.
func (c TestingCluster) StartPeers(peers ...*Peer) {
	c.cluster.StartPeers(c.ctx, peers...)
}

// StopPeers stops peers.
func (c TestingCluster) StopPeers(peers ...*Peer) {
	c.cluster.StopPeers(c.ctx, peers...)
}

// DropData drops data stored by peer.
func (c TestingCluster) DropData(p *Peer) {
	c.requireT.NoError(c.cluster.DropData(p))
}

// StartClients starts clients.
func (c TestingCluster) StartClients(clients ...*Client) {
	c.cluster.StartClients(c.ctx, clients...)
}

// StopClients stops clients.
func (c TestingCluster) StopClients(clients ...*Client) {
	c.cluster.StopClients(c.ctx, clients...)
}

// ForceLeader sets the peer which should be a leader after next voting.
func (c TestingCluster) ForceLeader(peer *Peer) {
	c.cluster.ForceLeader(peer)
}

// EnableLink enables link between peers.
func (c TestingCluster) EnableLink(peer1, peer2 *Peer) {
	c.requireT.NoError(c.cluster.EnableLink(c.ctx, peer1, peer2))
}

// DisableLink disables link between peers.
func (c TestingCluster) DisableLink(peer1, peer2 *Peer) {
	c.requireT.NoError(c.cluster.DisableLink(c.ctx, peer1, peer2))
}
