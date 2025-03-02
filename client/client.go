package client

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/outofforest/magma/gossip"
	"github.com/outofforest/magma/gossip/p2c"
	rafttypes "github.com/outofforest/magma/raft/types"
	"github.com/outofforest/parallel"
	"github.com/outofforest/proton"
	"github.com/outofforest/resonance"
	"github.com/outofforest/varuint64"
)

const timeout = 5 * time.Second

// Config is the configuration of magma client.
type Config struct {
	PeerAddress     string
	TxMessageConfig resonance.Config
}

// New creates new magma client.
func New(config Config) *Client {
	return &Client{
		config: config,
		txCh:   make(chan []byte),
	}
}

// Client connects to magma network, receives log updates and sends transactions.
type Client struct {
	config Config
	txCh   chan []byte
}

// Run runs client.
func (c *Client) Run(ctx context.Context) error {
	for {
		err := resonance.RunClient(ctx, c.config.PeerAddress, gossip.P2CConfig,
			func(ctx context.Context, conn *resonance.Connection) (retErr error) {
				return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
					spawn("receiver", parallel.Fail, func(ctx context.Context) error {
						for {
							_, err := conn.ReceiveBytes()
							if err != nil {
								return err
							}
							//nolint:staticcheck
							return errors.New("unexpected message")
						}
					})
					spawn("sender", parallel.Fail, func(ctx context.Context) error {
						if !conn.SendProton(&rafttypes.CommitInfo{}, p2c.NewMarshaller()) {
							return errors.WithStack(ctx.Err())
						}
						for {
							select {
							case <-ctx.Done():
								return errors.WithStack(ctx.Err())
							case tx := <-c.txCh:
								if !conn.SendBytes(tx) {
									return errors.WithStack(ctx.Err())
								}
							}
						}
					})
					return nil
				})
			},
		)
		if err != nil && errors.Is(err, ctx.Err()) {
			return errors.WithStack(err)
		}
	}
}

// Broadcast broadcasts transaction to the magma network.
func (c *Client) Broadcast(ctx context.Context, tx []any, m proton.Marshaller) error {
	if len(tx) == 0 {
		return nil
	}
	var size uint64
	for _, o := range tx {
		s, err := m.Size(o)
		if err != nil {
			return err
		}
		id, err := m.ID(o)
		if err != nil {
			return err
		}
		size += s + varuint64.Size(s) + varuint64.Size(id)
	}

	buf := make([]byte, size)
	var i uint64
	for _, o := range tx {
		id, err := m.ID(o)
		if err != nil {
			return err
		}
		msgSize, err := m.Size(o)
		if err != nil {
			return err
		}
		totalSize := msgSize + varuint64.Size(id)
		i += varuint64.Put(buf[i:], totalSize)
		i += varuint64.Put(buf[i:], id)
		_, _, err = m.Marshal(o, buf[i:])
		if err != nil {
			return err
		}
		i += msgSize
	}

	select {
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case <-time.After(timeout):
		return errors.New("timeout on sending transaction")
	case c.txCh <- buf:
	}

	return nil
}
