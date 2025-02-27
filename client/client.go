package client

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/outofforest/magma/gossip"
	"github.com/outofforest/magma/gossip/p2c"
	rafttypes "github.com/outofforest/magma/raft/types"
	"github.com/outofforest/proton"
	"github.com/outofforest/resonance"
	"github.com/outofforest/varuint64"
)

const timeout = 5 * time.Second

// Config is the configuration of magma client.
type Config[M proton.Marshaller] struct {
	PeerAddress     string
	TxMessageConfig resonance.Config[M]
}

// New creates new magma client.
func New[M proton.Marshaller](config Config[M]) *Client[M] {
	return &Client[M]{
		config: config,
		txCh:   make(chan []byte),
		m:      config.TxMessageConfig.MarshallerFactory(),
	}
}

// Client connects to magma network, receives log updates and sends transactions.
type Client[M proton.Marshaller] struct {
	config Config[M]
	txCh   chan []byte
	m      M
}

// Run runs client.
func (c *Client[M]) Run(ctx context.Context) error {
	for {
		err := resonance.RunClient(ctx, c.config.PeerAddress, gossip.P2CConfig,
			func(ctx context.Context, recvCh <-chan any, conn *resonance.Connection[p2c.Marshaller]) (retErr error) {
				for {
					select {
					case _, ok := <-recvCh:
						if !ok {
							return errors.WithStack(ctx.Err())
						}
						return errors.New("unexpected message received")
					case tx := <-c.txCh:
						conn.Send(&rafttypes.ClientRequest{
							Data: tx,
						})
					}
				}
			},
		)
		if err != nil && errors.Is(err, ctx.Err()) {
			return errors.WithStack(err)
		}
	}
}

// Broadcast broadcasts transaction to the magma network.
func (c *Client[M]) Broadcast(ctx context.Context, tx []any) error {
	if len(tx) == 0 {
		return nil
	}
	size := 2 * uint64(len(tx)) * varuint64.MaxSize
	for _, o := range tx {
		s, err := c.m.Size(o)
		if err != nil {
			return err
		}
		size += s
	}

	buf := make([]byte, size)
	var i uint64
	for _, o := range tx {
		id, err := c.m.ID(o)
		if err != nil {
			return err
		}
		msgSize, err := c.m.Size(o)
		if err != nil {
			return err
		}
		totalSize := msgSize + varuint64.Size(id)
		i += varuint64.Put(buf[i:], totalSize)
		i += varuint64.Put(buf[i:], id)
		_, _, err = c.m.Marshal(o, buf[i:])
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
