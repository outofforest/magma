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
func New(config Config, m proton.Marshaller) *Client {
	return &Client{
		config: config,
		txCh:   make(chan []byte),
		m:      m,
	}
}

// Client connects to magma network, receives log updates and sends transactions.
type Client struct {
	config       Config
	txCh         chan []byte
	m            proton.Marshaller
	nextLogIndex rafttypes.Index
}

// Run runs client.
func (c *Client) Run(ctx context.Context) error {
	for {
		err := resonance.RunClient(ctx, c.config.PeerAddress, gossip.P2CConfig,
			func(ctx context.Context, conn *resonance.Connection) error {
				if !conn.SendProton(&rafttypes.CommitInfo{
					NextLogIndex: c.nextLogIndex,
				}, p2c.NewMarshaller()) {
					return errors.WithStack(ctx.Err())
				}

				return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
					spawn("receiver", parallel.Fail, func(ctx context.Context) error {
						for {
							msg, err := conn.ReceiveBytes()
							if err != nil {
								return err
							}

							msgLen := uint64(len(msg))
							if msgLen == 0 {
								c.nextLogIndex++
								continue
							}

							tx := []any{}
							for len(msg) > 0 {
								size, n1 := varuint64.Parse(msg)
								id, n2 := varuint64.Parse(msg[n1:])
								m, msgSize, err := c.m.Unmarshal(id, msg[n1+n2:n1+size])
								if err != nil {
									return err
								}
								if msgSize != size-n2 {
									return errors.Errorf("unexpected message size")
								}
								tx = append(tx, m) //nolint:staticcheck
								msg = msg[n1+size:]
							}

							c.nextLogIndex += rafttypes.Index(msgLen + varuint64.Size(msgLen))
						}
					})
					spawn("sender", parallel.Fail, func(ctx context.Context) error {
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
func (c *Client) Broadcast(ctx context.Context, tx []any) error {
	if len(tx) == 0 {
		return nil
	}
	var size uint64
	for _, o := range tx {
		s, err := c.m.Size(o)
		if err != nil {
			return err
		}
		id, err := c.m.ID(o)
		if err != nil {
			return err
		}
		size += s + varuint64.Size(s) + varuint64.Size(id)
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
