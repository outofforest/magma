package client

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/outofforest/logger"
	"github.com/outofforest/magma/gossip/wire/c2p"
	rafttypes "github.com/outofforest/magma/raft/types"
	"github.com/outofforest/parallel"
	"github.com/outofforest/proton"
	"github.com/outofforest/resonance"
	"github.com/outofforest/varuint64"
)

// Config is the configuration of magma client.
type Config struct {
	PeerAddress      string
	C2P              resonance.Config
	BroadcastTimeout time.Duration
}

// New creates new magma client.
func New(config Config, m proton.Marshaller) *Client {
	c := &Client{
		config:        config,
		txCh:          make(chan []byte, 1),
		m:             m,
		timeoutTicker: time.NewTicker(time.Hour),
	}
	c.timeoutTicker.Stop()
	return c
}

// Client connects to magma network, receives log updates and sends transactions.
type Client struct {
	config        Config
	txCh          chan []byte
	m             proton.Marshaller
	nextLogIndex  rafttypes.Index
	timeoutTicker *time.Ticker
}

// Run runs client.
func (c *Client) Run(ctx context.Context) error {
	c.timeoutTicker.Reset(c.config.BroadcastTimeout)
	defer c.timeoutTicker.Stop()

	log := logger.Get(ctx)

	for {
		err := resonance.RunClient(ctx, c.config.PeerAddress, c.config.C2P,
			func(ctx context.Context, conn *resonance.Connection) error {
				if err := conn.SendProton(&rafttypes.CommitInfo{
					NextLogIndex: c.nextLogIndex,
				}, c2p.NewMarshaller()); err != nil {
					return errors.WithStack(err)
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
								if err := conn.SendBytes(tx); err != nil {
									return errors.WithStack(err)
								}
							}
						}
					})
					return nil
				})
			},
		)
		if ctx.Err() != nil {
			return errors.WithStack(ctx.Err())
		}

		log.Error("Connection failed", zap.Error(err))
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

	if size > c.config.C2P.MaxMessageSize {
		return errors.Errorf("tx size %d exceeds allowed maximum %d", size, c.config.C2P.MaxMessageSize)
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

	for range 2 {
		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		case <-c.timeoutTicker.C:
		case c.txCh <- buf:
			return nil
		}
	}

	return errors.New("timeout on sending transaction")
}
