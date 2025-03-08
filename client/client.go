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
		bufSize:       10 * (config.C2P.MaxMessageSize + varuint64.MaxSize),
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

	buf     []byte
	bufSize uint64
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
					CommittedCount: c.nextLogIndex,
				}, c2p.NewMarshaller()); err != nil {
					return errors.WithStack(err)
				}

				return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
					spawn("receiver", parallel.Fail, func(ctx context.Context) error {
					loop:
						for {
							msg, err := conn.ReceiveBytes()
							if err != nil {
								return err
							}

							msgLen := uint64(len(msg))
							if msgLen == 0 {
								c.nextLogIndex++
								continue loop
							}

							var n uint64
							buf := msg
							for len(buf) > 0 {
								size, n2 := varuint64.Parse(buf)
								if n2 == msgLen {
									// This is a term mark. Ignore.
									continue loop
								}
								n++
								buf = buf[n2+size:]
							}

							tx := make([]any, 0, n)
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
						for tx := range c.txCh {
							if err := conn.SendRawBytes(tx); err != nil {
								return errors.WithStack(err)
							}
						}

						return errors.WithStack(ctx.Err())
					})
					spawn("watchdog", parallel.Fail, func(ctx context.Context) error {
						defer close(c.txCh)

						<-ctx.Done()
						return errors.WithStack(ctx.Err())
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
func (c *Client) Broadcast(tx []any) (retErr error) {
	defer broadcastRecover(&retErr)

	if len(tx) == 0 {
		return nil
	}

	if uint64(len(c.buf)) < c.config.C2P.MaxMessageSize {
		c.buf = make([]byte, c.bufSize)
	}

	i := uint64(varuint64.MaxSize)
	for _, o := range tx {
		id, err := c.m.ID(o)
		if err != nil {
			return err
		}
		msgSize, err := c.m.Size(o)
		if err != nil {
			return err
		}
		if msgSize == 0 {
			return errors.New("tx message has 0 size")
		}
		totalSize := msgSize + varuint64.Size(id)
		i += varuint64.Put(c.buf[i:], totalSize)
		i += varuint64.Put(c.buf[i:], id)
		_, _, err = c.m.Marshal(o, c.buf[i:])
		if err != nil {
			return err
		}
		i += msgSize

		if i > c.config.C2P.MaxMessageSize {
			return errors.Errorf("tx size %d exceeds allowed maximum %d", i, c.config.C2P.MaxMessageSize)
		}
	}

	n := varuint64.Size(i - varuint64.MaxSize)
	varuint64.Put(c.buf[varuint64.MaxSize-n:], i-varuint64.MaxSize)

	for range 2 {
		select {
		case <-c.timeoutTicker.C:
		case c.txCh <- c.buf[varuint64.MaxSize-n : i]:
			c.buf = c.buf[i:]
			return nil
		}
	}

	return errors.New("timeout on sending transaction")
}

func broadcastRecover(err *error) {
	if recover() != nil {
		*err = errors.New("connection closed")
	}
}
