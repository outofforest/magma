package client

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/pkg/errors"
	"github.com/zeebo/xxh3"
	"go.uber.org/zap"

	"github.com/outofforest/logger"
	"github.com/outofforest/magma/gossip/wire/c2p"
	rafttypes "github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/state/repository/format"
	"github.com/outofforest/parallel"
	"github.com/outofforest/proton"
	"github.com/outofforest/resonance"
	"github.com/outofforest/varuint64"
)

// Config is the configuration of magma client.
type Config struct {
	PeerAddress      string
	MaxMessageSize   uint64
	BroadcastTimeout time.Duration
}

// New creates new magma client.
func New(config Config, m proton.Marshaller) *Client {
	c := &Client{
		config:        config,
		txCh:          make(chan []byte, 1),
		m:             m,
		timeoutTicker: time.NewTicker(time.Hour),
		bufSize:       10 * (config.MaxMessageSize + varuint64.MaxSize),
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
	var previousChecksum uint64

	for {
		err := resonance.RunClient(ctx, c.config.PeerAddress, resonance.Config{MaxMessageSize: c.config.MaxMessageSize},
			func(ctx context.Context, conn *resonance.Connection) error {
				conn.BufferReads()
				conn.BufferWrites()

				if err := conn.SendProton(&c2p.Init{
					NextLogIndex: c.nextLogIndex,
				}, c2p.NewMarshaller()); err != nil {
					return errors.WithStack(err)
				}

				return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
					spawn("receiver", parallel.Fail, func(ctx context.Context) error {
					loop:
						for {
							txRaw, err := conn.ReceiveRawBytes()
							if err != nil {
								return err
							}
							txTotalLen := rafttypes.Index(len(txRaw))
							txLen, n := varuint64.Parse(txRaw)

							if txLen < format.ChecksumSize {
								return errors.New("unexpected tx size")
							}

							i := len(txRaw) - format.ChecksumSize
							checksum := xxh3.HashSeed(txRaw[:i], previousChecksum)
							if binary.LittleEndian.Uint64(txRaw[i:]) != checksum {
								return errors.New("tx checksum mismatch")
							}
							previousChecksum = checksum

							txLen -= format.ChecksumSize
							txRaw = txRaw[n : n+txLen]

							var count uint64
							buf := txRaw
							for len(buf) > 0 {
								size, n2 := varuint64.Parse(buf)
								if n2 == txLen {
									// This is a term mark. Ignore.
									continue loop
								}
								count++
								buf = buf[n2+size:]
							}

							tx := make([]any, 0, count)
							for len(txRaw) > 0 {
								size, n1 := varuint64.Parse(txRaw)
								id, n2 := varuint64.Parse(txRaw[n1:])
								m, msgSize, err := c.m.Unmarshal(id, txRaw[n1+n2:n1+size])
								if err != nil {
									return err
								}
								if msgSize != size-n2 {
									return errors.Errorf("unexpected message size")
								}
								tx = append(tx, m) //nolint:staticcheck
								txRaw = txRaw[n1+size:]
							}

							c.nextLogIndex += txTotalLen
						}
					})
					spawn("sender", parallel.Fail, func(ctx context.Context) error {
						for {
							select {
							case <-ctx.Done():
								return errors.WithStack(ctx.Err())
							case tx := <-c.txCh:
								if err := conn.SendRawBytes(tx); err != nil {
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
func (c *Client) Broadcast(tx []any) (retErr error) {
	defer broadcastRecover(&retErr)

	if len(tx) == 0 {
		return nil
	}

	if uint64(len(c.buf)) < c.config.MaxMessageSize {
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

		if i+1 > c.config.MaxMessageSize {
			return errors.Errorf("tx size %d exceeds allowed maximum %d", i, c.config.MaxMessageSize)
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
