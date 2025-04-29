package client

import (
	"context"
	"encoding/binary"
	"reflect"
	"time"

	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
	"github.com/zeebo/xxh3"
	"go.uber.org/zap"

	"github.com/outofforest/logger"
	"github.com/outofforest/magma/gossip/wire/c2p"
	"github.com/outofforest/magma/state/repository/format"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/parallel"
	"github.com/outofforest/proton"
	"github.com/outofforest/resonance"
	"github.com/outofforest/varuint64"
)

// Config is the configuration of magma client.
type Config struct {
	PeerAddress      string
	PartitionID      types.PartitionID
	MaxMessageSize   uint64
	BroadcastTimeout time.Duration
}

func typeName(t reflect.Type) string {
	pkg := t.PkgPath()
	if pkg == "" {
		return t.Name()
	}
	return pkg + "." + t.Name()
}

type idInfo struct {
	IDIndex       int
	RevisionIndex int
}

// New creates new magma client.
func New(config Config, m proton.Marshaller) (*Client, error) {
	objectTypes := m.Messages()
	if len(objectTypes) == 0 {
		return nil, errors.New("no object types provided")
	}

	dbSchema := &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{},
	}

	idType := reflect.TypeOf(types.ID{})
	revisionType := reflect.TypeOf(types.Revision(0))

	typeDefs := map[uint64]idInfo{}
	for _, o := range objectTypes {
		t := reflect.TypeOf(o)
		idF, exists := t.FieldByName("ID")
		if !exists {
			return nil, errors.Errorf("object %s has no ID field", typeName(t))
		}
		if idF.Type != idType {
			return nil, errors.Errorf("object's %s ID field must be of type %s", typeName(t), typeName(idType))
		}
		revisionF, exists := t.FieldByName("Revision")
		if !exists {
			return nil, errors.Errorf("object %s has no Revision field", typeName(t))
		}
		if revisionF.Type != revisionType {
			return nil, errors.Errorf("object's %s Revision field must be of type %s", typeName(t),
				typeName(revisionType))
		}

		name := typeName(t)
		id, err := m.ID(reflect.New(t).Interface())
		if err != nil {
			return nil, err
		}
		if _, exists := typeDefs[id]; exists {
			return nil, errors.Errorf("double registration of object %s", name)
		}
		typeDefs[id] = idInfo{
			IDIndex:       idF.Index[0],
			RevisionIndex: revisionF.Index[0],
		}
		dbSchema.Tables[name] = &memdb.TableSchema{
			Name: name,
			Indexes: map[string]*memdb.IndexSchema{
				"id": {
					Name:    "id",
					Unique:  true,
					Indexer: &idIndexer{index: idF.Index[0]},
				},
			},
		}
	}

	db, err := memdb.NewMemDB(dbSchema)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	c := &Client{
		config:        config,
		txCh:          make(chan []byte, 1),
		m:             m,
		timeoutTicker: time.NewTicker(time.Hour),
		bufSize:       10 * (config.MaxMessageSize + varuint64.MaxSize),
		typeDefs:      typeDefs,
		db:            db,
	}
	c.timeoutTicker.Stop()
	return c, nil
}

// Client connects to magma network, receives log updates and sends transactions.
type Client struct {
	config        Config
	txCh          chan []byte
	m             proton.Marshaller
	timeoutTicker *time.Ticker

	buf     []byte
	bufSize uint64

	typeDefs map[uint64]idInfo
	db       *memdb.MemDB
}

// Run runs client.
func (c *Client) Run(ctx context.Context) error {
	c.timeoutTicker.Reset(c.config.BroadcastTimeout)
	defer c.timeoutTicker.Stop()

	log := logger.Get(ctx)
	var previousChecksum uint64
	var nextLogIndex types.Index

	for {
		err := resonance.RunClient(ctx, c.config.PeerAddress, resonance.Config{MaxMessageSize: c.config.MaxMessageSize},
			func(ctx context.Context, conn *resonance.Connection) error {
				conn.BufferReads()
				conn.BufferWrites()

				if err := conn.SendProton(&c2p.Init{
					PartitionID:  c.config.PartitionID,
					NextLogIndex: nextLogIndex,
				}, c2p.NewMarshaller()); err != nil {
					return errors.WithStack(err)
				}

				return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
					spawn("receiver", parallel.Fail, func(ctx context.Context) error {
						for {
							txRaw, err := conn.ReceiveRawBytes()
							if err != nil {
								return err
							}
							txTotalLen := types.Index(len(txRaw))
							txLen, n := varuint64.Parse(txRaw)

							if txLen < format.ChecksumSize {
								return errors.New("unexpected tx size")
							}

							i := len(txRaw) - format.ChecksumSize
							checksum := xxh3.HashSeed(txRaw[:i], previousChecksum)
							if binary.LittleEndian.Uint64(txRaw[i:]) != checksum {
								return errors.New("tx checksum mismatch")
							}

							txLen -= format.ChecksumSize
							txRaw = txRaw[n : n+txLen]

							if _, n2 := varuint64.Parse(txRaw); n2 == txLen {
								// This is a term mark. Ignore.
								previousChecksum = checksum
								nextLogIndex += txTotalLen
								continue
							}

							if err := c.applyTx(txRaw); err != nil {
								return err
							}

							previousChecksum = checksum
							nextLogIndex += txTotalLen
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

func (c *Client) applyTx(txRaw []byte) (retErr error) {
	tx := c.db.Txn(true)
	defer func() {
		if retErr != nil {
			tx.Abort()
		}
	}()

	for len(txRaw) > 0 {
		size, n1 := varuint64.Parse(txRaw)
		msgID, n2 := varuint64.Parse(txRaw[n1:])

		typeDef, exists := c.typeDefs[msgID]
		if !exists {
			return errors.Errorf("unknown message ID %d", msgID)
		}
		m, msgSize, err := c.m.Unmarshal(msgID, txRaw[n1+n2:n1+size])
		if err != nil {
			return err
		}
		if msgSize != size-n2 {
			return errors.Errorf("unexpected message size")
		}

		newO := reflect.ValueOf(m).Elem()
		name := typeName(newO.Type())
		oID := newO.Field(typeDef.IDIndex).Interface()
		oldO, err := tx.First(name, "id", oID)
		if err != nil {
			return errors.WithStack(err)
		}

		if oldO != nil {
			newRevision := newO.Field(typeDef.RevisionIndex).Interface().(types.Revision)
			oldRevision := reflect.ValueOf(oldO).Field(typeDef.RevisionIndex).Interface().(types.Revision)
			if newRevision <= oldRevision {
				tx.Abort()
				return nil
			}
		}

		if err := tx.Insert(name, newO.Interface()); err != nil {
			return errors.WithStack(err)
		}

		txRaw = txRaw[n1+size:]
	}
	tx.Commit()
	return nil
}

func broadcastRecover(err *error) {
	if recover() != nil {
		*err = errors.New("connection closed")
	}
}

type idIndexer struct {
	index int
}

func (idi *idIndexer) FromArgs(args ...any) ([]byte, error) {
	id := args[0].(types.ID)
	return id[:], nil
}

func (idi *idIndexer) FromObject(obj any) (bool, []byte, error) {
	id := reflect.ValueOf(obj).Field(idi.index).Interface().(types.ID)
	return true, id[:], nil
}
