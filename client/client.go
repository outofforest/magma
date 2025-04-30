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

var idType = reflect.TypeOf(types.ID{})

// Config is the configuration of magma client.
type Config struct {
	PeerAddress      string
	PartitionID      types.PartitionID
	MaxMessageSize   uint64
	BroadcastTimeout time.Duration
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

	revisionType := reflect.TypeOf(types.Revision(0))

	typeDefs := map[reflect.Type]typeInfo{}
	for _, o := range objectTypes {
		t := reflect.TypeOf(o)
		idF, exists := t.FieldByName("ID")
		if !exists {
			return nil, errors.Errorf("object %s has no ID field", typeName(t))
		}
		if !idF.Type.ConvertibleTo(idType) {
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
		if _, exists := typeDefs[t]; exists {
			return nil, errors.Errorf("double registration of object %s", name)
		}
		typeDefs[t] = typeInfo{
			IDIndex:       idF.Index[0],
			RevisionIndex: revisionF.Index[0],
		}
		dbSchema.Tables[name] = &memdb.TableSchema{
			Name: name,
			Indexes: map[string]*memdb.IndexSchema{
				idIndex: {
					Name:    idIndex,
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
		doneCh:        make(chan struct{}),
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
	doneCh        chan struct{}

	typeDefs map[reflect.Type]typeInfo
	db       *memdb.MemDB

	bufSize uint64
}

// Run runs client.
func (c *Client) Run(ctx context.Context) error {
	defer close(c.doneCh)
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

// NewTransactor creates new transactor.
func (c *Client) NewTransactor() *Transactor {
	return &Transactor{
		txCh:           c.txCh,
		m:              c.m,
		typeDefs:       c.typeDefs,
		db:             c.db,
		timeoutTicker:  c.timeoutTicker,
		maxMessageSize: c.config.MaxMessageSize,
		bufSize:        c.bufSize,
		doneCh:         c.doneCh,
	}
}

func (c *Client) applyTx(txRaw []byte) (retErr error) {
	tx := c.db.Txn(true)
	defer func() {
		if retErr != nil {
			tx.Abort()
		}
	}()

	for len(txRaw) > 0 {
		msgID, n := varuint64.Parse(txRaw)
		m, msgSize, err := c.m.Unmarshal(msgID, txRaw[n:])
		if err != nil {
			return err
		}

		newO := reflect.ValueOf(m).Elem()
		newOType := newO.Type()
		name := typeName(newOType)

		typeDef, exists := c.typeDefs[newOType]
		if !exists {
			return errors.Errorf("unknown type %s", name)
		}

		oID := newO.Field(typeDef.IDIndex).Interface()
		oldO, err := tx.First(name, idIndex, oID)
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

		txRaw = txRaw[n+msgSize:]
	}
	tx.Commit()
	return nil
}

// Transactor builds and broadcasts transactions.
type Transactor struct {
	txCh           chan []byte
	m              proton.Marshaller
	typeDefs       map[reflect.Type]typeInfo
	db             *memdb.MemDB
	timeoutTicker  *time.Ticker
	maxMessageSize uint64
	buf            []byte
	bufSize        uint64
	doneCh         chan struct{}
}

// Tx creates and broadcasts transaction to the magma network.
func (t *Transactor) Tx(ctx context.Context, txF func(tx *Tx) error) (retErr error) {
	tx := &Tx{
		View: &View{
			tx:       t.db.Txn(true),
			typeDefs: t.typeDefs,
		},
		changes: map[types.ID]any{},
	}

	defer tx.tx.Abort()
	defer txRecover(&retErr)

	if err := txF(tx); err != nil {
		return err
	}

	if len(tx.changes) == 0 {
		return nil
	}

	if uint64(len(t.buf)) < t.maxMessageSize {
		t.buf = make([]byte, t.bufSize)
	}

	i := uint64(varuint64.MaxSize)
	for _, o := range tx.changes {
		ov := reflect.New(reflect.TypeOf(o))
		ovValue := ov.Elem()
		ovValue.Set(reflect.ValueOf(o))

		name := typeName(ovValue.Type())
		typeDef, exists := t.typeDefs[ovValue.Type()]
		if !exists {
			return errors.Errorf("unknown type %s", name)
		}
		revisionF := ovValue.Field(typeDef.RevisionIndex)
		revisionF.Set(reflect.ValueOf(types.Revision(revisionF.Uint() + 1)))

		o = ov.Interface()

		id, err := t.m.ID(o)
		if err != nil {
			return err
		}
		i += varuint64.Put(t.buf[i:], id)
		_, msgSize, err := t.m.Marshal(o, t.buf[i:])
		if err != nil {
			return err
		}
		i += msgSize

		if i+1 > t.maxMessageSize {
			return errors.Errorf("tx size %d exceeds allowed maximum %d", i, t.maxMessageSize)
		}
	}

	n := varuint64.Size(i - varuint64.MaxSize)
	varuint64.Put(t.buf[varuint64.MaxSize-n:], i-varuint64.MaxSize)

	for range 2 {
		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		case <-t.doneCh:
			if ctx.Err() != nil {
				return errors.WithStack(ctx.Err())
			}
			return errors.New("client closed")
		case <-t.timeoutTicker.C:
		case t.txCh <- t.buf[varuint64.MaxSize-n : i]:
			t.buf = t.buf[i:]
			return nil
		}
	}

	return errors.New("timeout on sending transaction")
}

func txRecover(err *error) {
	if r := recover(); r != nil {
		if e, ok := r.(error); ok {
			*err = e
			return
		}
		*err = errors.New("transaction panicked")
	}
}

type idIndexer struct {
	index int
}

func (idi *idIndexer) FromArgs(args ...any) ([]byte, error) {
	id := reflect.ValueOf(args[0]).Convert(idType).Interface().(types.ID)
	return id[:], nil
}

func (idi *idIndexer) FromObject(o any) (bool, []byte, error) {
	id := reflect.ValueOf(o).Field(idi.index).Convert(idType).Interface().(types.ID)
	return true, id[:], nil
}

func typeName(t reflect.Type) string {
	pkg := t.PkgPath()
	if pkg == "" {
		return t.Name()
	}
	return pkg + "." + t.Name()
}

type typeInfo struct {
	IDIndex       int
	RevisionIndex int
}
