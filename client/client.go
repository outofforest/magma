package client

import (
	"context"
	"encoding/binary"
	"reflect"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
	"github.com/zeebo/xxh3"
	"go.uber.org/zap"

	"github.com/outofforest/logger"
	"github.com/outofforest/magma/client/wire"
	gossipwire "github.com/outofforest/magma/gossip/wire"
	"github.com/outofforest/magma/gossip/wire/c2p"
	"github.com/outofforest/magma/state/repository/format"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/parallel"
	"github.com/outofforest/proton"
	"github.com/outofforest/resonance"
	"github.com/outofforest/varuint64"
)

var (
	// ErrBroadcastTimeout means that client was not able to broadcast the transaction before timeout.
	ErrBroadcastTimeout = errors.New("broadcast timeout")

	// ErrAwaitTimeout means that client hasn't received broadcasted transaction before timeout.
	ErrAwaitTimeout = errors.New("await timeout")

	// ErrOutdatedTx means that awaited transaction is outdated and hasn't been applied.
	ErrOutdatedTx = errors.New("outdated transaction")
)

var idType = reflect.TypeOf(types.ID{})

// Config is the configuration of magma client.
type Config struct {
	Service          string
	PeerAddress      string
	PartitionID      types.PartitionID
	MaxMessageSize   uint64
	BroadcastTimeout time.Duration
	AwaitTimeout     time.Duration
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

	byID := map[uint64]typeInfo{}
	byType := map[reflect.Type]typeInfo{}
	for _, o := range objectTypes {
		t := reflect.TypeOf(o)

		idF, exists := t.FieldByName("ID")
		if !exists {
			return nil, errors.Errorf("object %s has no ID field", t)
		}
		if !idF.Type.ConvertibleTo(idType) {
			return nil, errors.Errorf("object's %s ID field must be of type %s", t, idType)
		}
		revisionF, exists := t.FieldByName("Revision")
		if !exists {
			return nil, errors.Errorf("object %s has no Revision field", t)
		}
		if revisionF.Type != revisionType {
			return nil, errors.Errorf("object's %s Revision field must be of type %s", t, revisionType)
		}

		if _, exists := byType[t]; exists {
			return nil, errors.Errorf("double registration of object %s", t)
		}

		mID, err := m.ID(reflect.New(t).Interface())
		if err != nil {
			return nil, err
		}

		table := typeName(t)
		info := typeInfo{
			IDIndex:       idF.Index[0],
			RevisionIndex: revisionF.Index[0],
			Type:          t,
			IDType:        idF.Type,
			Table:         table,
		}
		byID[mID] = info
		byType[t] = info
		dbSchema.Tables[table] = &memdb.TableSchema{
			Name: table,
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

	metaM := wire.NewMarshaller()

	metaID, err := metaM.ID(&wire.TxMetadata{})
	if err != nil {
		return nil, err
	}

	entityMetadataID, err := metaM.ID(&wire.EntityMetadata{})
	if err != nil {
		return nil, err
	}

	return &Client{
		config:           config,
		txCh:             make(chan tx, 1),
		m:                m,
		metaM:            metaM,
		doneCh:           make(chan struct{}),
		bufSize:          10 * (config.MaxMessageSize + varuint64.MaxSize),
		byID:             byID,
		byType:           byType,
		db:               db,
		metaID:           metaID,
		entityMetadataID: entityMetadataID,
	}, nil
}

// Client connects to magma network, receives log updates and sends transactions.
type Client struct {
	config Config
	txCh   chan tx
	m      proton.Marshaller
	metaM  proton.Marshaller
	doneCh chan struct{}

	byID   map[uint64]typeInfo
	byType map[reflect.Type]typeInfo
	db     *memdb.MemDB

	bufSize          uint64
	metaID           uint64
	entityMetadataID uint64
}

// Run runs client.
func (c *Client) Run(ctx context.Context) error {
	defer close(c.doneCh)

	log := logger.Get(ctx)
	var previousChecksum uint64
	var nextLogIndex types.Index

	var mu sync.Mutex
	awaitedTxs := map[uuid.UUID]chan<- error{}
	var awaitedTxsToClean []uuid.UUID

	cMarshaller := c2p.NewMarshaller()

	for {
		err := resonance.RunClient(ctx, c.config.PeerAddress, resonance.Config{MaxMessageSize: c.config.MaxMessageSize},
			func(ctx context.Context, conn *resonance.Connection) error {
				conn.BufferReads()
				conn.BufferWrites()

				if err := conn.SendProton(&c2p.Init{
					PartitionID:  c.config.PartitionID,
					NextLogIndex: nextLogIndex,
				}, cMarshaller); err != nil {
					return errors.WithStack(err)
				}

				return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
					spawn("receiver", parallel.Fail, func(ctx context.Context) error {
						for {
							m, err := conn.ReceiveProton(cMarshaller)
							if err != nil {
								return err
							}

							msg, ok := m.(*gossipwire.StartLogStream)
							if !ok {
								return errors.Errorf("unexpected message %T", msg)
							}

							var length uint64
							for length < msg.Length {
								txRaw, err := conn.ReceiveRawBytes()
								if err != nil {
									return err
								}

								length += uint64(len(txRaw))

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

								metaID, n := varuint64.Parse(txRaw)
								metaAny, metaSize, err := c.metaM.Unmarshal(metaID, txRaw[n:])
								if err != nil {
									return err
								}
								meta := metaAny.(*wire.TxMetadata)

								err = c.applyTx(meta.EntityMetadataID, txRaw[n+metaSize:])
								if err != nil && !errors.Is(err, ErrOutdatedTx) {
									return err
								}

								previousChecksum = checksum
								nextLogIndex += txTotalLen

								mu.Lock()
								receivedCh := awaitedTxs[meta.ID]
								if receivedCh != nil {
									delete(awaitedTxs, meta.ID)
									receivedCh <- err
								}
								mu.Unlock()
							}
						}
					})
					spawn("sender", parallel.Fail, func(ctx context.Context) error {
						for {
							select {
							case <-ctx.Done():
								return errors.WithStack(ctx.Err())
							case tx := <-c.txCh:
								mu.Lock()
								delete(awaitedTxs, tx.PreviousTxID)
								awaitedTxs[tx.ID] = tx.ReceivedCh
								mu.Unlock()

								if err := conn.SendRawBytes(tx.Tx); err != nil {
									return errors.WithStack(err)
								}
							}
						}
					})
					spawn("cleaner", parallel.Fail, func(ctx context.Context) error {
						for {
							select {
							case <-ctx.Done():
								return errors.WithStack(ctx.Err())
							case <-time.After(2 * c.config.AwaitTimeout):
								mu.Lock()
								for _, id := range awaitedTxsToClean {
									delete(awaitedTxs, id)
								}
								awaitedTxsToClean = make([]uuid.UUID, 0, len(awaitedTxs))
								for id := range awaitedTxs {
									awaitedTxsToClean = append(awaitedTxsToClean, id)
								}
								mu.Unlock()
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
		service:          c.config.Service,
		txCh:             c.txCh,
		m:                c.m,
		metaM:            c.metaM,
		byType:           c.byType,
		db:               c.db,
		maxMessageSize:   c.config.MaxMessageSize,
		bufSize:          c.bufSize,
		broadcastTimeout: c.config.BroadcastTimeout,
		awaitTimeout:     c.config.AwaitTimeout,
		doneCh:           c.doneCh,
		metaID:           c.metaID,
		entityMetadataID: c.entityMetadataID,
	}
}

func (c *Client) applyTx(entityMetaID uint64, txRaw []byte) (retErr error) {
	tx := c.db.Txn(true)
	defer func() {
		if retErr != nil {
			tx.Abort()
		}
	}()

	for len(txRaw) > 0 {
		entityMetaRaw, entityMetaSize, err := c.metaM.Unmarshal(entityMetaID, txRaw)
		if err != nil {
			return err
		}

		entityMeta := entityMetaRaw.(*wire.EntityMetadata)

		typeDef, exists := c.byID[entityMeta.MessageID]
		if !exists {
			return errors.Errorf("unknown type %s", typeDef.Type)
		}

		o, err := tx.First(typeDef.Table, idIndex, entityMeta.ID)
		if err != nil {
			return errors.WithStack(err)
		}

		oValue := reflect.ValueOf(o)
		if o != nil {
			oldRevision := oValue.Field(typeDef.RevisionIndex).Interface().(types.Revision)
			if entityMeta.Revision <= oldRevision {
				tx.Abort()
				return ErrOutdatedTx
			}
		}

		ov := reflect.New(typeDef.Type)
		ovValue := ov.Elem()
		if o != nil {
			ovValue.Set(oValue)
		}

		msgSize, err := c.m.ApplyPatch(ov.Interface(), txRaw[entityMetaSize:])
		if err != nil {
			return err
		}

		if o == nil {
			idF := ovValue.Field(typeDef.IDIndex)
			idF.Set(reflect.ValueOf(entityMeta.ID).Convert(typeDef.IDType))
		}
		ovValue.Field(typeDef.RevisionIndex).Set(reflect.ValueOf(entityMeta.Revision))

		if err := tx.Insert(typeDef.Table, ovValue.Interface()); err != nil {
			return errors.WithStack(err)
		}

		txRaw = txRaw[entityMetaSize+msgSize:]
	}
	tx.Commit()
	return nil
}

type tx struct {
	ID           uuid.UUID
	Tx           []byte
	ReceivedCh   chan<- error
	PreviousTxID uuid.UUID
}

// Transactor builds and broadcasts transactions.
type Transactor struct {
	service          string
	txCh             chan tx
	m                proton.Marshaller
	metaM            proton.Marshaller
	byType           map[reflect.Type]typeInfo
	db               *memdb.MemDB
	maxMessageSize   uint64
	buf              []byte
	bufSize          uint64
	broadcastTimeout time.Duration
	awaitTimeout     time.Duration
	doneCh           chan struct{}
	previousTxID     uuid.UUID
	metaID           uint64
	entityMetadataID uint64
}

// Tx creates and broadcasts transaction to the magma network.
func (t *Transactor) Tx(ctx context.Context, txF func(tx *Tx) error) (retErr error) {
	pendingTx := &Tx{
		View: &View{
			tx:     t.db.Txn(true),
			byType: t.byType,
		},
		changes: map[types.ID]reflect.Value{},
	}

	defer pendingTx.tx.Abort()
	defer txRecover(&retErr)

	snapshot := t.db.Txn(false)
	if err := txF(pendingTx); err != nil {
		return err
	}

	pendingTx.tx.Abort()

	if len(pendingTx.changes) == 0 {
		return nil
	}

	if uint64(len(t.buf)) < t.maxMessageSize {
		t.buf = make([]byte, t.bufSize)
	}

	i := uint64(varuint64.MaxSize)

	txID := uuid.New()
	previousTxID := t.previousTxID
	t.previousTxID = txID

	meta := &wire.TxMetadata{
		ID:               txID,
		Time:             time.Now(),
		Service:          t.service,
		EntityMetadataID: t.entityMetadataID,
	}

	i += varuint64.Put(t.buf[i:], t.metaID)
	_, metaSize, err := t.metaM.Marshal(meta, t.buf[i:])
	if err != nil {
		return err
	}
	i += metaSize

	for _, v := range pendingTx.changes {
		ov := reflect.New(v.Type())
		ovValue := ov.Elem()
		ovValue.Set(v)
		o := ov.Interface()

		id, err := t.m.ID(o)
		if err != nil {
			return err
		}

		typeDef, exists := t.byType[ovValue.Type()]
		if !exists {
			return errors.Errorf("unknown type %s", ovValue.Type())
		}

		idF := ovValue.Field(typeDef.IDIndex)
		oOldValue, err := snapshot.First(typeDef.Table, idIndex, idF.Interface())
		if err != nil {
			return errors.WithStack(err)
		}
		oOldV := reflect.New(v.Type())
		oOldValueV := oOldV.Elem()
		if oOldValue != nil {
			oOldValueV.Set(reflect.ValueOf(oOldValue))
		}

		revisionF := oOldValueV.Field(typeDef.RevisionIndex)
		entityMeta := &wire.EntityMetadata{
			ID:        idF.Convert(idType).Interface().(types.ID),
			Revision:  types.Revision(revisionF.Uint() + 1),
			MessageID: id,
		}
		_, entitySize, err := t.metaM.Marshal(entityMeta, t.buf[i:])
		if err != nil {
			return err
		}
		i += entitySize

		_, msgSize, err := t.m.MakePatch(o, oOldV.Interface(), t.buf[i:])
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

	receivedCh := make(chan error, 1)
	select {
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case <-t.doneCh:
		if ctx.Err() != nil {
			return errors.WithStack(ctx.Err())
		}
		return errors.New("client closed")
	case <-time.After(t.broadcastTimeout):
		return ErrBroadcastTimeout
	case t.txCh <- tx{
		ID:           txID,
		Tx:           t.buf[varuint64.MaxSize-n : i],
		ReceivedCh:   receivedCh,
		PreviousTxID: previousTxID,
	}:
		t.buf = t.buf[i:]
	}

	select {
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case <-t.doneCh:
		if ctx.Err() != nil {
			return errors.WithStack(ctx.Err())
		}
		return errors.New("client closed")
	case <-time.After(t.awaitTimeout):
		return ErrAwaitTimeout
	case err := <-receivedCh:
		return err
	}
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
	Type          reflect.Type
	IDType        reflect.Type
	Table         string
}
