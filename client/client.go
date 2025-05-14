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
	"github.com/outofforest/magma/client/indices"
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
	Marshaller       proton.Marshaller
	Indices          []indices.Index
	TriggerFunc      func(ctx context.Context, v *View) error
}

// New creates new magma client.
func New(config Config) (*Client, error) {
	objectTypes := config.Marshaller.Messages()
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

		mID, err := config.Marshaller.ID(reflect.New(t).Interface())
		if err != nil {
			return nil, err
		}

		tableName := typeName(t)
		info := typeInfo{
			IDIndex:       idF.Index[0],
			RevisionIndex: revisionF.Index[0],
			Type:          t,
			IDType:        idF.Type,
			Table:         tableName,
		}
		byID[mID] = info
		byType[t] = info

		table := &memdb.TableSchema{
			Name: tableName,
			Indexes: map[string]*memdb.IndexSchema{
				idIndex: {
					Name:    idIndex,
					Unique:  true,
					Indexer: &idIndexer{index: idF.Index[0]},
				},
			},
		}
		for _, index := range config.Indices {
			if index.Type() == t {
				table.Indexes[index.Name()] = index.Schema()
			}
		}
		dbSchema.Tables[tableName] = table
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
		metaM:            metaM,
		doneCh:           make(chan struct{}),
		bufSize:          10 * (config.MaxMessageSize + varuint64.MaxSize),
		byID:             byID,
		byType:           byType,
		db:               db,
		metaID:           metaID,
		entityMetadataID: entityMetadataID,
		readyCh:          make(chan struct{}),
	}, nil
}

// Client connects to magma network, receives log updates and sends transactions.
type Client struct {
	config Config
	txCh   chan tx
	metaM  proton.Marshaller
	doneCh chan struct{}

	byID   map[uint64]typeInfo
	byType map[reflect.Type]typeInfo
	db     *memdb.MemDB

	bufSize          uint64
	metaID           uint64
	entityMetadataID uint64

	readyCh chan struct{}
}

// Run runs client.
//
//nolint:gocyclo
func (c *Client) Run(ctx context.Context) error {
	defer close(c.doneCh)

	log := logger.Get(ctx)
	var previousChecksum uint64
	var nextLogIndex types.Index

	var mu sync.Mutex
	awaitedTxs := map[uuid.UUID]chan<- error{}
	var awaitedTxsToClean []uuid.UUID

	cMarshaller := c2p.NewMarshaller()

	var firstHotEnd bool

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
					triggerCh := make(chan struct{}, 1)

					spawn("receiver", parallel.Fail, func(ctx context.Context) error {
						defer close(triggerCh)

						for {
							m, err := conn.ReceiveProton(cMarshaller)
							if err != nil {
								return err
							}

							switch msg := m.(type) {
							case *gossipwire.StartLogStream:
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
							case *gossipwire.HotEnd:
								if !firstHotEnd {
									firstHotEnd = true
									close(c.readyCh)
								}

								if c.config.TriggerFunc != nil {
									if len(triggerCh) > 0 {
										select {
										case <-triggerCh:
										default:
										}
									}
									triggerCh <- struct{}{}
								}
							default:
								return errors.Errorf("unexpected message %T", msg)
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
					if c.config.TriggerFunc != nil {
						spawn("trigger", parallel.Fail, func(ctx context.Context) error {
							for range triggerCh {
								if err := c.config.TriggerFunc(ctx, &View{
									tx:     c.db.Txn(false),
									byType: c.byType,
								}); err != nil {
									return err
								}
							}
							return errors.WithStack(ctx.Err())
						})
					}

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

// WarmUp waits until hot end is reached for the first time.
func (c *Client) WarmUp(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case <-c.readyCh:
		return nil
	}
}

// View returns db view.
func (c *Client) View() *View {
	return &View{
		tx:     c.db.Txn(false),
		byType: c.byType,
	}
}

// NewTransactor creates new transactor.
func (c *Client) NewTransactor() *Transactor {
	return &Transactor{
		client:  c,
		changes: map[types.ID]reflect.Value{},
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

		oV := reflect.New(typeDef.Type)
		if o != nil {
			oV2 := o.(reflect.Value).Elem()
			oldRevision := oV2.Field(typeDef.RevisionIndex).Interface().(types.Revision)
			if entityMeta.Revision <= oldRevision {
				tx.Abort()
				return ErrOutdatedTx
			}
			oV.Elem().Set(oV2)
		}

		msgSize, err := c.config.Marshaller.ApplyPatch(oV.Interface(), txRaw[entityMetaSize:])
		if err != nil {
			return err
		}

		if o == nil {
			idF := oV.Elem().Field(typeDef.IDIndex)
			idF.Set(reflect.ValueOf(entityMeta.ID).Convert(typeDef.IDType))
		}
		oV.Elem().Field(typeDef.RevisionIndex).Set(reflect.ValueOf(entityMeta.Revision))

		if err := tx.Insert(typeDef.Table, oV); err != nil {
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
	client       *Client
	changes      map[types.ID]reflect.Value
	previousTxID uuid.UUID
	buf          []byte
}

// Tx creates and broadcasts transaction to the magma network.
func (t *Transactor) Tx(ctx context.Context, txF func(tx *Tx) error) (retErr error) {
	defer clear(t.changes)

	pendingTx := &Tx{
		View: &View{
			tx:     t.client.db.Txn(true),
			byType: t.client.byType,
		},
		changes: t.changes,
	}

	defer pendingTx.tx.Abort()
	defer txRecover(&retErr)

	snapshot := t.client.db.Txn(false)
	if err := txF(pendingTx); err != nil {
		return err
	}

	pendingTx.tx.Abort()

	if len(pendingTx.changes) == 0 {
		return nil
	}

	if uint64(len(t.buf)) < t.client.config.MaxMessageSize {
		t.buf = make([]byte, t.client.bufSize)
	}

	i := uint64(varuint64.MaxSize)

	txID := uuid.New()
	previousTxID := t.previousTxID
	t.previousTxID = txID

	meta := &wire.TxMetadata{
		ID:               txID,
		Time:             time.Now(),
		Service:          t.client.config.Service,
		EntityMetadataID: t.client.entityMetadataID,
	}

	i += varuint64.Put(t.buf[i:], t.client.metaID)
	_, metaSize, err := t.client.metaM.Marshal(meta, t.buf[i:])
	if err != nil {
		return err
	}
	i += metaSize

	for _, v := range pendingTx.changes {
		id, err := t.client.config.Marshaller.ID(v.Interface())
		if err != nil {
			return err
		}

		vv := v.Elem()
		typeDef, exists := t.client.byType[vv.Type()]
		if !exists {
			return errors.Errorf("unknown type %s", vv.Type())
		}

		idF := vv.Field(typeDef.IDIndex)
		old, err := snapshot.First(typeDef.Table, idIndex, idF.Interface())
		if err != nil {
			return errors.WithStack(err)
		}

		var oldV reflect.Value
		var revision types.Revision
		if old == nil {
			oldV = reflect.New(typeDef.Type)
		} else {
			oldV = old.(reflect.Value)
			revisionF := oldV.Elem().Field(typeDef.RevisionIndex)
			revision = types.Revision(revisionF.Uint() + 1)
		}

		entityMeta := &wire.EntityMetadata{
			ID:        idF.Convert(idType).Interface().(types.ID),
			Revision:  revision,
			MessageID: id,
		}
		_, entitySize, err := t.client.metaM.Marshal(entityMeta, t.buf[i:])
		if err != nil {
			return err
		}
		i += entitySize

		_, msgSize, err := t.client.config.Marshaller.MakePatch(v.Interface(), oldV.Interface(), t.buf[i:])
		if err != nil {
			return err
		}
		i += msgSize

		if i+1 > t.client.config.MaxMessageSize {
			return errors.Errorf("tx size %d exceeds allowed maximum %d", i, t.client.config.MaxMessageSize)
		}
	}

	n := varuint64.Size(i - varuint64.MaxSize)
	varuint64.Put(t.buf[varuint64.MaxSize-n:], i-varuint64.MaxSize)

	receivedCh := make(chan error, 1)
	select {
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case <-t.client.doneCh:
		if ctx.Err() != nil {
			return errors.WithStack(ctx.Err())
		}
		return errors.New("client closed")
	case <-time.After(t.client.config.BroadcastTimeout):
		return ErrBroadcastTimeout
	case t.client.txCh <- tx{
		ID:           txID,
		Tx:           t.buf[varuint64.MaxSize-n : i],
		ReceivedCh:   receivedCh,
		PreviousTxID: previousTxID,
	}:
	}

	select {
	case <-ctx.Done():
		err = errors.WithStack(ctx.Err())
	case <-t.client.doneCh:
		if ctx.Err() != nil {
			err = errors.WithStack(ctx.Err())
		} else {
			err = errors.New("client closed")
		}
	case <-time.After(t.client.config.AwaitTimeout):
		err = ErrAwaitTimeout
	case err = <-receivedCh:
	}

	if err != nil {
		t.buf = t.buf[i:]
	}

	return err
}

func txRecover(err *error) {
	if r := recover(); r != nil {
		if e, ok := r.(error); ok {
			*err = e
			return
		}
		*err = errors.Errorf("transaction panicked: %s", r)
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
	id := o.(reflect.Value).Elem().Field(idi.index).Convert(idType).Interface().(types.ID)
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
