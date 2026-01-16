package client

import (
	"context"
	"encoding/binary"
	"reflect"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/zeebo/xxh3"
	"go.uber.org/zap"

	"github.com/outofforest/logger"
	"github.com/outofforest/magma/client/wire"
	gossipwire "github.com/outofforest/magma/gossip/wire"
	"github.com/outofforest/magma/gossip/wire/c2p"
	"github.com/outofforest/magma/state/repository/format"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/memdb"
	"github.com/outofforest/parallel"
	"github.com/outofforest/proton"
	"github.com/outofforest/resonance"
	"github.com/outofforest/varuint64"
)

var (
	// ErrTxTooBig means that adding new entity to transaction caused it to exceed the max size limit.
	ErrTxTooBig = errors.New("transaction exceeds the max size limit")

	// ErrTxFailure is a general error meaning that transaction failed.
	ErrTxFailure = errors.New("transaction failed")

	// ErrTxTimeout means timeout has occurred when sending transaction.
	ErrTxTimeout = errors.Wrap(ErrTxFailure, "transaction timeout")

	// ErrTxBroadcastTimeout means that client was not able to broadcast the transaction before timeout.
	ErrTxBroadcastTimeout = errors.Wrap(ErrTxTimeout, "broadcast timeout")

	// ErrTxAwaitTimeout means that client hasn't received broadcasted transaction before timeout.
	ErrTxAwaitTimeout = errors.Wrap(ErrTxTimeout, "await timeout")

	// ErrTxOutdatedTx means that awaited transaction is outdated and hasn't been applied.
	ErrTxOutdatedTx = errors.Wrap(ErrTxFailure, "outdated transaction")
)

var idType = reflect.TypeFor[memdb.ID]()

// TriggerFunc defines function triggered after applying transactions.
type TriggerFunc func(ctx context.Context, v *View) error

// Config is the configuration of magma client.
type Config struct {
	Service          string
	PeerAddress      string
	PartitionID      types.PartitionID
	MaxMessageSize   uint64
	BroadcastTimeout time.Duration
	AwaitTimeout     time.Duration
	Marshaller       proton.Marshaller
	Indices          []memdb.Index
	TriggerFunc      TriggerFunc
}

// New creates new magma client.
func New(config Config) (*Client, error) {
	objectTypes := config.Marshaller.Messages()
	if len(objectTypes) == 0 {
		return nil, errors.New("no object types provided")
	}

	dbIndexes := make([][]memdb.Index, 0, len(objectTypes))

	byID := map[uint64]typeInfo{}
	byType := map[reflect.Type]typeInfo{}
	for tableID, o := range objectTypes {
		t := reflect.TypeOf(o)

		if err := validateType(t); err != nil {
			return nil, err
		}

		if _, exists := byType[t]; exists {
			return nil, errors.Errorf("double registration of object %s", t)
		}

		msgID, err := config.Marshaller.ID(reflect.New(t).Interface())
		if err != nil {
			return nil, err
		}

		info := typeInfo{
			Type:    t,
			MsgID:   msgID,
			TableID: uint64(tableID),
		}
		byID[msgID] = info
		byType[t] = info

		dbIndexes = buildDBIndexesForType(dbIndexes, config.Indices, t)
	}

	db, err := memdb.NewMemDB(dbIndexes)
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
		txCh:             make(chan txEnvelope),
		metaM:            metaM,
		doneCh:           make(chan struct{}),
		bufSize:          max(10*(config.MaxMessageSize+2*varuint64.MaxSize), 4*1024),
		byID:             byID,
		byType:           byType,
		db:               db,
		metaID:           metaID,
		entityMetadataID: entityMetadataID,
		readyCh:          make(chan struct{}),
		awaitedTxs:       map[memdb.ID]chan<- any{},
	}, nil
}

type pendingEntity struct {
	TableID uint64
	Entity  *reflect.Value
}

// Client connects to magma network, receives log updates and sends transactions.
type Client struct {
	config Config
	txCh   chan txEnvelope
	metaM  proton.Marshaller
	doneCh chan struct{}

	byID   map[uint64]typeInfo
	byType map[reflect.Type]typeInfo
	db     *memdb.MemDB

	bufSize          uint64
	metaID           uint64
	entityMetadataID uint64

	previousChecksum uint64
	nextIndex        types.Index
	readyCh          chan struct{}
	firstHotEnd      bool

	mu         sync.Mutex
	awaitedTxs map[memdb.ID]chan<- any

	pendingEntities []pendingEntity
}

// Run runs client.
func (c *Client) Run(ctx context.Context) error {
	defer close(c.doneCh)

	log := logger.Get(ctx)

	var awaitedTxsToClean []memdb.ID

	cMarshaller := c2p.NewMarshaller()
	commitCh := make(chan struct{})

	for {
		err := resonance.RunClient(ctx, c.config.PeerAddress, resonance.Config{MaxMessageSize: c.config.MaxMessageSize},
			func(ctx context.Context, conn *resonance.Connection) error {
				conn.BufferReads()
				conn.BufferWrites()

				if _, err := conn.SendProton(&c2p.InitRequest{
					PartitionID: c.config.PartitionID,
					NextIndex:   c.nextIndex,
				}, cMarshaller); err != nil {
					return errors.WithStack(err)
				}

				msg, _, err := conn.ReceiveProton(cMarshaller)
				if err != nil {
					return err
				}
				if _, ok := msg.(*c2p.InitResponse); !ok {
					return errors.Errorf("expected init response, got: %T", msg)
				}

				return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
					triggerCh := make(chan struct{}, 1)

					spawn("receiver", parallel.Fail, func(ctx context.Context) error {
						defer close(triggerCh)

						var tx *memdb.Txn
						defer func() {
							if tx != nil {
								tx.Commit()
							}
						}()

						for {
							m, _, err := conn.ReceiveProton(cMarshaller)
							if err != nil {
								return err
							}

							switch msg := m.(type) {
							case *gossipwire.StartLogStream:
								if tx == nil {
									tx = c.db.Txn(true)
								}
								var length uint64
								for length < msg.Length {
									txRaw, _, err := conn.ReceiveRawBytes()
									if err != nil {
										return err
									}

									length += uint64(len(txRaw))

									checksum, err := c.applyTx(commitCh, c.previousChecksum, tx, txRaw)
									if err != nil {
										tx.Abort()
										return err
									}
									c.previousChecksum = checksum
									c.nextIndex += types.Index(len(txRaw))
								}
							case *gossipwire.HotEnd:
								if tx == nil {
									continue
								}

								tx.Commit()
								close(commitCh)

								tx = nil
								commitCh = make(chan struct{})

								c.applyHotEnd(triggerCh)
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
								c.mu.Lock()
								delete(c.awaitedTxs, tx.PreviousTxID)
								c.awaitedTxs[tx.ID] = tx.ReceivedCh
								c.mu.Unlock()

								if _, err := conn.SendRawBytes(tx.Tx); err != nil {
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
							case <-time.After(c.config.AwaitTimeout):
								c.mu.Lock()
								for _, id := range awaitedTxsToClean {
									delete(c.awaitedTxs, id)
								}
								awaitedTxsToClean = make([]memdb.ID, 0, len(c.awaitedTxs))
								for id := range c.awaitedTxs {
									awaitedTxsToClean = append(awaitedTxsToClean, id)
								}
								c.mu.Unlock()
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
func (c *Client) NewTransactor() Transactor {
	return &transactor{
		client:  c,
		changes: map[memdb.ID]change{},
	}
}

func (c *Client) applyHotEnd(triggerCh chan struct{}) {
	if !c.firstHotEnd {
		c.firstHotEnd = true
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
}

func (c *Client) applyTx(
	commitCh <-chan struct{},
	previousChecksum uint64,
	tx *memdb.Txn,
	txRaw []byte,
) (uint64, error) {
	txLen, n := varuint64.Parse(txRaw)

	if txLen < format.ChecksumSize {
		return 0, errors.New("unexpected tx size")
	}

	i := len(txRaw) - format.ChecksumSize
	checksum := xxh3.HashSeed(txRaw[:i], previousChecksum)
	if binary.LittleEndian.Uint64(txRaw[i:]) != checksum {
		return 0, errors.New("tx checksum mismatch")
	}

	txLen -= format.ChecksumSize
	txRaw = txRaw[n : n+txLen]

	if _, n2 := varuint64.Parse(txRaw); n2 == txLen {
		// This is a term mark. Ignore.
		return checksum, nil
	}

	metaID, n := varuint64.Parse(txRaw)
	metaAny, metaSize, err := c.metaM.Unmarshal(metaID, txRaw[n:])
	if err != nil {
		return 0, err
	}
	meta := metaAny.(*wire.TxMetadata)

	err = c.storeTx(meta.EntityMetadataID, tx, txRaw[n+metaSize:])
	if err != nil && !errors.Is(err, ErrTxOutdatedTx) {
		return 0, err
	}

	c.mu.Lock()
	receivedCh := c.awaitedTxs[meta.ID]
	delete(c.awaitedTxs, meta.ID)
	c.mu.Unlock()

	if receivedCh != nil {
		if err != nil {
			receivedCh <- err
		} else {
			receivedCh <- commitCh
		}
	}

	return checksum, nil
}

func (c *Client) storeTx(entityMetaID uint64, tx *memdb.Txn, txRaw []byte) (retErr error) {
	c.pendingEntities = c.pendingEntities[:0]
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

		o, err := tx.First(typeDef.TableID, memdb.IDIndexID, entityMeta.ID)
		if err != nil {
			return errors.WithStack(err)
		}

		oV := reflect.New(typeDef.Type)
		if o == nil {
			copyMetaToEntity(oV, entityMeta)
		} else {
			oV2 := *o
			if entityMeta.Revision <= revisionFromEntity(oV2) {
				return ErrTxOutdatedTx
			}
			oV.Elem().Set(oV2.Elem())
			setRevisionInEntity(oV, &entityMeta.Revision)
		}

		msgSize, err := c.config.Marshaller.ApplyPatch(oV.Interface(), txRaw[entityMetaSize:])
		if err != nil {
			return err
		}

		c.pendingEntities = append(c.pendingEntities, pendingEntity{TableID: typeDef.TableID, Entity: &oV})
		txRaw = txRaw[entityMetaSize+msgSize:]
	}

	for _, e := range c.pendingEntities {
		if _, err := tx.Insert(e.TableID, e.Entity); err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

type txEnvelope struct {
	ID           memdb.ID
	Tx           []byte
	ReceivedCh   chan any
	PreviousTxID memdb.ID
}

// Transactor builds and broadcasts transactions.
type Transactor interface {
	Tx(ctx context.Context, txF func(tx *Tx) error) error
}

type transactor struct {
	client       *Client
	changes      map[memdb.ID]change
	previousTxID memdb.ID
	buf          []byte
}

// Tx creates and broadcasts transaction to the magma network.
func (t *transactor) Tx(ctx context.Context, txF func(tx *Tx) error) error {
	tx, usedSize, err := t.prepareTx(txF)
	if err != nil {
		return err
	}
	if tx.Tx == nil {
		return nil
	}

	if err = t.broadcastAndAwaitTx(ctx, tx); err != nil {
		t.buf = t.buf[usedSize:]
	}
	return err
}

func (t *transactor) prepareTx(txF func(tx *Tx) error) (retTx txEnvelope, retUsedSize uint64, retErr error) {
	defer clear(t.changes)

	t.client.db.AwaitTxn()

	if uint64(len(t.buf)) < t.client.config.MaxMessageSize {
		t.buf = make([]byte, t.client.bufSize)
	}

	txID := memdb.NewID[memdb.ID]()
	metaSize := uint64(varuint64.MaxSize)

	meta := &wire.TxMetadata{
		ID:               txID,
		Time:             time.Now(),
		Service:          t.client.config.Service,
		EntityMetadataID: t.client.entityMetadataID,
	}

	metaSize += varuint64.Put(t.buf[metaSize:], t.client.metaID)
	_, metaMsgSize, err := t.client.metaM.Marshal(meta, t.buf[metaSize:])
	if err != nil {
		return txEnvelope{}, 0, err
	}
	metaSize += metaMsgSize

	pendingTx := &Tx{
		// By taking a snapshot, we don't block the main DB from processing incoming changes.
		db:     t.client.db.Snapshot(),
		client: t.client,
		// Checksum size is subtracted here because later we receive the transaction back with checksum included,
		// so the checksum must fit in max message size.
		buf:     t.buf[metaSize : metaSize+t.client.config.MaxMessageSize-metaMsgSize-format.ChecksumSize],
		changes: t.changes,
	}

	defer txRecover(&retErr)

	if err := txF(pendingTx); err != nil {
		return txEnvelope{}, 0, err
	}

	if pendingTx.size == 0 {
		return txEnvelope{}, 0, nil
	}

	previousTxID := t.previousTxID
	t.previousTxID = txID

	txSize := metaSize + pendingTx.size - varuint64.MaxSize
	n := varuint64.Size(txSize)
	start := varuint64.MaxSize - n
	end := start + n + txSize
	varuint64.Put(t.buf[start:], txSize)

	return txEnvelope{
		ID:           txID,
		Tx:           t.buf[start:end],
		ReceivedCh:   make(chan any, 1),
		PreviousTxID: previousTxID,
	}, end, nil
}

func (t *transactor) broadcastAndAwaitTx(ctx context.Context, tx txEnvelope) error {
	select {
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case <-t.client.doneCh:
		if ctx.Err() != nil {
			return errors.WithStack(ctx.Err())
		}
		return errors.New("client closed")
	case <-time.After(t.client.config.BroadcastTimeout):
		return ErrTxBroadcastTimeout
	case t.client.txCh <- tx:
	}

	select {
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case <-t.client.doneCh:
		if ctx.Err() != nil {
			return errors.WithStack(ctx.Err())
		}
		return errors.New("client closed")
	case <-time.After(t.client.config.AwaitTimeout):
		return ErrTxAwaitTimeout
	case result := <-tx.ReceivedCh:
		switch r := result.(type) {
		case error:
			return r
		case <-chan struct{}:
			select {
			case <-ctx.Done():
				return errors.WithStack(ctx.Err())
			case <-t.client.doneCh:
				if ctx.Err() != nil {
					return errors.WithStack(ctx.Err())
				}
				return errors.New("client closed")
			case <-r:
				return nil
			}
		default:
			panic("impossible situation")
		}
	}
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

type typeInfo struct {
	Type    reflect.Type
	MsgID   uint64
	TableID uint64
}

var revisionType = reflect.TypeFor[types.Revision]()

func validateType(t reflect.Type) error {
	idF, exists := t.FieldByName("ID")
	if !exists {
		return errors.Errorf("object %s has no ID field", t)
	}
	if !idF.Type.ConvertibleTo(idType) {
		return errors.Errorf("object's %s ID field must be of type %s", t, idType)
	}
	if idF.Index[0] != 0 || idF.Offset != 0 {
		return errors.Errorf("id must be the first field in type %s", t)
	}
	revisionF, exists := t.FieldByName("Revision")
	if !exists {
		return errors.Errorf("object %s has no Revision field", t)
	}
	if revisionF.Type != revisionType {
		return errors.Errorf("object's %s Revision field must be of type %s", t, revisionType)
	}
	if revisionF.Index[0] != 1 || revisionF.Offset != memdb.IDLength {
		return errors.Errorf("revision must be the second field in type %d", t)
	}

	return nil
}

func buildDBIndexesForType(dbIndexes [][]memdb.Index, indices []memdb.Index, t reflect.Type) [][]memdb.Index {
	table := []memdb.Index{}
	for _, index := range indices {
		if index.Type() == t {
			table = append(table, index)
		}
	}
	return append(dbIndexes, table)
}
