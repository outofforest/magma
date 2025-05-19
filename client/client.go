package client

import (
	"context"
	"encoding/binary"
	"reflect"
	"sync"
	"time"
	"unsafe"

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
	memdbid "github.com/outofforest/memdb/id"
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

var idType = reflect.TypeOf(memdb.ID{})

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
	TriggerFunc      func(ctx context.Context, v *View) error
}

// New creates new magma client.
func New(config Config) (*Client, error) {
	objectTypes := config.Marshaller.Messages()
	if len(objectTypes) == 0 {
		return nil, errors.New("no object types provided")
	}

	dbIndexes := make([][]memdb.Index, 0, len(objectTypes))

	revisionType := reflect.TypeOf(types.Revision(0))

	byID := map[uint64]typeInfo{}
	byType := map[reflect.Type]typeInfo{}
	for tableID, o := range objectTypes {
		t := reflect.TypeOf(o)

		idF, exists := t.FieldByName("ID")
		if !exists {
			return nil, errors.Errorf("object %s has no ID field", t)
		}
		if !idF.Type.ConvertibleTo(idType) {
			return nil, errors.Errorf("object's %s ID field must be of type %s", t, idType)
		}
		if idF.Index[0] != 0 || idF.Offset != 0 {
			return nil, errors.Errorf("id must be the first field in type %s", t)
		}
		revisionF, exists := t.FieldByName("Revision")
		if !exists {
			return nil, errors.Errorf("object %s has no Revision field", t)
		}
		if revisionF.Type != revisionType {
			return nil, errors.Errorf("object's %s Revision field must be of type %s", t, revisionType)
		}
		if revisionF.Index[0] != 1 || revisionF.Offset != memdbid.Length {
			return nil, errors.Errorf("revision must be the second field in type %d", t)
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

		table := []memdb.Index{}
		for _, index := range config.Indices {
			if index.Type() == t {
				table = append(table, index)
			}
		}
		dbIndexes = append(dbIndexes, table)
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
		bufSize:          10 * (config.MaxMessageSize + varuint64.MaxSize),
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

	for {
		err := resonance.RunClient(ctx, c.config.PeerAddress, resonance.Config{MaxMessageSize: c.config.MaxMessageSize},
			func(ctx context.Context, conn *resonance.Connection) error {
				conn.BufferReads()
				conn.BufferWrites()

				if err := conn.SendProton(&c2p.InitRequest{
					PartitionID: c.config.PartitionID,
					NextIndex:   c.nextIndex,
				}, cMarshaller); err != nil {
					return errors.WithStack(err)
				}

				msg, err := conn.ReceiveProton(cMarshaller)
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

						var commitCh chan struct{}
						var tx *memdb.Txn
						defer func() {
							if tx != nil {
								tx.Commit()
							}
						}()

						for {
							m, err := conn.ReceiveProton(cMarshaller)
							if err != nil {
								return err
							}

							switch msg := m.(type) {
							case *gossipwire.StartLogStream:
								if tx == nil {
									tx = c.db.Txn(true)
									commitCh = make(chan struct{})
								}
								var length uint64
								for length < msg.Length {
									txRaw, err := conn.ReceiveRawBytes()
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
								commitCh = nil

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
func (c *Client) NewTransactor() *Transactor {
	return &Transactor{
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
	if err != nil && !errors.Is(err, ErrOutdatedTx) {
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

		o, err := tx.First(typeDef.TableID, memdbid.IndexID, entityMeta.ID)
		if err != nil {
			return errors.WithStack(err)
		}

		oV := reflect.New(typeDef.Type)
		if o == nil {
			copyMetaToEntity(oV, entityMeta)
		} else {
			oV2 := *o
			if entityMeta.Revision <= revisionFromEntity(oV2) {
				return ErrOutdatedTx
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
type Transactor struct {
	client       *Client
	changes      map[memdb.ID]change
	previousTxID memdb.ID
	buf          []byte
}

// Tx creates and broadcasts transaction to the magma network.
func (t *Transactor) Tx(ctx context.Context, txF func(tx *Tx) error) error {
	tx, i, err := t.prepareTx(txF)
	if err != nil {
		return err
	}
	if tx.Tx == nil {
		return nil
	}

	err = t.broadcastAndAwaitTx(ctx, tx)
	if err != nil {
		t.buf = t.buf[i:]
	}
	return err
}

func (t *Transactor) prepareTx(txF func(tx *Tx) error) (tx txEnvelope, i uint64, retErr error) {
	defer clear(t.changes)

	t.client.db.AwaitTxn()

	pendingTx := &Tx{
		View: &View{
			// By taking a snapshot, we don't block the main DB from processing incoming changes.
			tx:     t.client.db.Snapshot().Txn(true),
			byType: t.client.byType,
		},
		changes: t.changes,
	}

	defer pendingTx.tx.Abort()
	defer txRecover(&retErr)

	if err := txF(pendingTx); err != nil {
		return txEnvelope{}, 0, err
	}

	pendingTx.tx.Abort()

	if len(pendingTx.changes) == 0 {
		return txEnvelope{}, 0, nil
	}

	if uint64(len(t.buf)) < t.client.config.MaxMessageSize {
		t.buf = make([]byte, t.client.bufSize)
	}

	i = uint64(varuint64.MaxSize)

	txID := memdb.NewID[memdb.ID]()
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
		return txEnvelope{}, 0, err
	}
	i += metaSize

	for _, ch := range pendingTx.changes {
		vv := ch.New.Elem()
		typeDef, exists := t.client.byType[vv.Type()]
		if !exists {
			return txEnvelope{}, 0, errors.Errorf("unknown type %s", vv.Type())
		}

		unsafeID := unsafeIDFromEntity(*ch.New)

		var oldV reflect.Value
		if ch.Old == nil {
			oldV = reflect.New(typeDef.Type)
			setIDInEntity(oldV, (*memdb.ID)(unsafe.Pointer(&unsafeID[0])))
		} else {
			oldV = *ch.Old
		}

		entityMeta := &wire.EntityMetadata{
			MessageID: typeDef.MsgID,
		}
		copyMetaFromEntity(entityMeta, oldV)
		entityMeta.Revision++

		_, entitySize, err := t.client.metaM.Marshal(entityMeta, t.buf[i:])
		if err != nil {
			return txEnvelope{}, 0, err
		}
		i += entitySize

		_, msgSize, err := t.client.config.Marshaller.MakePatch(ch.New.Interface(), oldV.Interface(), t.buf[i:])
		if err != nil {
			return txEnvelope{}, 0, err
		}
		i += msgSize

		if i+1 > t.client.config.MaxMessageSize {
			return txEnvelope{}, 0,
				errors.Errorf("tx size %d exceeds allowed maximum %d", i, t.client.config.MaxMessageSize)
		}
	}

	n := varuint64.Size(i - varuint64.MaxSize)
	varuint64.Put(t.buf[varuint64.MaxSize-n:], i-varuint64.MaxSize)

	return txEnvelope{
		ID:           txID,
		Tx:           t.buf[varuint64.MaxSize-n : i],
		ReceivedCh:   make(chan any, 1),
		PreviousTxID: previousTxID,
	}, 0, nil
}

func (t *Transactor) broadcastAndAwaitTx(ctx context.Context, tx txEnvelope) error {
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
		return ErrAwaitTimeout
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
