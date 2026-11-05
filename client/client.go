package client

import (
	"context"
	"reflect"
	"sync"
	"time"
	"unsafe"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/outofforest/logger"
	"github.com/outofforest/magma/client/db"
	"github.com/outofforest/magma/client/wire"
	"github.com/outofforest/magma/config"
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
	_ ReadClient    = &Client{}
	_ WriteClient   = &Client{}
	_ db.Transactor = &transactor{}
)

var defaultPointer unsafe.Pointer

// TriggerFunc defines function triggered after applying transactions.
type TriggerFunc func(ctx context.Context, v *db.View, types map[reflect.Type]struct{}) error

// Config is the configuration of magma client.
type Config struct {
	CA               *resonance.CA
	Service          string
	PeerAddresses    []string
	Partition        config.PartitionConfig
	MaxMessageSize   uint64
	BroadcastTimeout time.Duration
	AwaitTimeout     time.Duration
	Indices          []memdb.Index
	TriggerFunc      TriggerFunc
}

// Client connects to magma network, receives log updates and sends transactions.
type Client struct {
	config  Config
	metaM   proton.Marshaller
	m       proton.Marshaller
	applier *db.Applier
	txCh    chan txEnvelope
	doneCh  chan struct{}

	db *memdb.MemDB

	bufSize          uint64
	metaID           uint64
	entityMetadataID uint64

	previousChecksum uint64
	nextIndex        types.Index
	readyCh          chan struct{}
	firstHotEnd      bool

	mu         sync.Mutex
	awaitedTxs map[memdb.ID]chan<- any
}

// New creates new magma client.
func New(config Config) (*Client, error) {
	if len(config.PeerAddresses) == 0 {
		return nil, errors.New("no peer addresses has been provided")
	}

	m := config.Partition.InitialMarshaller
	if len(config.Partition.Upgrades) > 0 {
		m = config.Partition.Upgrades[len(config.Partition.Upgrades)-1].Marshaller
	}

	dbConfig, err := db.NewDBConfig(m, config.Indices)
	if err != nil {
		return nil, err
	}

	memDB, err := memdb.NewMemDB(dbConfig)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	metaM := wire.NewMarshaller()

	applier, err := db.NewApplier(metaM, m)
	if err != nil {
		return nil, err
	}

	metaID, err := metaM.ID(&wire.TxMetadata{})
	if err != nil {
		return nil, err
	}

	entityMetadataID, err := metaM.ID(&wire.TxEntityMetadata{})
	if err != nil {
		return nil, err
	}

	return &Client{
		config:           config,
		metaM:            metaM,
		m:                m,
		applier:          applier,
		txCh:             make(chan txEnvelope),
		doneCh:           make(chan struct{}),
		bufSize:          max(10*(config.MaxMessageSize+2*varuint64.MaxSize), 4*1024),
		db:               memDB,
		metaID:           metaID,
		entityMetadataID: entityMetadataID,
		readyCh:          make(chan struct{}),
		awaitedTxs:       map[memdb.ID]chan<- any{},
	}, nil
}

type trigger struct {
	View         *db.View
	UpdatedTypes map[reflect.Type]struct{}
}

// Run runs client.
//
//nolint:gocyclo
func (c *Client) Run(ctx context.Context) error {
	defer close(c.doneCh)

	log := logger.Get(ctx)

	var awaitedTxsToClean []memdb.ID

	cMarshaller := c2p.NewMarshaller()
	commitCh := make(chan struct{})
	var peerIndex uint64
	var failure error

	for {
		tryPeerIndex := peerIndex
		peerIndex = (peerIndex + 1) % uint64(len(c.config.PeerAddresses))
		err := resonance.RunClient(ctx, c.config.PeerAddresses[tryPeerIndex], resonance.Config{
			CA:             c.config.CA,
			MaxMessageSize: c.config.MaxMessageSize,
		}, func(ctx context.Context, conn *resonance.Connection) error {
			peerIndex = 0

			conn.BufferReads()
			conn.BufferWrites()

			if _, err := conn.SendProton(&c2p.InitRequest{
				PartitionID: c.config.Partition.ID,
				Version:     types.Version(len(c.config.Partition.Upgrades)),
				NextIndex:   c.nextIndex,
			}, cMarshaller); err != nil {
				return errors.WithStack(err)
			}

			msg, _, err := conn.ReceiveProton(cMarshaller)
			if err != nil {
				return err
			}

			initResponse, ok := msg.(*c2p.InitResponse)
			if !ok {
				failure = errors.Errorf("expected init response, got: %T", msg)
				return failure
			}
			if initResponse.Error != "" {
				failure = errors.Errorf("protocol error: %s", initResponse.Error)
				return failure
			}

			return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
				triggerCh := make(chan trigger, 1)

				spawn("receiver", parallel.Fail, func(ctx context.Context) error {
					defer close(triggerCh)

					var tx *memdb.Txn
					updatedTypes := map[reflect.Type]struct{}{}

					if initResponse.InitSize > 0 {
						tx = c.db.Txn(true)

						var err error
						c.previousChecksum, err = c.applier.ApplyInit(conn, initResponse.InitSize, tx, updatedTypes)
						if err != nil {
							return err
						}
					}

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

								checksum, err := c.applyTx(commitCh, c.previousChecksum, tx, txRaw, updatedTypes)
								if err != nil {
									failure = err
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

							commitCh = make(chan struct{})

							c.applyHotEnd(triggerCh, c.View(), updatedTypes)
							tx = nil
							updatedTypes = map[reflect.Type]struct{}{}
						default:
							failure = errors.Errorf("unexpected message %T", msg)
							return failure
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
						for t := range triggerCh {
							if err := c.config.TriggerFunc(ctx, t.View, t.UpdatedTypes); err != nil {
								return err
							}
						}
						return errors.WithStack(ctx.Err())
					})
				}

				return nil
			})
		})

		if failure != nil {
			return failure
		}

		if ctx.Err() != nil {
			return errors.WithStack(ctx.Err())
		}

		log.Error("Connection failed, reconnecting after 2 seconds", zap.Error(err))

		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		case <-time.After(2 * time.Second):
		}
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
func (c *Client) View() *db.View {
	return c.applier.View(c.db.Txn(true))
}

// NewTransactor creates new transactor.
func (c *Client) NewTransactor() db.Transactor {
	return &transactor{
		client:  c,
		changes: map[changeID]change{},
	}
}

func (c *Client) applyHotEnd(triggerCh chan trigger, view *db.View, updatedTypes map[reflect.Type]struct{}) {
	if !c.firstHotEnd {
		c.firstHotEnd = true
		close(c.readyCh)
	}

	if c.config.TriggerFunc != nil {
		if len(triggerCh) > 0 {
			select {
			case oldT := <-triggerCh:
				for id := range oldT.UpdatedTypes {
					updatedTypes[id] = struct{}{}
				}
			default:
			}
		}
		triggerCh <- trigger{
			View:         view,
			UpdatedTypes: updatedTypes,
		}
	}
}

var emptyTxID = memdb.ID{}

func (c *Client) applyTx(
	commitCh <-chan struct{},
	previousChecksum uint64,
	tx *memdb.Txn,
	txRaw []byte,
	updatedTypes map[reflect.Type]struct{},
) (uint64, error) {
	txID, checksum, err := c.applier.ApplyTx(previousChecksum, tx, txRaw, updatedTypes)
	switch {
	case err == nil:
		if txID == emptyTxID {
			return checksum, nil
		}
	case !errors.Is(err, db.ErrTxOutdatedTx):
		return 0, err
	}

	c.mu.Lock()
	receivedCh := c.awaitedTxs[txID]
	delete(c.awaitedTxs, txID)
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

type txEnvelope struct {
	ID           memdb.ID
	Tx           []byte
	ReceivedCh   chan any
	PreviousTxID memdb.ID
}

type transactor struct {
	client       *Client
	changes      map[changeID]change
	previousTxID memdb.ID
	buf          []byte
}

// Tx creates and broadcasts transaction to the magma network.
func (t *transactor) Tx(ctx context.Context, txF func(tx db.Tx) error) error {
	tx, usedSize, err := t.prepareTx(memdb.NewID[memdb.ID](), time.Now(), txF)
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

func (t *transactor) prepareTx(
	txID memdb.ID,
	txTime time.Time,
	txF func(tx db.Tx) error,
) (
	retTx txEnvelope,
	retUsedSize uint64,
	retErr error,
) {
	defer clear(t.changes)

	if uint64(len(t.buf)) < t.client.config.MaxMessageSize {
		t.buf = make([]byte, t.client.bufSize)
	}

	metaSize := uint64(varuint64.MaxSize)

	meta := &wire.TxMetadata{
		ID:               txID,
		Time:             txTime,
		Service:          t.client.config.Service,
		EntityMetadataID: t.client.entityMetadataID,
	}

	metaSize += varuint64.Put(t.buf[metaSize:], t.client.metaID)
	_, metaMsgSize, err := t.client.metaM.Marshal(meta, t.buf[metaSize:])
	if err != nil {
		return txEnvelope{}, 0, err
	}
	metaSize += metaMsgSize

	pendingTx := &tx{
		// By taking a snapshot, we don't block the main DB from processing incoming changes.
		txn:    t.client.db.Txn(false),
		client: t.client,
		// PreviousChecksum size is subtracted here because later we receive the transaction back with checksum included,
		// so the checksum must fit in max message size.
		buf:     t.buf[metaSize : metaSize+t.client.config.MaxMessageSize-metaMsgSize-format.ChecksumSize],
		changes: t.changes,
	}

	defer txRecover(&retErr)

	if err := txF(pendingTx); err != nil {
		return txEnvelope{}, 0, err
	}

	if pendingTx.numOfObjects == 0 {
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
		return db.ErrTxBroadcastTimeout
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
		return db.ErrTxAwaitTimeout
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

type change struct {
	OldValue   unsafe.Pointer
	StartIndex uint64
	EndIndex   uint64
	Added      bool
}

type changeID struct {
	ID    memdb.ID
	MsgID uint64
}

var _ db.Tx = &tx{}

type tx struct {
	client       *Client
	txn          *memdb.Txn
	buf          []byte
	size         uint64
	numOfObjects uint64
	changes      map[changeID]change
}

// View returns non-persistent view of the DB.
func (tx *tx) View() *db.View {
	return tx.client.applier.View(tx.txn.Txn(true))
}

// Set sets object in transaction. This function includes the object in tx even if patch is empty.
// This is done to detect possible conflicts with other transactions. Use this function if atomicity
// is required (most of the cases). Compare to SoftSet below.
func (tx *tx) Set(o any) error {
	return tx.set(o, false)
}

// SoftSet sets object in transaction. It does it only if the object patch is not empty.
// This function is good to use when you don't care about atomicity in the tx.
// It might happen that SoftSet doesn't include the object in tx due to empty patch, but somewhere
// else conflicting transaction is created. This conflict is not detected because we haven't included object
// with incremented revision.
// Good scenario to use SoftSet is loading batches of unrelated object where conflicts don't matter.
func (tx *tx) SoftSet(o any) error {
	return tx.set(o, true)
}

func (tx *tx) set(o any, isSoftSet bool) error {
	dbTx := tx.txn.Txn(true)

	id, typeDef, oldValue, newValue := tx.client.applier.Insert(dbTx, o)

	chID := changeID{
		ID:    id,
		MsgID: typeDef.MsgID,
	}
	chg, chgExists := tx.changes[chID]

	//nolint:nestif
	if chgExists {
		if chg.EndIndex > 0 {
			if chg.EndIndex == tx.size {
				tx.size = chg.StartIndex
			} else {
				copy(tx.buf[chg.StartIndex:], tx.buf[chg.EndIndex:tx.size])
				sizeUpdate := chg.EndIndex - chg.StartIndex
				for i, ch := range tx.changes {
					if ch.StartIndex > chg.StartIndex {
						ch.StartIndex -= sizeUpdate
						ch.EndIndex -= sizeUpdate
					}
					tx.changes[i] = ch
				}
				tx.size -= sizeUpdate
			}

			if chg.Added {
				tx.numOfObjects--
			}

			// This is done to know later that there is nothing to move in the tx if set fails.
			chg.StartIndex = 0
			chg.EndIndex = 0
			chg.Added = false
			tx.changes[chID] = chg
		}
	} else {
		chg = change{
			OldValue: oldValue,
		}
	}

	newValueRef := reflect.NewAt(typeDef.Type, newValue)
	unsafeID := db.UnsafeIDFromEntity(newValue)
	var oldV reflect.Value
	var isNeeded bool
	if chg.OldValue == defaultPointer {
		oldV = reflect.New(typeDef.Type)
		db.SetIDInEntity(oldV.UnsafePointer(), (*memdb.ID)(unsafe.Pointer(&unsafeID[0])))
		isNeeded = true
	} else {
		oldV = reflect.NewAt(typeDef.Type, chg.OldValue)

		var err error
		isNeeded, err = tx.client.m.IsPatchNeeded(newValueRef.Interface(), oldV.Interface())
		if err != nil {
			return err
		}
		if isSoftSet && !isNeeded {
			return nil
		}
	}

	entityMeta := &wire.TxEntityMetadata{
		MessageID: typeDef.MsgID,
	}
	db.CopyMetaFromEntity(entityMeta, oldV.UnsafePointer())
	entityMeta.Revision++

	chg.StartIndex = tx.size
	chg.EndIndex = chg.StartIndex
	_, entitySize, err := tx.client.metaM.Marshal(entityMeta, tx.buf[chg.EndIndex:])

	switch {
	case err == nil:
	case errors.Is(err, proton.ErrBufferFailure):
		return db.ErrTxTooBig
	default:
		return err
	}

	chg.EndIndex += entitySize

	_, msgSize, err := tx.client.m.MakePatch(newValueRef.Interface(), oldV.Interface(),
		tx.buf[chg.EndIndex:])

	switch {
	case err == nil:
	case errors.Is(err, proton.ErrBufferFailure):
		return db.ErrTxTooBig
	default:
		return err
	}

	if isNeeded {
		chg.Added = true
		tx.numOfObjects++
	}

	chg.EndIndex += msgSize
	tx.size = chg.EndIndex
	tx.changes[chID] = chg

	dbTx.Commit()

	return nil
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
