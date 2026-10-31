package client

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/pkg/errors"
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
	_ ReadClient  = &Client{}
	_ WriteClient = &Client{}
	_ Transactor  = &transactor{}
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
type TriggerFunc func(ctx context.Context, v *View, types map[reflect.Type]struct{}) error

// Config is the configuration of magma client.
type Config struct {
	CA               *resonance.CA
	Service          string
	PeerAddresses    []string
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
	if len(config.PeerAddresses) == 0 {
		return nil, errors.New("no peer addresses has been provided")
	}

	dbConfig, err := NewDBConfig(config.Marshaller, config.Indices)
	if err != nil {
		return nil, err
	}

	db, err := memdb.NewMemDB(dbConfig)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	applier, err := NewApplier(config.Marshaller)
	if err != nil {
		return nil, err
	}

	metaID, err := applier.metaM.ID(&wire.TxMetadata{})
	if err != nil {
		return nil, err
	}

	entityMetadataID, err := applier.metaM.ID(&wire.EntityMetadata{})
	if err != nil {
		return nil, err
	}

	return &Client{
		config:           config,
		applier:          applier,
		txCh:             make(chan txEnvelope),
		doneCh:           make(chan struct{}),
		bufSize:          max(10*(config.MaxMessageSize+2*varuint64.MaxSize), 4*1024),
		db:               db,
		metaID:           metaID,
		entityMetadataID: entityMetadataID,
		readyCh:          make(chan struct{}),
		awaitedTxs:       map[memdb.ID]chan<- any{},
	}, nil
}

// Client connects to magma network, receives log updates and sends transactions.
type Client struct {
	config  Config
	applier *Applier
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

type trigger struct {
	View         *View
	UpdatedTypes map[reflect.Type]struct{}
}

// Run runs client.
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
				PartitionID: c.config.PartitionID,
				Namespace:   gossipwire.NamespaceFromMarshaller(c.config.Marshaller),
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
		tx:     c.db.Txn(true),
		byType: c.applier.byType,
	}
}

// NewTransactor creates new transactor.
func (c *Client) NewTransactor() Transactor {
	return &transactor{
		client:  c,
		changes: map[changeID]change{},
	}
}

func (c *Client) applyHotEnd(triggerCh chan trigger, view *View, updatedTypes map[reflect.Type]struct{}) {
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
	case !errors.Is(err, ErrTxOutdatedTx):
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

// Transactor builds and broadcasts transactions.
type Transactor interface {
	Tx(ctx context.Context, txF func(tx Tx) error) error
}

// Tx prepares transaction.
type Tx interface {
	View() *View
	Set(o any) error
	SoftSet(o any) error
}

type transactor struct {
	client       *Client
	changes      map[changeID]change
	previousTxID memdb.ID
	buf          []byte
}

// Tx creates and broadcasts transaction to the magma network.
func (t *transactor) Tx(ctx context.Context, txF func(tx Tx) error) error {
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

func (t *transactor) prepareTx(txF func(tx Tx) error) (retTx txEnvelope, retUsedSize uint64, retErr error) {
	defer clear(t.changes)

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
	_, metaMsgSize, err := t.client.applier.metaM.Marshal(meta, t.buf[metaSize:])
	if err != nil {
		return txEnvelope{}, 0, err
	}
	metaSize += metaMsgSize

	pendingTx := &tx{
		// By taking a snapshot, we don't block the main DB from processing incoming changes.
		txn:    t.client.db.Txn(false),
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

var revisionType = reflect.TypeFor[types.Revision]()

func validateType(t reflect.Type, isRevisionRequired bool) (reflect.Type, error) {
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

	if isRevisionRequired {
		revisionF, exists := t.FieldByName("Revision")
		if !exists {
			return nil, errors.Errorf("object %s has no Revision field", t)
		}
		if revisionF.Type != revisionType {
			return nil, errors.Errorf("object's %s Revision field must be of type %s", t, revisionType)
		}
		if revisionF.Index[0] != 1 || revisionF.Offset != memdb.IDLength {
			return nil, errors.Errorf("revision must be the second field in type %d", t)
		}
	}

	return idF.Type, nil
}

func NewDBConfig(m proton.Marshaller, indices []memdb.Index) (memdb.Config, error) {
	entities, err := typesFromMarshaller(m)
	if err != nil {
		return memdb.Config{}, err
	}

	return memdb.Config{
		Entities: entities,
		Indices:  indices,
	}, nil
}

func typesFromMarshaller(m proton.Marshaller) ([]reflect.Type, error) {
	entities := m.Messages()
	if len(entities) == 0 {
		return nil, errors.New("no object types provided")
	}

	types := make([]reflect.Type, 0, len(entities))
	for _, e := range entities {
		types = append(types, reflect.TypeOf(e))
	}

	return types, nil
}
