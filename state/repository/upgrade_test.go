package repository

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"

	"github.com/outofforest/magma/client"
	"github.com/outofforest/magma/client/db"
	"github.com/outofforest/magma/config"
	"github.com/outofforest/magma/integration/entities"
	"github.com/outofforest/magma/integration/entities2"
	"github.com/outofforest/magma/raft/types"
	"github.com/outofforest/magma/state/repository/format"
	magmatypes "github.com/outofforest/magma/types"
	"github.com/outofforest/qa"
	"github.com/outofforest/varuint64"
)

func TestSeparateUpgrades(t *testing.T) {
	requireT := require.New(t)
	ctx := qa.NewContext(t)

	m := entities.NewMarshaller()
	m2 := entities2.NewMarshaller()

	acc1 := entities.Account{
		ID:        entities.AccountID{0x1f},
		FirstName: "Jane",
		LastName:  "Doe",
	}
	acc2 := entities2.Account{
		ID:        entities2.AccountID{0x1e},
		FirstName: "John",
		LastName:  "Smith",
		Name:      "John Smith",
	}

	r, dir := newRepo(t, "", config.PartitionConfig{
		InitialMarshaller: m,
		Upgrades: []db.Upgrade{
			{
				Marshaller: m2,
				UpgradeFunc: func(view *db.View, ch chan<- any) error {
					for a := range db.All[entities.Account](view) {
						ch <- entities2.Account{
							ID:        entities2.AccountID(a.ID),
							FirstName: a.FirstName,
							LastName:  a.LastName,
							Name:      a.FirstName + " " + a.LastName,
						}
					}
					ch <- acc2
					return nil
				},
			},
			{
				Marshaller: m2,
				UpgradeFunc: func(view *db.View, ch chan<- any) error {
					for a := range db.All[entities2.Account](view) {
						a.LastName += ".2"
						ch <- a
					}
					return nil
				},
			},
		},
	})

	w0, sync0, close0, err := createTerm(r, 1)
	requireT.NoError(err)
	defer close0()

	c := client.NewTestClient(t, client.NewTestConfig(m), readChecksum(w0), w0)
	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		return tx.Set(acc1)
	}))

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		acc, exists := db.Get[entities.Account](tx.View(), acc1.ID)
		if !exists {
			return errors.New("account does not exist")
		}
		acc.FirstName = "Jane2"
		return tx.Set(acc)
	}))

	requireT.NoError(sync0())

	requireT.NoError(r.SetCurrentVersion(1))
	w1, _, close1, err := createTerm(r, 2, w0)
	requireT.NoError(err)
	defer close1()

	requireT.NoError(r.upgrade(ctx))

	initRaw, err := os.ReadFile(filepath.Join(dir, "1.init"))
	requireT.NoError(err)
	requireT.Equal([]byte{
		0xe, 0x1, 0x2, 0xf8, 0x92, 0xb3, 0xcf, 0xa7, 0xfc, 0x8b, 0xf3, 0xee, 0x83, 0x1, 0x2,
		0x25,
		0x1f, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0x4,
		0x9, 0x4a, 0x61, 0x6e, 0x65, 0x32, 0x20, 0x44, 0x6f, 0x65,
		0x5, 0x4a, 0x61, 0x6e, 0x65, 0x32,
		0x3, 0x44, 0x6f, 0x65,
		0x27,
		0x1e, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0x4,
		0xa, 0x4a, 0x6f, 0x68, 0x6e, 0x20, 0x53, 0x6d, 0x69, 0x74, 0x68,
		0x4, 0x4a, 0x6f, 0x68, 0x6e,
		0x5, 0x53, 0x6d, 0x69, 0x74, 0x68,
	}, initRaw)

	requireT.NoError(r.SetCurrentVersion(2))
	_, _, close2, err := createTerm(r, 3, w0, w1)
	requireT.NoError(err)
	defer close2()

	requireT.NoError(r.upgrade(ctx))

	initRaw, err = os.ReadFile(filepath.Join(dir, "2.init"))
	requireT.NoError(err)
	requireT.Equal([]byte{
		0xe, 0x2, 0x3, 0xd3, 0x85, 0xc4, 0xff, 0xf2, 0xde, 0x83, 0x99, 0xed, 0x8d, 0x1, 0x2,
		0x29,
		0x1e, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0x4,
		0xa, 0x4a, 0x6f, 0x68, 0x6e, 0x20, 0x53, 0x6d, 0x69, 0x74, 0x68,
		0x4, 0x4a, 0x6f, 0x68, 0x6e,
		0x7, 0x53, 0x6d, 0x69, 0x74, 0x68, 0x2e, 0x32,
		0x27,
		0x1f, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0x4,
		0x9, 0x4a, 0x61, 0x6e, 0x65, 0x32, 0x20, 0x44, 0x6f, 0x65,
		0x5, 0x4a, 0x61, 0x6e, 0x65, 0x32,
		0x5, 0x44, 0x6f, 0x65, 0x2e, 0x32,
	}, initRaw)
}

func TestCombinedUpgrades(t *testing.T) {
	requireT := require.New(t)
	ctx := qa.NewContext(t)

	m := entities.NewMarshaller()
	m2 := entities2.NewMarshaller()

	acc1 := entities.Account{
		ID:        entities.AccountID{0x1f},
		FirstName: "Jane",
		LastName:  "Doe",
	}
	acc2 := entities2.Account{
		ID:        entities2.AccountID{0x1e},
		FirstName: "John",
		LastName:  "Smith",
		Name:      "John Smith",
	}

	r, dir := newRepo(t, "", config.PartitionConfig{
		InitialMarshaller: m,
		Upgrades: []db.Upgrade{
			{
				Marshaller: m2,
				UpgradeFunc: func(view *db.View, ch chan<- any) error {
					for a := range db.All[entities.Account](view) {
						ch <- entities2.Account{
							ID:        entities2.AccountID(a.ID),
							FirstName: a.FirstName,
							LastName:  a.LastName,
							Name:      a.FirstName + " " + a.LastName,
						}
					}
					ch <- acc2
					return nil
				},
			},
			{
				Marshaller: m2,
				UpgradeFunc: func(view *db.View, ch chan<- any) error {
					for a := range db.All[entities2.Account](view) {
						a.LastName += ".2"
						ch <- a
					}
					return nil
				},
			},
		},
	})

	w0, sync0, close0, err := createTerm(r, 1)
	requireT.NoError(err)
	defer close0()

	c := client.NewTestClient(t, client.NewTestConfig(m), readChecksum(w0), w0)
	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		return tx.Set(acc1)
	}))

	requireT.NoError(c.NewTransactor().Tx(ctx, func(tx db.Tx) error {
		acc, exists := db.Get[entities.Account](tx.View(), acc1.ID)
		if !exists {
			return errors.New("account does not exist")
		}
		acc.FirstName = "Jane2"
		return tx.Set(acc)
	}))

	requireT.NoError(sync0())

	requireT.NoError(r.SetCurrentVersion(2))
	_, _, close1, err := createTerm(r, 2, w0)
	requireT.NoError(err)
	defer close1()

	requireT.NoError(r.upgrade(ctx))

	initRaw, err := os.ReadFile(filepath.Join(dir, "1.init"))
	requireT.NoError(err)
	requireT.Equal([]byte{
		0xe, 0x2, 0x2, 0xf8, 0x92, 0xb3, 0xcf, 0xa7, 0xfc, 0x8b, 0xf3, 0xee, 0x83, 0x1, 0x2,
		0x29,
		0x1e, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0x4,
		0xa, 0x4a, 0x6f, 0x68, 0x6e, 0x20, 0x53, 0x6d, 0x69, 0x74, 0x68,
		0x4, 0x4a, 0x6f, 0x68, 0x6e,
		0x7, 0x53, 0x6d, 0x69, 0x74, 0x68, 0x2e, 0x32,
		0x27,
		0x1f, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0x4,
		0x9, 0x4a, 0x61, 0x6e, 0x65, 0x32, 0x20, 0x44, 0x6f, 0x65,
		0x5, 0x4a, 0x61, 0x6e, 0x65, 0x32,
		0x5, 0x44, 0x6f, 0x65, 0x2e, 0x32,
	}, initRaw)
}

func createTerm(
	r *Repository,
	term types.Term,
	previousBufs ...*bytes.Buffer,
) (retB *bytes.Buffer, retS func() error, retC func(), retErr error) {
	var ni magmatypes.Index
	var previousChecksum uint64

	if len(previousBufs) > 0 {
		ni = nextIndex(previousBufs)
		previousChecksum = readChecksum(previousBufs[len(previousBufs)-1])
	}

	f, err := r.Create(term, ni, previousChecksum)
	if err != nil {
		return nil, nil, nil, err
	}
	defer func() {
		if retErr != nil {
			_ = f.Close()
		}
	}()

	fMap, err := f.Map()
	if err != nil {
		return nil, nil, nil, err
	}

	w := bytes.NewBuffer(fMap[:0])

	if err := appendTerm(term, w, previousChecksum); err != nil {
		return nil, nil, nil, err
	}

	if err := f.Sync(); err != nil {
		return nil, nil, nil, err
	}

	return w, f.Sync, func() {
		_ = f.Close()
	}, nil
}

func appendTerm(term types.Term, w io.Writer, previousChecksum uint64) error {
	var termEntry [2*varuint64.MaxSize + format.ChecksumSize]byte

	n := varuint64.Put(termEntry[varuint64.MaxSize:], uint64(term))
	totalSize := n + format.ChecksumSize
	n2 := varuint64.Size(totalSize)
	start := varuint64.MaxSize - n2
	varuint64.Put(termEntry[start:], totalSize)

	checksum := xxh3.HashSeed(termEntry[start:varuint64.MaxSize+n], previousChecksum)
	binary.LittleEndian.PutUint64(termEntry[varuint64.MaxSize+n:], checksum)

	_, err := w.Write(termEntry[start : varuint64.MaxSize+n+format.ChecksumSize])
	return errors.WithStack(err)
}

func readChecksum(buf *bytes.Buffer) uint64 {
	bs := buf.Bytes()
	return binary.LittleEndian.Uint64(bs[len(bs)-format.ChecksumSize:])
}

func nextIndex(bufs []*bytes.Buffer) magmatypes.Index {
	var index magmatypes.Index
	for _, buf := range bufs {
		index += magmatypes.Index(buf.Len())
	}
	return index
}
