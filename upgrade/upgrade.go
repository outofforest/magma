package upgrade

import (
	"errors"
	"io"
	"reflect"

	"github.com/outofforest/magma/client"
	"github.com/outofforest/magma/state"
	"github.com/outofforest/magma/state/repository"
	"github.com/outofforest/memdb"
	"github.com/outofforest/proton"
	"github.com/outofforest/resonance"
)

func Upgrade(
	repo *repository.Repository,
	s *state.State,
	maxMessageSize uint64,
	m proton.Marshaller,
) error {
	dbConfig, err := client.NewDBConfig(m, nil)
	db, err := memdb.NewMemDB(dbConfig)
	if err != nil {
		return err
	}

	applier, err := client.NewApplier(m)
	if err != nil {
		return err
	}

	p := repository.NewTailProvider()
	p.SetTail(s.NextIndex(), 0)
	it := repo.Iterator(p, 0)

	tx := db.Txn(true)
	updatedTypes := map[reflect.Type]struct{}{}

	for {
		r, _, err := it.Reader(false)
		if err != nil {
			return err
		}
		if r == nil {
			break
		}

		reader := resonance.NewReader(r, maxMessageSize)
		var checksum uint64
		for {
			txRaw, _, err := reader.ReceiveRawBytes()
			switch {
			case err == nil:
			case errors.Is(err, io.EOF):
				break
			default:
				return err
			}

			_, checksum, err = applier.ApplyTx(checksum, tx, txRaw, updatedTypes)
			if err != nil && !errors.Is(err, client.ErrTxOutdatedTx) {
				return err
			}
		}
	}

	return nil
}
