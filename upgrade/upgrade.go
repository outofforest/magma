package upgrade

import (
	"github.com/outofforest/magma/state"
	"github.com/outofforest/magma/state/repository"
)

func Upgrade(repo *repository.Repository, s *state.State) error {
	p := repository.NewTailProvider()
	p.SetTail(s.NextIndex(), 0)

	it := repo.Iterator(p, 0)

	for {
		r, _, err := it.Reader(false)
		if err != nil {
			return err
		}
		if r == nil {
			return nil
		}
	}
}
