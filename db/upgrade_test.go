package db

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/magma/integration/entities"
	"github.com/outofforest/magma/integration/entities2"
	"github.com/outofforest/memdb"
)

func TestUpgrade(t *testing.T) {
	requireT := require.New(t)

	view, err := CreateView(entities.NewMarshaller())
	requireT.NoError(err)

	acc1 := entities.Account{
		ID:        memdb.NewID[entities.AccountID](),
		FirstName: "John",
		LastName:  "Smith",
	}
	acc2 := entities.Account{
		ID:        memdb.NewID[entities.AccountID](),
		FirstName: "Jane",
		LastName:  "Doe",
	}

	view.Set(acc1)
	view.Set(acc2)

	upgrade := Upgrade{
		Marshaller: entities2.NewMarshaller(),
		UpgradeFunc: func(view *View, set UpgradeSetFunc) error {
			for a := range All[entities.Account](view) {
				set(entities2.Account{
					ID:        entities2.AccountID(a.ID),
					FirstName: a.FirstName,
					LastName:  a.LastName,
					Name:      a.FirstName + " " + a.LastName,
				})
			}
			return nil
		},
	}
	view2, err := ExecuteUpgrade(view, upgrade)
	requireT.NoError(err)

	acc, exists := Get[entities2.Account](view2, entities2.AccountID(acc1.ID))
	requireT.True(exists)
	requireT.Equal(entities2.Account{
		ID:        entities2.AccountID(acc1.ID),
		FirstName: acc1.FirstName,
		LastName:  acc1.LastName,
		Name:      acc1.FirstName + " " + acc1.LastName,
	}, acc)

	acc, exists = Get[entities2.Account](view2, entities2.AccountID(acc2.ID))
	requireT.True(exists)
	requireT.Equal(entities2.Account{
		ID:        entities2.AccountID(acc2.ID),
		FirstName: acc2.FirstName,
		LastName:  acc2.LastName,
		Name:      acc2.FirstName + " " + acc2.LastName,
	}, acc)
}

func TestUpgradeFailure(t *testing.T) {
	requireT := require.New(t)

	view, err := CreateView(entities.NewMarshaller())
	requireT.NoError(err)

	acc1 := entities.Account{
		ID:        memdb.NewID[entities.AccountID](),
		FirstName: "John",
		LastName:  "Smith",
	}
	acc2 := entities.Account{
		ID:        memdb.NewID[entities.AccountID](),
		FirstName: "Jane",
		LastName:  "Doe",
	}

	view.Set(acc1)
	view.Set(acc2)

	errUpgrade := errors.New("upgrade failed")
	upgrade := Upgrade{
		Marshaller: entities2.NewMarshaller(),
		UpgradeFunc: func(view *View, set UpgradeSetFunc) error {
			return errUpgrade
		},
	}
	view2, err := ExecuteUpgrade(view, upgrade)
	requireT.ErrorIs(err, errUpgrade)
	requireT.Nil(view2)
}

func TestUpgradePanics(t *testing.T) {
	requireT := require.New(t)

	view, err := CreateView(entities.NewMarshaller())
	requireT.NoError(err)

	acc1 := entities.Account{
		ID:        memdb.NewID[entities.AccountID](),
		FirstName: "John",
		LastName:  "Smith",
	}
	acc2 := entities.Account{
		ID:        memdb.NewID[entities.AccountID](),
		FirstName: "Jane",
		LastName:  "Doe",
	}

	view.Set(acc1)
	view.Set(acc2)

	errUpgrade := errors.New("upgrade failed")
	upgrade := Upgrade{
		Marshaller: entities2.NewMarshaller(),
		UpgradeFunc: func(view *View, set UpgradeSetFunc) error {
			panic(errUpgrade)
		},
	}
	view2, err := ExecuteUpgrade(view, upgrade)
	requireT.ErrorIs(err, errUpgrade)
	requireT.Nil(view2)
}
