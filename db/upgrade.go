package db

import (
	"github.com/outofforest/magma/internal"
	"github.com/outofforest/memdb"
	"github.com/outofforest/proton"
)

// UpgradeSetFunc is the function used to set upgraded entities.
type UpgradeSetFunc = func(e any)

// UpgradeFunc defines the function for db conversion.
type UpgradeFunc func(view *View, set UpgradeSetFunc) error

// Upgrade defines db upgrade.
type Upgrade struct {
	Marshaller  proton.Marshaller
	UpgradeFunc UpgradeFunc
}

// CopyAll copies all entities of type T to next version.
func CopyAll[T any](view *View, set UpgradeSetFunc) error {
	for e := range All[T](view) {
		set(e)
	}
	return nil
}

// ExecuteUpgrade is used to execute upgrade in tests.
func ExecuteUpgrade(view *View, upgrade Upgrade) (*View, error) {
	dstView, setF, err := NewUpgradeSetter(upgrade.Marshaller)
	if err != nil {
		return nil, err
	}

	err = func() (retErr error) {
		defer internal.RecoveryHandler(&retErr)

		return upgrade.UpgradeFunc(view, setF)
	}()
	if err != nil {
		return nil, err
	}

	return dstView, err
}

// NewUpgradeSetter returns set function for upgrade and view to fill.
func NewUpgradeSetter(m proton.Marshaller) (*View, UpgradeSetFunc, error) {
	dstView, err := CreateView(m)
	if err != nil {
		return nil, nil, err
	}

	return dstView, func(e any) {
		dstView.Set(e)
	}, nil
}

// CreateView creates fresh empty view based on marshaller.
func CreateView(m proton.Marshaller) (*View, error) {
	entities, err := internal.EntitiesFromMarshaller(m)
	if err != nil {
		return nil, err
	}

	dbConfig, err := internal.NewDBConfig(entities, nil)
	if err != nil {
		return nil, err
	}

	memDB, err := memdb.NewMemDB(dbConfig)
	if err != nil {
		return nil, err
	}

	_, byType, err := internal.NewCache(m)
	if err != nil {
		return nil, err
	}

	return NewView(memDB.Txn(true), byType), nil
}
