package config

import (
	"github.com/outofforest/magma/db"
	"github.com/outofforest/magma/types"
	"github.com/outofforest/proton"
	"github.com/outofforest/resonance"
)

// Config is the config of magma.
type Config struct {
	P2PCA             *resonance.CA
	C2PCA             *resonance.CA
	ServerID          types.ServerID
	Partitions        []PartitionConfig
	Servers           []ServerConfig
	MaxMessageSize    uint64
	MaxUncommittedLog uint64
	PageSize          uint64
	Directory         string
}

// PartitionConfig stores configuration of partition.
type PartitionConfig struct {
	ID                types.PartitionID
	InitialMarshaller proton.Marshaller
	Upgrades          []db.Upgrade
}

// ServerConfig stores configuration of server.
type ServerConfig struct {
	ID         types.ServerID
	P2PAddress string
	Partitions types.Partitions
}
