package main

import (
	"github.com/outofforest/magma/gossip/wire"
	"github.com/outofforest/magma/raft/types"
	"github.com/outofforest/proton"
)

//go:generate go run .

func main() {
	proton.Generate("../l2p.proton.go",
		types.LogSyncRequest{},
		types.LogSyncResponse{},
		wire.StartLogStream{},
	)
}
