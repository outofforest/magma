package main

import (
	"github.com/outofforest/magma/gossip/wire/l2p"
	"github.com/outofforest/magma/raft/types"
	"github.com/outofforest/proton"
)

//go:generate go run .

func main() {
	proton.Generate("../l2p.proton.go",
		types.AppendEntriesRequest{},
		types.AppendEntriesResponse{},
		l2p.StartTransfer{},
	)
}
