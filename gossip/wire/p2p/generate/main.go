package main

import (
	"github.com/outofforest/magma/raft/types"
	"github.com/outofforest/proton"
)

//go:generate go run .

func main() {
	proton.Generate("../p2p.proton.go",
		types.AppendEntriesACK{},
		types.VoteRequest{},
		types.VoteResponse{},
		types.Heartbeat{},
	)
}
