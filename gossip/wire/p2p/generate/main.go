package main

import (
	"github.com/outofforest/magma/raft/types"
	"github.com/outofforest/proton"
)

//go:generate go run .

func main() {
	proton.Generate("../p2p.proton.go",
		types.VoteRequest{},
		types.VoteResponse{},
	)
}
