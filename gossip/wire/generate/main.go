package main

import (
	"github.com/outofforest/magma/gossip/wire"
	"github.com/outofforest/magma/raft/wire/p2c"
	"github.com/outofforest/magma/raft/wire/p2p"
	"github.com/outofforest/proton"
)

//go:generate go run .

func main() {
	proton.Generate("../wire.proton.go",
		p2p.AppendEntriesRequest{},
		p2p.AppendEntriesResponse{},
		p2p.VoteRequest{},
		p2p.VoteResponse{},
		p2c.ClientRequest{},
		wire.Hello{},
	)
}
