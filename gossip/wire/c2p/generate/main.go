package main

import (
	"github.com/outofforest/magma/gossip/wire"
	"github.com/outofforest/magma/gossip/wire/c2p"
	"github.com/outofforest/proton"
)

//go:generate go run .

func main() {
	proton.Generate("../c2p.proton.go",
		c2p.Init{},
		wire.StartLogStream{},
	)
}
