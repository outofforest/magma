package main

import (
	"github.com/outofforest/magma/gossip/wire"
	"github.com/outofforest/magma/gossip/wire/c2p"
	"github.com/outofforest/proton"
)

//go:generate go run .

func main() {
	proton.Generate("../c2p.proton.go",
		proton.Message(c2p.Init{}),
		proton.Message(wire.StartLogStream{}),
		proton.Message(wire.HotEnd{}),
	)
}
