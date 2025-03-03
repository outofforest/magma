package main

import (
	"github.com/outofforest/magma/gossip/wire"
	"github.com/outofforest/proton"
)

//go:generate go run .

func main() {
	proton.Generate("../tx2p.proton.go",
		wire.Hello{},
	)
}
