package main

import (
	"github.com/outofforest/magma/gossip/wire"
	"github.com/outofforest/proton"
)

//go:generate go run .

func main() {
	proton.Generate("../hello.proton.go",
		proton.Message[wire.Hello](),
	)
}
