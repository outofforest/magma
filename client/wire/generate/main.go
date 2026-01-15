package main

import (
	"github.com/outofforest/magma/client/wire"
	"github.com/outofforest/proton"
)

//go:generate go run .

func main() {
	proton.Generate("../wire.proton.go",
		proton.Message[wire.TxMetadata](),
		proton.Message[wire.EntityMetadata](),
	)
}
