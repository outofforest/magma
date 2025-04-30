package main

import (
	"github.com/outofforest/magma/client/wire"
	"github.com/outofforest/proton"
)

//go:generate go run .

func main() {
	proton.Generate("../wire.proton.go",
		wire.TxMetadata{},
	)
}
