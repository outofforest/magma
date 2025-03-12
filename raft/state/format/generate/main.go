package main

import (
	"github.com/outofforest/magma/raft/state/format"
	"github.com/outofforest/proton"
)

//go:generate go run .

func main() {
	proton.Generate("../format.proton.go",
		format.Header{},
	)
}
