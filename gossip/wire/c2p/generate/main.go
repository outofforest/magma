package main

import (
	rafttypes "github.com/outofforest/magma/raft/types"
	"github.com/outofforest/proton"
)

//go:generate go run .

func main() {
	proton.Generate("../c2p.proton.go",
		rafttypes.CommitInfo{},
	)
}
