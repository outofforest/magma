package main

import (
	rafttypes "github.com/outofforest/magma/raft/types"
	"github.com/outofforest/proton"
)

//go:generate go run .

func main() {
	proton.Generate("../p2c.proton.go",
		rafttypes.CommitInfo{},
	)
}
