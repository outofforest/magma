package main

import (
	"github.com/outofforest/magma/state/events/format"
	"github.com/outofforest/proton"
)

//go:generate go run .

func main() {
	proton.Generate("../format.proton.go",
		proton.Message(format.Term{}),
		proton.Message(format.Vote{}),
	)
}
