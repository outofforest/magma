package main

import (
	"github.com/outofforest/magma/state/repository/format"
	"github.com/outofforest/proton"
)

//go:generate go run .

func main() {
	proton.Generate("../format.proton.go",
		proton.Message[format.Header](),
	)
}
