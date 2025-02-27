package main

import (
	"github.com/outofforest/magma/client/entities"
	"github.com/outofforest/proton"
)

//go:generate go run .

func main() {
	proton.Generate("../entities.proton.go",
		entities.Account{},
	)
}
