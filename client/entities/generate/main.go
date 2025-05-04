package main

import (
	"github.com/outofforest/magma"
	"github.com/outofforest/magma/client/entities"
)

//go:generate go run .

func main() {
	magma.Generate("../entities.proton.go",
		entities.Account{},
	)
}
