package main

import (
	"github.com/outofforest/magma"
	"github.com/outofforest/magma/client/localentities"
)

//go:generate go run .

func main() {
	magma.Generate("../entities.proton.go",
		magma.Entity[localentities.Entity1](),
		magma.Entity[localentities.Entity2](),
	)
}
