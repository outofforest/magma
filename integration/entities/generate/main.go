package main

import (
	"github.com/outofforest/magma"
	"github.com/outofforest/magma/integration/entities"
)

//go:generate go run .

func main() {
	magma.Generate("../entities.proton.go",
		magma.Entity[entities.Account](),
		magma.Entity[entities.Fields](),
		magma.Entity[entities.Blob](),
	)
}
