package main

import (
	"github.com/outofforest/magma"
	"github.com/outofforest/magma/integration/entities2"
)

//go:generate go run .

func main() {
	magma.Generate("../entities.proton.go",
		magma.Entity[entities2.Empty](),
		magma.Entity[entities2.Ignore]("Ignored"),
		magma.Entity[entities2.Blob](),
		magma.Entity[entities2.Fields](),
		magma.Entity[entities2.Account](),
	)
}
