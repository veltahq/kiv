package main

import (
	"fmt"

	"github.com/veltahq/kiv/engine"
)

func main() {
	db := engine.NewDatabase{
		Name:   "testDB",
		Tables: map[string]engine.Table{},
	}

	fmt.Println(db)
}
