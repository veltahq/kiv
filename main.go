package main

import (
	"github.com/veltahq/kiv/engine"
)

func main() {
	db := &engine.NewDatabase{
		Name:   "test",
		Tables: make(map[string]engine.Table),
	}
}
