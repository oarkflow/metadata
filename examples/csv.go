package main

import (
	"fmt"

	"github.com/oarkflow/metadata"
)

func ma1in() {
	cfg := metadata.Config{
		Driver:   "json",
		Database: "icd10_codes.json",
	}
	source := metadata.New(cfg)
	src, err := source.Connect()
	if err != nil {
		panic(err)
	}
	fmt.Println(src.GetFields(""))
}
