package main

import (
	"fmt"

	"github.com/oarkflow/metadata"
)

func main() {
	cfg := metadata.Config{
		Host:     "localhost",
		Port:     3306,
		Driver:   "mysql",
		Username: "root",
		Password: "root",
		Database: "cleardb",
	}
	source := metadata.New(cfg)
	src, err := source.Connect()
	if err != nil {
		panic(err)
	}
	fmt.Println(src.GetFields("tbl_event_dx_fac"))
}
