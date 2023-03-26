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
	cfg2 := metadata.Config{
		Host:     "localhost",
		Port:     5432,
		Driver:   "postgres",
		Username: "postgres",
		Password: "postgres",
		Database: "clear20",
	}
	destination := metadata.New(cfg2)
	dst, err := destination.Connect()
	if err != nil {
		panic(err)
	}
	fmt.Println(dst.GetFields("accounts"))
	err = src.Migrate("tbl_event_dx_fac", dst)
	if err != nil {
		panic(err)
	}
}
