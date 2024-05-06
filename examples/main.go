package main

import (
	"fmt"
	"runtime"

	"github.com/oarkflow/squealx"

	"github.com/oarkflow/metadata"
)

type Person struct {
	FirstName string
	LastName  string
	Email     string
}

func mai1n() {
	cfg := metadata.Config{
		Host:     "localhost",
		Port:     5432,
		Driver:   "postgresql",
		Username: "postgres",
		Password: "postgres",
		Database: "clear20",
	}
	source := metadata.New(cfg)
	src, err := source.Connect()
	if err != nil {
		panic(err)
	}
	personStructs := []Person{
		{FirstName: "Ardie", LastName: "Savea", Email: "asavea@ab.co.nz"},
		{FirstName: "Sonny Bill", LastName: "Williams", Email: "sbw@ab.co.nz"},
		{FirstName: "Ngani", LastName: "Laumape", Email: "nlaumape@ab.co.nz"},
	}
	err = src.Store("person", personStructs)
	if err != nil {
		panic(err)
	}
}

func main() {
	_, source := conn()
	src, err := source.Connect()
	if err != nil {
		panic(err)
	}
	last := false
	paging := &squealx.Paging{
		Limit: 1,
		Page:  1,
	}
	f, _ := src.GetRawCollection("SELeCT * FROM accounts")
	fmt.Println(len(f))
	for !last {
		resp := src.GetRawPaginatedCollection("SELECT * FROM accounts", *paging)
		if resp.Error != nil {
			panic(resp.Error)
		}
		fromDB := resp.Items
		switch fromDB := fromDB.(type) {
		case *[]map[string]any:
			if fromDB == nil || len(*fromDB) == 0 {
				last = true
			}
		case []map[string]any:
			if fromDB == nil || len(fromDB) == 0 {
				last = true
			}
		}
		runtime.GC()
		fmt.Println(paging, fromDB)
		paging.Page++
	}
	fmt.Println(paging)
}

func conn() (src, dst metadata.DataSource) {
	cfg1 := metadata.Config{
		Host:     "localhost",
		Port:     3306,
		Driver:   "mysql",
		Username: "root",
		Password: "T#sT1234",
		Database: "tests",
	}
	cfg := metadata.Config{
		Host:     "localhost",
		Port:     5432,
		Driver:   "postgresql",
		Username: "postgres",
		Password: "postgres",
		Database: "clear20_dev",
	}
	src = metadata.New(cfg1)
	dst = metadata.New(cfg)
	return
}
