package main

import (
	"github.com/oarkflow/metadata"
	v2 "github.com/oarkflow/metadata/v2"
)

type Person struct {
	FirstName string
	LastName  string
	Email     string
}

func mai1n() {
	cfg := v2.Config{
		Host:     "localhost",
		Port:     5432,
		Driver:   "postgresql",
		Username: "postgres",
		Password: "postgres",
		Database: "sujit",
	}
	source := v2.New(cfg)
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
	source, destination := conn()

	err := metadata.MigrateTables(source, destination, "users")
	if err != nil {
		panic(err)
	}

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
		Database: "sujit",
	}
	src = metadata.New(cfg1)
	dst = metadata.New(cfg)
	return
}
