package main

import (
	"fmt"

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
	source, _ := conn()
	dest, err := source.Connect()
	if err != nil {
		panic(err)
	}
	rows, err := dest.GetRawCollection("SELECT * FROM users LIMIT 10")
	if err != nil {
		panic(err)
	}
	fmt.Println(rows)
	/*
		err := metadata.MigrateTables(source, destination, "users")
		if err != nil {
			panic(err)
		}
	*/
}

func conn() (src, dst v2.DataSource) {
	cfg1 := v2.Config{
		Host:     "localhost",
		Port:     3306,
		Driver:   "mysql",
		Username: "root",
		Password: "T#sT1234",
		Database: "tests",
	}
	cfg := v2.Config{
		Host:     "localhost",
		Port:     5432,
		Driver:   "postgresql",
		Username: "postgres",
		Password: "postgres",
		Database: "sujit",
	}
	src = v2.New(cfg1)
	dst = v2.New(cfg)
	return
}
