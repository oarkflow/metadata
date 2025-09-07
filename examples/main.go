package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/oarkflow/metadata"
	"github.com/oarkflow/squealx"
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
	cfg := []byte(`{"host":"localhost","port":5432,"driver":"postgresql","username":"postgres","password":"postgres","database":"clear_dev"}`)
	var config metadata.Config
	err := json.Unmarshal(cfg, &config)
	if err != nil {
		panic(err)
	}
	source := metadata.New(config)
	src, err := source.Connect()
	if err != nil {
		panic(err)
	}
	if src == nil {
		panic("No DB")
	}
	last := false
	paging := &squealx.Paging{
		Limit: 1,
		Page:  1,
	}
	for !last {
		resp := src.GetRawPaginatedCollection("SELECT * FROM facilities", *paging)
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
		paging.Page++
	}
	fmt.Println(paging)

	// Run comprehensive database examples
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("COMPREHENSIVE DATABASE EXAMPLES")
	fmt.Println(strings.Repeat("=", 60))

	RunDatabaseExamples()
	SQLiteExample()
	EnhancedMigrationExample()
	DataTypeAnalysisExample()
	RunConstraintExamples()
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
		Database: "clear20",
	}
	src = metadata.New(cfg1)
	dst = metadata.New(cfg)
	return
}
