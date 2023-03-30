package main

import (
	"encoding/json"
	"fmt"

	"github.com/oarkflow/metadata"
)

var data = []byte(`
[
	{
		"name": "cpt_code",
		"title": "CPT Code",
		"key": "pri",
		"type": "string",
		"length": 100,
		"is_nullable": "NO",
		"required": true,
		"default": true,
		"comment": "CPT Code"
	},
	{
		"name": "sd",
		"title": "Short Description",
		"type": "string"
	},
	{
		"name": "full_description",
		"title": "Long Description",
		"type": "string",
		"old_name": "l"
	},
	{
		"name": "fd",
		"title": "Full Description",
		"type": "string"
	},
	{
		"name": "nfrvu",
		"title": "nfrvu",
		"type": "double",
		"default": 0.00
	},
	{
		"name": "facrvu",
		"title": "facrvu",
		"type": "double",
		"default": 0.00
	},
	{
		"name": "status",
		"title": "Status",
		"type": "string",
		"default": null
	}
]
`)

func main() {
	var fields []metadata.Field
	err := json.Unmarshal(data, &fields)
	if err != nil {
		panic(err)
	}
	cfg := metadata.Config{
		Host:     "localhost",
		Port:     3306,
		Driver:   "mysql",
		Username: "root",
		Password: "root",
		Database: "itbeema",
	}
	source := metadata.New(cfg)
	src, err := source.Connect()
	if err != nil {
		panic(err)
	}
	queryToExecute, err := src.GenerateSQL("lu_cpt", fields)
	if err != nil {
		panic(err)
	}
	fmt.Println(queryToExecute)
	// fmt.Println(src.Exec(queryToExecute))
}
