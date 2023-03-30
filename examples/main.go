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
		"nullable": false,
		"required": true,
		"default": true,
		"description": "CPT Code"
	},
	{
		"name": "sd",
		"title": "Short Description",
		"type": "string"
	},
	{
		"name": "ld",
		"title": "Long Description",
		"type": "string"
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
		Port:     5432,
		Driver:   "postgresql",
		Username: "postgres",
		Password: "postgres",
		Database: "sujit",
	}
	source := metadata.New(cfg)
	src, err := source.Connect()
	if err != nil {
		panic(err)
	}
	existingFields, err := src.GetFields("lu_cpt")
	if err != nil {
		panic(err)
	}
	fmt.Println(src.GenerateSQL("lu_cpt", existingFields, fields))
}
