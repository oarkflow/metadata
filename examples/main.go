package main

import (
	"encoding/json"
	"fmt"

	"github.com/oarkflow/metadata"
)

var data = []byte(`
[
					{
						"name": "credential_id",
						"title": "Credential ID",
						"key": "pri",
						"type": "int",
						"is_nullable": "NO",
						"required": true,
						"description": "Unique ID of the credential"
					},
					{
						"name": "user_id",
						"title": "User ID",
						"type": "int",
						"is_nullable": "NO"
					},
					{
						"name": "credential",
						"title": "Credential",
						"type": "varchar",
						"length": 256,
						"is_nullable": "NO"
					},
					{
						"name": "created_at",
						"title": "Created At",
						"type": "timestamp",
						"is_nullable": "NO",
						"default": "CURRENT_TIMESTAMP"
					},
					{
						"name": "updated_at",
						"title": "Updated At",
						"type": "timestamp",
						"is_nullable": "NO",
						"default": "CURRENT_TIMESTAMP"
					},
					{
						"name": "deleted_at",
						"title": "Deleted At",
						"type": "timestamp"
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
	queryToExecute, err := src.GenerateSQL("credentials", fields)
	if err != nil {
		panic(err)
	}
	// fmt.Println(queryToExecute)
	fmt.Println(src.Exec(queryToExecute))
}
