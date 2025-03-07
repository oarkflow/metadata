package main

import (
	"encoding/json"
	"fmt"

	"github.com/oarkflow/metadata"
)

var data = []byte(`
{
	"name": "attendingPhysicianADTIn",
	"title": "AttendingPhysicianADTIn",
	"fields": [
		{
			"name": "id",
			"key": "pri",
			"type": "bigint",
			"is_nullable": "NO",
			"extra": "AUTO_INCREMENT"
		},
		{
			"name": "transactionId",
			"type": "bigint",
			"is_nullable": "NO",
			"comment": "Transaction ID"
		},
		{
			"name": "physicianId",
			"type": "varchar",
			"is_nullable": "YES",
			"comment": "Physician ID"
		},
		{
			"name": "lastName",
			"type": "varchar",
			"is_nullable": "YES",
			"comment": "Last Name"
		},
		{
			"name": "firstName",
			"type": "varchar",
			"is_nullable": "YES",
			"comment": "First Name"
		},
		{
			"name": "middleName",
			"type": "varchar",
			"is_nullable": "YES",
			"comment": "Middle Name"
		}
	],
	"constraints": {
		"indices": [
			{
				"columns": ["transactionId"]
			}
		]
	},
	"update": true,
	"model_type": "table",
	"database": "mirth"
}

`)

type Query struct {
	File   string `json:"file"`
	String string `json:"string"`
}

type Constraint struct {
	Indices     []metadata.Indices    `json:"indices"`
	ForeignKeys []metadata.ForeignKey `json:"foreign"`
}

type Partition struct {
	Type  string `json:"type"`
	Field string `json:"field"`
}

type Model struct {
	Name            string           `json:"name"`
	OldName         string           `json:"old_name"`
	Key             string           `json:"key"`
	Title           string           `json:"title"`
	Database        string           `json:"database"`
	ModelType       string           `json:"model_type"`
	Query           Query            `json:"query"`
	Constraints     Constraint       `json:"constraints"`
	Partition       Partition        `json:"partition"`
	Fields          []metadata.Field `json:"fields"`
	FullTextSearch  bool             `json:"full_text_search"`
	GenerateRestApi bool             `json:"generate_rest_api"`
	Update          bool             `json:"update"`
}

func main() {
	var model Model
	err := json.Unmarshal(data, &model)
	if err != nil {

	}
	cfg := metadata.Config{
		Host:     "localhost",
		Port:     3306,
		Driver:   "mysql",
		Username: "root",
		Password: "root",
		Database: "eamitest",
	}
	source := metadata.New(cfg)
	connector, err := source.Connect()
	if err != nil {
		panic(err)
	}
	sql, err := connector.GenerateSQL(model.Name, model.Fields, model.Constraints.Indices...)
	if err != nil {
		panic(err)
	}
	fmt.Println(sql)

	/*sqlParts := strings.Split(sql, ";")
	for _, sq := range sqlParts {
		if sq != "" {
			err = connector.Exec(sq)
			if err != nil {
				fmt.Println(sq)
				panic(err)
			}
		}
	}*/
}
