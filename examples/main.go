package main

import (
	"fmt"

	"github.com/oarkflow/metadata"
)

func main() {
	cfg := metadata.Config{
		Host:     "localhost",
		Port:     5432,
		Driver:   "postgresql",
		Username: "postgres",
		Password: "postgres",
		Database: "clear20",
	}
	source := metadata.New(cfg)
	_, err := source.Connect()
	if err != nil {
		panic(err)
	}
	fields, err := source.GetFields("users")
	if err != nil {
		panic(err)
	}
	schema := metadata.AsJsonSchema(fields, false, "users")
	fmt.Println(schema.String())
}

func migrationTest() {
	source, destination := conn()

	err := metadata.MigrateViews(source, destination)
	if err != nil {
		panic(err)
	}

	/*err := metadata.CloneTable(source, destination, "tbl_patient_event", "")
	if err != nil {
		panic(err)
	}*/

}

func conn() (metadata.DataSource, metadata.DataSource) {
	cfg1 := metadata.Config{
		Host:     "localhost",
		Port:     3307,
		Driver:   "mysql",
		Username: "root",
		Password: "root",
		Database: "cleardb",
	}
	cfg := metadata.Config{
		Host:     "localhost",
		Port:     5432,
		Driver:   "postgresql",
		Username: "postgres",
		Password: "postgres",
		Database: "clear",
	}
	source := metadata.New(cfg1)
	destination := metadata.New(cfg)
	return source, destination
}
