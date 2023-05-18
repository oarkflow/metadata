package main

import (
	"github.com/oarkflow/metadata"
)

func main() {
	source, destination := conn()
	/*err := metadata.MigrateDB(source, destination)
	if err != nil {
		panic(err)
	}*/

	err := metadata.CloneTable(source, destination, "tbl_cpt_blacklist_gender", "")
	if err != nil {
		panic(err)
	}

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
