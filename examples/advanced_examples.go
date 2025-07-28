package main

import (
	"fmt"
	"log"

	"github.com/oarkflow/metadata"
)

// SQLiteExample demonstrates SQLite-specific functionality
func SQLiteExample() {
	fmt.Println("=== SQLite Example ===")

	cfg := metadata.Config{
		Host:     "", // SQLite doesn't need host
		Port:     0,  // SQLite doesn't need port
		Driver:   "sqlite",
		Database: "test.db", // File path for SQLite
	}

	source := metadata.New(cfg)
	if source == nil {
		log.Printf("Failed to create SQLite data source")
		return
	}

	fmt.Printf("SQLite Config: %+v\n", source.Config())
	fmt.Printf("Database Type: %s\n", source.GetType())
	fmt.Printf("Data Type Map for 'text': %s\n", source.GetDataTypeMap("text"))

	// Test field generation for SQLite
	testFields := []metadata.Field{
		{
			Name:       "id",
			DataType:   "INTEGER",
			IsNullable: "NO",
			Key:        "PRI",
			Extra:      "autoincrement",
		},
		{
			Name:       "name",
			DataType:   "TEXT",
			IsNullable: "NO",
		},
		{
			Name:       "email",
			DataType:   "TEXT",
			IsNullable: "YES",
		},
		{
			Name:       "score",
			DataType:   "REAL",
			IsNullable: "YES",
			Default:    0.0,
		},
		{
			Name:       "is_active",
			DataType:   "INTEGER", // SQLite uses INTEGER for boolean
			IsNullable: "NO",
			Default:    1,
		},
		{
			Name:       "created_at",
			DataType:   "DATETIME",
			IsNullable: "NO",
			Default:    "CURRENT_TIMESTAMP",
		},
	}

	// Test SQL generation
	sql, err := source.GenerateSQL("test_table", testFields)
	if err == nil {
		fmt.Printf("Generated SQLite SQL:\n%s\n", sql)
	} else {
		fmt.Printf("SQL generation failed: %v\n", err)
	}

	// Test FieldAsString method
	fmt.Println("Field SQL Generation:")
	for _, field := range testFields {
		fieldSQL := source.FieldAsString(field, "column")
		fmt.Printf("  %s: %s\n", field.Name, fieldSQL)
	}
}

// EnhancedMigrationExample demonstrates migration between databases
func EnhancedMigrationExample() {
	fmt.Println("\n=== Enhanced Migration Example ===")

	// Source: MySQL
	mysqlCfg := metadata.Config{
		Host:     "localhost",
		Port:     3306,
		Driver:   "mysql",
		Username: "root",
		Password: "password",
		Database: "source_db",
	}

	// Target: PostgreSQL
	pgCfg := metadata.Config{
		Host:     "localhost",
		Port:     5432,
		Driver:   "postgresql",
		Username: "postgres",
		Password: "password",
		Database: "target_db",
	}

	mysqlSource := metadata.New(mysqlCfg)
	pgSource := metadata.New(pgCfg)

	if mysqlSource == nil || pgSource == nil {
		log.Printf("Failed to create data sources for migration")
		return
	}

	// Sample fields from MySQL table
	mysqlFields := []metadata.Field{
		{
			Name:       "id",
			DataType:   "int(11)",
			IsNullable: "NO",
			Key:        "PRI",
			Extra:      "auto_increment",
		},
		{
			Name:       "name",
			DataType:   "varchar(255)",
			IsNullable: "NO",
		},
		{
			Name:       "price",
			DataType:   "decimal(10,2)",
			IsNullable: "YES",
			Default:    0.00,
		},
	}

	// Convert MySQL fields to PostgreSQL equivalents
	pgFields := convertFieldsForPostgreSQL(mysqlFields)

	// Generate SQLs
	mysqlSQL, _ := mysqlSource.GenerateSQL("products", mysqlFields)
	pgSQL, _ := pgSource.GenerateSQL("products", pgFields)

	fmt.Printf("MySQL Source SQL:\n%s\n", mysqlSQL)
	fmt.Printf("PostgreSQL Target SQL:\n%s\n", pgSQL)

	// Show data type mapping
	fmt.Println("Data Type Mapping:")
	for i, mysqlField := range mysqlFields {
		fmt.Printf("  %s: %s -> %s\n", mysqlField.Name, mysqlField.DataType, pgFields[i].DataType)
	}
}

// convertFieldsForPostgreSQL converts MySQL field definitions to PostgreSQL equivalents
func convertFieldsForPostgreSQL(mysqlFields []metadata.Field) []metadata.Field {
	pgFields := make([]metadata.Field, len(mysqlFields))

	for i, field := range mysqlFields {
		pgField := field // Copy the field

		// Convert MySQL data types to PostgreSQL equivalents
		switch {
		case field.DataType == "int(11)" && field.Extra == "auto_increment":
			pgField.DataType = "SERIAL"
			pgField.Extra = ""
		case field.DataType[:7] == "varchar":
			pgField.DataType = field.DataType // PostgreSQL supports varchar(n)
		case field.DataType[:7] == "decimal":
			// Convert decimal(10,2) to numeric(10,2)
			pgField.DataType = "numeric" + field.DataType[7:]
		default:
			pgField.DataType = field.DataType
		}

		pgFields[i] = pgField
	}

	return pgFields
}

// DataTypeAnalysisExample shows detailed data type parsing
func DataTypeAnalysisExample() {
	fmt.Println("\n=== Data Type Analysis Example ===")

	testDataTypes := []string{
		"varchar(255)",
		"decimal(10,2)",
		"numeric(18,4)",
		"int(11)",
		"char(50)",
		"text",
		"bigint",
		"datetime",
		"timestamp",
	}

	// Test with different database drivers
	drivers := []string{"mysql", "postgresql", "mssql"}

	for _, driver := range drivers {
		fmt.Printf("\n--- %s Data Type Analysis ---\n", driver)

		cfg := metadata.Config{Driver: driver}
		source := metadata.New(cfg)

		if source == nil {
			fmt.Printf("Failed to create %s source\n", driver)
			continue
		}

		for _, dataType := range testDataTypes {
			field := metadata.Field{
				Name:       "test_field",
				DataType:   dataType,
				IsNullable: "YES",
			}

			fieldSQL := source.FieldAsString(field, "column")
			fmt.Printf("  %s: %s\n", dataType, fieldSQL)
		}
	}
}
