package main

import (
	"fmt"
	"log"

	"github.com/oarkflow/metadata"
)

// TestDataTypes represents various data types for testing
type TestDataTypes struct {
	ID          int     `json:"id"`
	Name        string  `json:"name"`
	Email       string  `json:"email"`
	Age         int     `json:"age"`
	Score       float64 `json:"score"`
	IsActive    bool    `json:"is_active"`
	Description string  `json:"description"`
}

func RunDatabaseExamples() {
	fmt.Println("=== MySQL Example ===")
	testMySQL()

	fmt.Println("\n=== PostgreSQL Example ===")
	testPostgreSQL()

	fmt.Println("\n=== MSSQL Example ===")
	testMSSQL()

	fmt.Println("\n=== Data Type Parsing Example ===")
	testDataTypeParsing()
}

func testMySQL() {
	cfg := metadata.Config{
		Host:     "localhost",
		Port:     3306,
		Driver:   "mysql",
		Username: "root",
		Password: "password",
		Database: "test_db",
	}

	src := metadata.New(cfg)
	if src == nil {
		log.Printf("Failed to create MySQL data source")
		return
	}

	source, err := src.Connect()
	if err != nil {
		log.Printf("Failed to connect to MySQL: %v", err)
		return
	}

	// Test connection (this would fail without actual DB, but demonstrates the API)
	fmt.Printf("MySQL Config: %+v\n", source.Config())
	fmt.Printf("Database Type: %s\n", source.GetType())
	fmt.Printf("Data Type Map for 'varchar': %s\n", source.GetDataTypeMap("varchar"))

	// Test field generation with data type parsing
	testFields := []metadata.Field{
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
			Name:       "email",
			DataType:   "varchar(100)",
			IsNullable: "YES",
		},
		{
			Name:       "score",
			DataType:   "decimal(10,2)",
			IsNullable: "YES",
			Default:    0.0,
		},
	}

	// Test SQL generation
	sql, err := source.GenerateSQL("test_table", testFields)
	if err == nil {
		fmt.Printf("Generated MySQL SQL:\n%s\n", sql)
	} else {
		fmt.Printf("SQL generation failed: %v\n", err)
	}

	// Test FieldAsString method
	for _, field := range testFields {
		fieldSQL := source.FieldAsString(field, "column")
		fmt.Printf("Field SQL: %s\n", fieldSQL)
	}
}

func testPostgreSQL() {
	cfg := metadata.Config{
		Host:     "localhost",
		Port:     5432,
		Driver:   "postgresql",
		Username: "postgres",
		Password: "password",
		Database: "test_db",
	}

	source := metadata.New(cfg)
	if source == nil {
		log.Printf("Failed to create PostgreSQL data source")
		return
	}

	fmt.Printf("PostgreSQL Config: %+v\n", source.Config())
	fmt.Printf("Database Type: %s\n", source.GetType())
	fmt.Printf("Data Type Map for 'varchar': %s\n", source.GetDataTypeMap("varchar"))

	// Test field generation with data type parsing
	testFields := []metadata.Field{
		{
			Name:       "id",
			DataType:   "serial",
			IsNullable: "NO",
			Key:        "PRI",
		},
		{
			Name:       "name",
			DataType:   "varchar(255)",
			IsNullable: "NO",
		},
		{
			Name:       "email",
			DataType:   "varchar(100)",
			IsNullable: "YES",
		},
		{
			Name:       "score",
			DataType:   "numeric(10,2)",
			IsNullable: "YES",
			Default:    0.0,
		},
		{
			Name:       "is_active",
			DataType:   "boolean",
			IsNullable: "NO",
			Default:    true,
		},
	}

	// Test SQL generation
	sql, err := source.GenerateSQL("test_table", testFields)
	if err == nil {
		fmt.Printf("Generated PostgreSQL SQL:\n%s\n", sql)
	} else {
		fmt.Printf("SQL generation failed: %v\n", err)
	}

	// Test FieldAsString method
	for _, field := range testFields {
		fieldSQL := source.FieldAsString(field, "column")
		fmt.Printf("Field SQL: %s\n", fieldSQL)
	}
}

func testMSSQL() {
	cfg := metadata.Config{
		Host:     "localhost",
		Port:     1433,
		Driver:   "mssql",
		Username: "sa",
		Password: "Password123!",
		Database: "test_db",
	}

	source := metadata.New(cfg)
	if source == nil {
		log.Printf("Failed to create MSSQL data source")
		return
	}

	fmt.Printf("MSSQL Config: %+v\n", source.Config())
	fmt.Printf("Database Type: %s\n", source.GetType())
	fmt.Printf("Data Type Map for 'varchar': %s\n", source.GetDataTypeMap("varchar"))

	// Test field generation with data type parsing
	testFields := []metadata.Field{
		{
			Name:       "id",
			DataType:   "int",
			IsNullable: "NO",
			Key:        "PRI",
			Extra:      "auto_increment",
		},
		{
			Name:       "name",
			DataType:   "nvarchar(255)",
			IsNullable: "NO",
		},
		{
			Name:       "email",
			DataType:   "nvarchar(100)",
			IsNullable: "YES",
		},
		{
			Name:       "score",
			DataType:   "decimal(18,2)",
			IsNullable: "YES",
			Default:    0.0,
		},
		{
			Name:       "is_active",
			DataType:   "bit",
			IsNullable: "NO",
			Default:    1,
		},
	}

	// Test SQL generation
	sql, err := source.GenerateSQL("test_table", testFields)
	if err == nil {
		fmt.Printf("Generated MSSQL SQL:\n%s\n", sql)
	} else {
		fmt.Printf("SQL generation failed: %v\n", err)
	}

	// Test FieldAsString method
	for _, field := range testFields {
		fieldSQL := source.FieldAsString(field, "column")
		fmt.Printf("Field SQL: %s\n", fieldSQL)
	}
}

func testDataTypeParsing() {
	// Test the parseDataTypeWithParameters function indirectly through FieldAsString
	testCases := []struct {
		dataType string
		expected string
	}{
		{"varchar(255)", "varchar with length 255"},
		{"decimal(10,2)", "decimal with length 10 and precision 2"},
		{"numeric(18,4)", "numeric with length 18 and precision 4"},
		{"int", "int without parameters"},
		{"text", "text without parameters"},
	}

	cfg := metadata.Config{
		Driver: "mysql",
	}
	source := metadata.New(cfg)

	for _, tc := range testCases {
		field := metadata.Field{
			Name:     "test_field",
			DataType: tc.dataType,
		}

		fieldSQL := source.FieldAsString(field, "column")
		fmt.Printf("DataType: %s -> SQL: %s\n", tc.dataType, fieldSQL)
	}
}
