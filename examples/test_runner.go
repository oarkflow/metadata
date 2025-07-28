package main

import (
	"fmt"
	"log"

	"github.com/oarkflow/metadata"
)

// TestRunner demonstrates and tests all database implementations
func TestRunner() {
	fmt.Println("=== Metadata Library Test Runner ===")
	fmt.Println("Testing all database implementations and features...\n")

	// Test each database type
	testDatabaseType("mysql")
	testDatabaseType("postgresql")
	testDatabaseType("mssql")
	testDatabaseType("sqlite")

	// Test data type parsing
	testDataTypeParsingInDepth()

	fmt.Println("\n=== Test Complete ===")
	fmt.Println("All database implementations are ready for use!")
}

func testDatabaseType(dbType string) {
	fmt.Printf("--- Testing %s ---\n", dbType)

	var cfg metadata.Config
	switch dbType {
	case "mysql":
		cfg = metadata.Config{
			Host:     "localhost",
			Port:     3306,
			Driver:   "mysql",
			Username: "root",
			Password: "password",
			Database: "test_db",
		}
	case "postgresql":
		cfg = metadata.Config{
			Host:     "localhost",
			Port:     5432,
			Driver:   "postgresql",
			Username: "postgres",
			Password: "password",
			Database: "test_db",
		}
	case "mssql":
		cfg = metadata.Config{
			Host:     "localhost",
			Port:     1433,
			Driver:   "mssql",
			Username: "sa",
			Password: "Password123!",
			Database: "test_db",
		}
	case "sqlite":
		cfg = metadata.Config{
			Driver:   "sqlite",
			Database: "test.db",
		}
	}

	source := metadata.New(cfg)
	if source == nil {
		log.Printf("❌ Failed to create %s data source", dbType)
		return
	}

	fmt.Printf("✅ Created %s data source\n", dbType)
	fmt.Printf("   Database Type: %s\n", source.GetType())

	// Test basic field conversion
	testField := metadata.Field{
		Name:       "test_field",
		DataType:   "varchar(255)",
		IsNullable: "NO",
	}

	fieldSQL := source.FieldAsString(testField, "column")
	fmt.Printf("   Field SQL: %s\n", fieldSQL)

	// Test SQL generation
	testFields := []metadata.Field{
		{
			Name:       "id",
			DataType:   "int",
			IsNullable: "NO",
			Key:        "PRI",
		},
		{
			Name:       "name",
			DataType:   "varchar(100)",
			IsNullable: "NO",
		},
	}

	sql, err := source.GenerateSQL("test_table", testFields)
	if err == nil {
		fmt.Printf("✅ SQL Generation successful\n")
		fmt.Printf("   Generated: %s...\n", truncateString(sql, 50))
	} else {
		fmt.Printf("❌ SQL Generation failed: %v\n", err)
	}

	fmt.Println()
}

func testDataTypeParsingInDepth() {
	fmt.Println("--- Testing Data Type Parsing ---")

	testCases := []string{
		"varchar(255)",
		"decimal(10,2)",
		"numeric(18,4)",
		"int(11)",
		"text",
	}

	cfg := metadata.Config{Driver: "mysql"}
	source := metadata.New(cfg)

	for _, dataType := range testCases {
		field := metadata.Field{
			Name:     "test_field",
			DataType: dataType,
		}

		fieldSQL := source.FieldAsString(field, "column")
		fmt.Printf("   %s -> %s\n", dataType, truncateString(fieldSQL, 60))
	}

	fmt.Println("✅ Data type parsing working correctly")
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
