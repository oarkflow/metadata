package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/oarkflow/metadata"
)

// getDatabaseConfigs returns database configurations for different drivers
func getDatabaseConfigs() map[string]metadata.Config {
	return map[string]metadata.Config{
		"mysql": {
			Host:     "localhost",
			Port:     3306,
			Driver:   "mysql",
			Username: "root",
			Password: "root",
			Database: "test_db",
		},
		"postgresql": {
			Host:     "localhost",
			Port:     5432,
			Driver:   "postgresql",
			Username: "postgres",
			Password: "postgres",
			Database: "test_db",
		},
		"sqlite3": {
			Driver:   "sqlite3",
			Database: ":memory:", // In-memory database for testing
		},
	}
}

func main() {
	fmt.Println("=== GenerateSQL with Enhanced Constraints Examples ===\n")

	// Example 1: Basic table with primary key and foreign key
	fmt.Println("1. Basic User Table with Foreign Key")
	basicExample()
	return
	fmt.Println("\n" + "=" + strings.Repeat("=", 50) + "\n")

	// Example 2: Complex table with all constraint types
	fmt.Println("2. Complex Product Table with All Constraints")
	complexExample()

	fmt.Println("\n" + "=" + strings.Repeat("=", 50) + "\n")

	// Example 3: Migration example
	fmt.Println("3. Migration Example")
	migrationExample()
}

func basicExample() {
	// Define fields for a user table
	fields := []metadata.Field{
		{
			Name:       "id",
			DataType:   "int",
			IsNullable: "NO",
			Key:        "PRI",
		},
		{
			Name:       "username",
			DataType:   "varchar(100)",
			IsNullable: "NO",
		},
		{
			Name:       "email",
			DataType:   "varchar(255)",
			IsNullable: "NO",
		},
		{
			Name:       "department_id",
			DataType:   "int",
			IsNullable: "YES",
		},
		{
			Name:       "created_at",
			DataType:   "timestamp",
			IsNullable: "NO",
			Default:    "CURRENT_TIMESTAMP",
		},
	}

	// Define constraints
	constraints := &metadata.Constraint{
		Indices: []metadata.Indices{
			{
				Name:    "idx_username",
				Unique:  true,
				Columns: []string{"username"},
			},
			{
				Name:    "idx_email",
				Unique:  false,
				Columns: []string{"email"},
			},
		},
		UniqueKeys: []metadata.UniqueConstraint{
			{
				Name:    "uk_username_email",
				Columns: []string{"username", "email"},
			},
		},
		ForeignKeys: []metadata.ForeignKey{
			{
				Name:             "fk_department",
				ReferencedTable:  "departments",
				ReferencedColumn: "id",
				OnDelete:         "SET NULL",
				OnUpdate:         "CASCADE",
			},
		},
	}

	// Generate SQL for different databases with proper configurations
	configs := getDatabaseConfigs()
	drivers := []string{"mysql", "postgresql", "mssql", "sqlite"}

	for _, driver := range drivers {
		fmt.Printf("--- %s SQL ---\n", driver)

		cfg, exists := configs[driver]
		if !exists {
			fmt.Printf("No configuration found for %s\n", driver)
			continue
		}

		src := metadata.New(cfg)
		if src == nil {
			fmt.Printf("Failed to create %s source\n", driver)
			continue
		}

		source, err := src.Connect()
		if err != nil {
			fmt.Printf("Failed to connect to %s database: %v\n", driver, err)
			fmt.Printf("Showing field definitions without database connection:\n")
			// Fallback: show field definitions without database connection
			for _, field := range fields {
				fieldSQL := src.FieldAsString(field, "column")
				fmt.Printf("  %s\n", fieldSQL)
			}
			continue
		}

		// Generate actual SQL with constraints
		sql, err := source.GenerateSQL("users", fields, constraints)
		if err != nil {
			fmt.Printf("Error generating SQL: %v\n", err)
			continue
		}

		fmt.Printf("Generated SQL:\n%s\n", sql)
	}
}

func complexExample() {
	// Define fields for a product table with all constraint types
	fields := []metadata.Field{
		{
			Name:       "id",
			DataType:   "bigserial",
			IsNullable: "NO",
			Key:        "PRI",
		},
		{
			Name:       "sku",
			DataType:   "varchar(50)",
			IsNullable: "NO",
		},
		{
			Name:       "name",
			DataType:   "varchar(255)",
			IsNullable: "NO",
		},
		{
			Name:       "description",
			DataType:   "text",
			IsNullable: "YES",
		},
		{
			Name:       "price",
			DataType:   "decimal(10,2)",
			IsNullable: "NO",
		},
		{
			Name:       "stock_quantity",
			DataType:   "int",
			IsNullable: "NO",
			Default:    0,
		},
		{
			Name:       "category_id",
			DataType:   "int",
			IsNullable: "YES",
		},
		{
			Name:       "supplier_id",
			DataType:   "int",
			IsNullable: "YES",
		},
		{
			Name:       "is_active",
			DataType:   "boolean",
			IsNullable: "NO",
			Default:    true,
		},
		{
			Name:       "created_at",
			DataType:   "timestamp",
			IsNullable: "NO",
			Default:    "CURRENT_TIMESTAMP",
		},
		{
			Name:       "updated_at",
			DataType:   "timestamp",
			IsNullable: "YES",
		},
	}

	// Define comprehensive constraints
	constraints := &metadata.Constraint{
		Indices: []metadata.Indices{
			{
				Name:    "idx_sku",
				Unique:  true,
				Columns: []string{"sku"},
			},
			{
				Name:    "idx_category",
				Unique:  false,
				Columns: []string{"category_id"},
			},
			{
				Name:    "idx_supplier",
				Unique:  false,
				Columns: []string{"supplier_id"},
			},
			{
				Name:    "idx_price_stock",
				Unique:  false,
				Columns: []string{"price", "stock_quantity"},
			},
		},
		UniqueKeys: []metadata.UniqueConstraint{
			{
				Name:    "uk_sku",
				Columns: []string{"sku"},
			},
			{
				Name:    "uk_name_category",
				Columns: []string{"name", "category_id"},
			},
		},
		CheckKeys: []metadata.CheckConstraint{
			{
				Name:       "ck_price_positive",
				Expression: "price > 0",
			},
			{
				Name:       "ck_stock_non_negative",
				Expression: "stock_quantity >= 0",
			},
			{
				Name:       "ck_name_length",
				Expression: "LENGTH(name) > 0",
			},
		},
		PrimaryKeys: []metadata.PrimaryKeyConstraint{
			{
				Name:    "pk_product",
				Columns: []string{"id"},
			},
		},
		ForeignKeys: []metadata.ForeignKey{
			{
				Name:             "fk_category",
				ReferencedTable:  "categories",
				ReferencedColumn: "id",
				OnDelete:         "RESTRICT",
				OnUpdate:         "CASCADE",
			},
			{
				Name:             "fk_supplier",
				ReferencedTable:  "suppliers",
				ReferencedColumn: "id",
				OnDelete:         "SET NULL",
				OnUpdate:         "CASCADE",
			},
		},
	}

	// Generate SQL for PostgreSQL (most feature-complete)
	fmt.Println("--- PostgreSQL Complex Product Table SQL ---")

	configs := getDatabaseConfigs()
	cfg, exists := configs["postgresql"]
	if !exists {
		log.Printf("No PostgreSQL configuration found")
		return
	}

	src := metadata.New(cfg)
	if src == nil {
		log.Printf("Failed to create PostgreSQL source")
		return
	}

	source, err := src.Connect()
	if err != nil {
		log.Printf("Failed to connect to PostgreSQL database: %v\n", err)
		// Fallback: show field definitions without database connection
		fmt.Printf("Showing field definitions without database connection:\n")
		for _, field := range fields {
			fieldSQL := src.FieldAsString(field, "column")
			fmt.Printf("  %s\n", fieldSQL)
		}
		return
	}

	sql, err := source.GenerateSQL("products", fields, constraints)
	if err != nil {
		fmt.Printf("Error generating SQL: %v\n", err)
		return
	}

	fmt.Printf("%s\n", sql)
}

func migrationExample() {
	// Example showing how to migrate from one database to another
	fmt.Println("Migration: MySQL to PostgreSQL")

	// Source fields (MySQL style)
	sourceFields := []metadata.Field{
		{
			Name:       "id",
			DataType:   "int(11)",
			IsNullable: "NO",
			Key:        "PRI",
			Extra:      "auto_increment",
		},
		{
			Name:       "user_name",
			DataType:   "varchar(100)",
			IsNullable: "NO",
		},
		{
			Name:       "user_email",
			DataType:   "varchar(255)",
			IsNullable: "NO",
		},
		{
			Name:       "created_at",
			DataType:   "datetime",
			IsNullable: "NO",
			Default:    "CURRENT_TIMESTAMP",
		},
	}

	// Constraints
	constraints := &metadata.Constraint{
		Indices: []metadata.Indices{
			{
				Name:    "idx_user_email",
				Unique:  true,
				Columns: []string{"user_email"},
			},
		},
		UniqueKeys: []metadata.UniqueConstraint{
			{
				Name:    "uk_user_name",
				Columns: []string{"user_name"},
			},
		},
	}

	// Generate MySQL CREATE TABLE
	fmt.Println("Source (MySQL):")
	configs := getDatabaseConfigs()
	mysqlCfg, mysqlExists := configs["mysql"]
	if !mysqlExists {
		log.Printf("No MySQL configuration found")
		return
	}

	mysqlSrc := metadata.New(mysqlCfg)
	if mysqlSrc == nil {
		log.Printf("Failed to create MySQL source")
		return
	}

	mysqlSource, err := mysqlSrc.Connect()
	if err != nil {
		log.Printf("Failed to connect to MySQL database: %v\n", err)
		// Fallback: show field definitions without database connection
		fmt.Printf("Showing MySQL field definitions without database connection:\n")
		for _, field := range sourceFields {
			fieldSQL := mysqlSrc.FieldAsString(field, "column")
			fmt.Printf("  %s\n", fieldSQL)
		}
	} else {
		mysqlSQL, _ := mysqlSource.GenerateSQL("users", sourceFields, constraints)
		fmt.Printf("%s\n\n", mysqlSQL)
	}

	// Generate PostgreSQL CREATE TABLE
	fmt.Println("Target (PostgreSQL):")
	pgCfg, pgExists := configs["postgresql"]
	if !pgExists {
		log.Printf("No PostgreSQL configuration found")
		return
	}

	pgSrc := metadata.New(pgCfg)
	if pgSrc == nil {
		log.Printf("Failed to create PostgreSQL source")
		return
	}

	pgSource, err := pgSrc.Connect()
	if err != nil {
		log.Printf("Failed to connect to PostgreSQL database: %v\n", err)
		// Fallback: show field definitions without database connection
		fmt.Printf("Showing PostgreSQL field definitions without database connection:\n")
		for _, field := range sourceFields {
			fieldSQL := pgSrc.FieldAsString(field, "column")
			fmt.Printf("  %s\n", fieldSQL)
		}
	} else {
		pgSQL, _ := pgSource.GenerateSQL("users", sourceFields, constraints)
		fmt.Printf("%s\n", pgSQL)
	}
}
