# Metadata Library Examples

This directory contains comprehensive examples demonstrating the metadata library's capabilities across different database systems.

## Overview

The metadata library provides a unified interface for working with different database systems including MySQL, PostgreSQL, Microsoft SQL Server, and SQLite. It supports advanced features like data type parsing, SQL generation, and database-agnostic metadata extraction.

## Files

### Core Examples

- **`main.go`** - Original example with PostgreSQL connection and MySQL-to-PostgreSQL migration
- **`database_examples.go`** - Comprehensive examples for all database types
- **`advanced_examples.go`** - Advanced features including SQLite support and migration examples
- **`test_runner.go`** - Simple test runner to verify all implementations

### Additional Examples

- **`csv.go`** - CSV data processing examples
- **`migrate.go`** - Database migration utilities
- **`parse.go`** - Data parsing examples

## Key Features Demonstrated

### 1. Database Type Support

The library supports four major database systems:

- **MySQL** - Full support with enhanced data type parsing
- **PostgreSQL** - Complete implementation with advanced features
- **Microsoft SQL Server** - Newly implemented with comprehensive functionality
- **SQLite** - File-based database support

### 2. Enhanced Data Type Parsing

The library now properly handles complex data types like:
- `varchar(255)` - Variable character fields with length
- `decimal(10,2)` - Decimal fields with precision and scale
- `numeric(18,4)` - Numeric fields with custom precision
- Database-specific types (e.g., `nvarchar` for MSSQL, `TEXT` for SQLite)

### 3. SQL Generation

Automatic SQL generation for:
- Table creation statements
- Field definitions with proper constraints
- Database-specific syntax optimization

### 4. Migration Support

Cross-database migration capabilities:
- MySQL to PostgreSQL
- Data type conversion
- Schema transformation

## Running the Examples

### Prerequisites

Make sure you have Go installed and the required database drivers:

```bash
go mod tidy
```

### Basic Testing (No Database Required)

Run the test runner to see all implementations in action:

```bash
go run test_runner.go
```

This will test all database types without requiring actual database connections.

### Full Examples (Database Required)

To run examples with actual database connections:

```bash
# Run the main example (requires PostgreSQL)
go run main.go

# Run specific database examples
go run database_examples.go

# Run advanced examples
go run advanced_examples.go
```

### Configuration

Update the database configuration in each example file:

```go
cfg := metadata.Config{
    Host:     "localhost",
    Port:     5432,
    Driver:   "postgresql",
    Username: "your_username",
    Password: "your_password",
    Database: "your_database",
}
```

## Example Usage

### Basic Database Connection

```go
import "github.com/oarkflow/metadata"

cfg := metadata.Config{
    Host:     "localhost",
    Port:     3306,
    Driver:   "mysql",
    Username: "root",
    Password: "password",
    Database: "test_db",
}

source := metadata.New(cfg)
```

### Data Type Parsing

```go
field := metadata.Field{
    Name:       "price",
    DataType:   "decimal(10,2)",
    IsNullable: "YES",
}

fieldSQL := source.FieldAsString(field, "column")
// Result: price DECIMAL(10,2) NULL
```

### SQL Generation

```go
fields := []metadata.Field{
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
}

sql, err := source.GenerateSQL("users", fields)
```

## New Features

### Enhanced FieldAsString Method

The `FieldAsString` method now properly parses complex data types:

- **Before**: `varchar(255)` → `varchar NULL`
- **After**: `varchar(255)` → `varchar(255) NULL`

### Complete MSSQL Implementation

Microsoft SQL Server support is now fully implemented with:
- Native MSSQL data types (`nvarchar`, `decimal`, `bit`)
- SQL Server-specific syntax
- Index management
- Full CRUD operations

### SQLite Support

Added comprehensive SQLite support with:
- File-based database handling
- SQLite-specific data types (`INTEGER`, `TEXT`, `REAL`)
- PRAGMA-based metadata extraction

### Interface Compliance

All database implementations now properly implement the `DataSource` interface with:
- `GetTheIndices(table string, database ...string)` method
- `FieldAsString(field Field, context string)` method
- Consistent method signatures across all drivers

## Database-Specific Notes

### MySQL
- Supports `auto_increment` fields
- Enhanced `decimal(precision,scale)` parsing
- Optimized for MySQL 5.7+ syntax

### PostgreSQL
- Uses `SERIAL` for auto-incrementing fields
- Supports `numeric(precision,scale)` data types
- Compatible with PostgreSQL 9.6+ features

### Microsoft SQL Server
- Uses `IDENTITY` for auto-incrementing fields
- Supports `nvarchar` for Unicode strings
- Compatible with SQL Server 2016+ features

### SQLite
- Uses `INTEGER PRIMARY KEY` for auto-increment
- Simplified data type system
- File-based database support

## Troubleshooting

### Connection Issues
1. Verify database server is running
2. Check connection parameters (host, port, credentials)
3. Ensure database exists and user has proper permissions

### Data Type Issues
1. Verify data type format matches database expectations
2. Check precision and scale parameters for numeric types
3. Refer to database-specific documentation for supported types

### Import Issues
1. Run `go mod tidy` to ensure all dependencies are available
2. Check that database drivers are properly imported
3. Verify Go version compatibility (requires Go 1.18+)

## Contributing

When adding new examples:
1. Follow the existing naming conventions
2. Include comprehensive error handling
3. Document any database-specific requirements
4. Add tests for new functionality
