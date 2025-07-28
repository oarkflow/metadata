package metadata

import (
	"fmt"
	"strings"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/dbresolver"

	// "github.com/oarkflow/squealx/drivers/sqlite" // TODO: Enable when sqlite driver is available
	"github.com/oarkflow/squealx/orm"
)

type SQLite struct {
	schema     string
	dsn        string
	id         string
	client     dbresolver.DBResolver
	disableLog bool
	pooling    ConnectionPooling
	config     Config
}

var sqliteQueries = map[string]string{
	"create_table":        "CREATE TABLE IF NOT EXISTS %s",
	"alter_table":         "ALTER TABLE %s",
	"column":              "`%s` %s",
	"add_column":          "ADD COLUMN `%s` %s",
	"change_column":       "ALTER COLUMN `%s` %s", // SQLite doesn't support ALTER COLUMN directly
	"remove_column":       "DROP COLUMN `%s`",     // SQLite 3.35.0+
	"create_unique_index": "CREATE UNIQUE INDEX %s ON %s (%s);",
	"create_index":        "CREATE INDEX %s ON %s (%s);",
}

var sqliteDataTypes = map[string]string{
	// Integer types (SQLite treats all integers as INTEGER)
	"int":              "INTEGER",
	"integer":          "INTEGER",
	"tinyint":          "INTEGER",
	"smallint":         "INTEGER",
	"mediumint":        "INTEGER",
	"bigint":           "INTEGER",
	"unsigned big int": "INTEGER",
	"int2":             "INTEGER", // PostgreSQL int2
	"int4":             "INTEGER", // PostgreSQL int4
	"int8":             "INTEGER", // PostgreSQL int8
	"bit":              "INTEGER",

	// PostgreSQL serial types (SQLite uses AUTOINCREMENT)
	"serial":      "INTEGER", // Will be handled specially for PRIMARY KEY AUTOINCREMENT
	"serial2":     "INTEGER", // Will be handled specially for PRIMARY KEY AUTOINCREMENT
	"serial4":     "INTEGER", // Will be handled specially for PRIMARY KEY AUTOINCREMENT
	"serial8":     "INTEGER", // Will be handled specially for PRIMARY KEY AUTOINCREMENT
	"bigserial":   "INTEGER", // Will be handled specially for PRIMARY KEY AUTOINCREMENT
	"smallserial": "INTEGER", // Will be handled specially for PRIMARY KEY AUTOINCREMENT

	// Text types (SQLite treats all text as TEXT)
	"character":         "TEXT",
	"varchar":           "TEXT",
	"varying character": "TEXT",
	"nchar":             "TEXT",
	"native character":  "TEXT",
	"nvarchar":          "TEXT",
	"text":              "TEXT",
	"clob":              "TEXT",
	"string":            "TEXT",
	"char":              "TEXT",
	"tinytext":          "TEXT",
	"mediumtext":        "TEXT",
	"longtext":          "TEXT",
	"ntext":             "TEXT", // SQL Server ntext
	"varchar_ansi":      "TEXT", // SQL Server varchar
	"text_ansi":         "TEXT", // SQL Server text
	"character varying": "TEXT", // PostgreSQL character varying

	// Blob types (SQLite treats all binary as BLOB)
	"blob":       "BLOB",
	"binary":     "BLOB",
	"varbinary":  "BLOB",
	"tinyblob":   "BLOB",
	"mediumblob": "BLOB",
	"longblob":   "BLOB",
	"image":      "BLOB", // SQL Server image
	"bytea":      "BLOB", // PostgreSQL bytea

	// Real types (SQLite treats all floating point as REAL)
	"real":             "REAL",
	"double":           "REAL",
	"double precision": "REAL",
	"float":            "REAL",
	"float4":           "REAL", // PostgreSQL float4
	"float8":           "REAL", // PostgreSQL float8
	"decimal":          "REAL",
	"numeric":          "REAL",
	"money":            "REAL", // SQL Server money
	"smallmoney":       "REAL", // SQL Server smallmoney

	// Date and time types (SQLite has flexible date/time storage)
	"date":                     "DATE",
	"datetime":                 "DATETIME",
	"timestamp":                "DATETIME",
	"timestamptz":              "DATETIME", // PostgreSQL timestamptz
	"timestamp with time zone": "DATETIME",
	"time":                     "TIME",
	"time with time zone":      "TIME", // PostgreSQL time with timezone
	"timetz":                   "TIME", // PostgreSQL timetz
	"year":                     "INTEGER",
	"datetime2":                "DATETIME", // SQL Server datetime2
	"smalldatetime":            "DATETIME", // SQL Server smalldatetime
	"datetimeoffset":           "TEXT",     // SQL Server datetimeoffset -> store as ISO 8601 text
	"interval":                 "TEXT",     // PostgreSQL interval

	// Boolean types (SQLite uses INTEGER for boolean)
	"bool":    "INTEGER",
	"boolean": "INTEGER",

	// JSON type (SQLite 3.38+ has JSON support, but store as TEXT for compatibility)
	"json":  "TEXT",
	"jsonb": "TEXT", // PostgreSQL binary JSON

	// XML type (stored as TEXT)
	"xml": "TEXT",

	// UUID type (stored as TEXT)
	"uuid":             "TEXT",
	"uniqueidentifier": "TEXT", // SQL Server uniqueidentifier
	"guid":             "TEXT", // SQL Server guid

	// Geometric types (stored as TEXT or BLOB depending on format)
	"geometry":           "BLOB",
	"geography":          "BLOB", // SQL Server geography
	"point":              "TEXT", // PostgreSQL point
	"line":               "TEXT", // PostgreSQL line
	"lseg":               "TEXT", // PostgreSQL line segment
	"box":                "TEXT", // PostgreSQL box
	"path":               "TEXT", // PostgreSQL path
	"polygon":            "TEXT", // PostgreSQL polygon
	"circle":             "TEXT", // PostgreSQL circle
	"linestring":         "TEXT", // MySQL linestring
	"multipoint":         "TEXT", // MySQL multipoint
	"multilinestring":    "TEXT", // MySQL multilinestring
	"multipolygon":       "TEXT", // MySQL multipolygon
	"geometrycollection": "TEXT", // MySQL geometrycollection

	// Network types (PostgreSQL - stored as TEXT)
	"inet":     "TEXT", // IP address
	"cidr":     "TEXT", // CIDR notation
	"macaddr":  "TEXT", // MAC address
	"macaddr8": "TEXT", // EUI-64 MAC address

	// Array and range types (PostgreSQL - stored as TEXT, typically JSON format)
	"array":     "TEXT",
	"int4range": "TEXT", // PostgreSQL int4range
	"int8range": "TEXT", // PostgreSQL int8range
	"numrange":  "TEXT", // PostgreSQL numrange
	"tsrange":   "TEXT", // PostgreSQL tsrange
	"tstzrange": "TEXT", // PostgreSQL tstzrange
	"daterange": "TEXT", // PostgreSQL daterange

	// Text search types (PostgreSQL - stored as TEXT)
	"tsvector": "TEXT", // PostgreSQL text search vector
	"tsquery":  "TEXT", // PostgreSQL text search query

	// Bit string types (PostgreSQL)
	"bit varying": "BLOB", // PostgreSQL bit varying
	"varbit":      "BLOB", // PostgreSQL varbit

	// Enum and Set (MySQL - stored as TEXT)
	"enum": "TEXT", // MySQL ENUM
	"set":  "TEXT", // MySQL SET

	// SQL Server specific types
	"hierarchyid": "TEXT", // SQL Server hierarchyid
	"sql_variant": "TEXT", // SQL Server sql_variant
	"cursor":      "TEXT", // SQL Server cursor (metadata)
	"table":       "TEXT", // SQL Server table type (metadata)
}

func (p *SQLite) Connect() (DataSource, error) {
	// TODO: Implement when SQLite driver is available
	return nil, fmt.Errorf("SQLite driver not available")
	/*
		if p.client == nil {
			db1, err := sqlite.Open(p.dsn, p.id)
			if err != nil {
				return nil, err
			}
			p.client, err = dbresolver.New(dbresolver.WithMasterDBs(db1), dbresolver.WithReadWritePolicy(dbresolver.ReadWrite))
			if err != nil {
				return nil, err
			}
			p.client.SetConnMaxLifetime(time.Duration(p.pooling.MaxLifetime) * time.Second)
			p.client.SetConnMaxIdleTime(time.Duration(p.pooling.MaxIdleTime) * time.Second)
			p.client.SetMaxOpenConns(p.pooling.MaxOpenCons)
			p.client.SetMaxIdleConns(p.pooling.MaxIdleCons)
			p.client.SetDefaultDB(p.id)
		}
		return p, nil
	*/
}

func (p *SQLite) GetSources(database ...string) (tables []Source, err error) {
	err = p.client.Select(&tables, `
		SELECT
			name,
			type
		FROM sqlite_master
		WHERE type IN ('table', 'view')`)
	return
}

func (p *SQLite) Config() Config {
	return p.config
}

func (p *SQLite) GetDataTypeMap(dataType string) string {
	// Parse data type to handle cases like varchar(255), numeric(10,2), etc.
	baseDataType, _, _ := parseDataTypeWithParameters(dataType)
	
	if v, ok := sqliteDataTypes[baseDataType]; ok {
		return v
	}
	return "TEXT"
}

func (p *SQLite) GetTables(database ...string) (tables []Source, err error) {
	err = p.client.Select(&tables, `
		SELECT
			name,
			type
		FROM sqlite_master
		WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`)
	return
}

func (p *SQLite) GetViews(database ...string) (tables []Source, err error) {
	err = p.client.Select(&tables, `
		SELECT
			name,
			sql as definition
		FROM sqlite_master
		WHERE type = 'view'`)
	return
}

func (p *SQLite) Client() any {
	return p.client
}

func (p *SQLite) GetDBName(database ...string) string {
	if len(database) > 0 {
		return database[0]
	}
	return p.schema
}

func (p *SQLite) Store(table string, val any) error {
	_, err := p.client.Exec(orm.InsertQuery(table, val), val)
	return err
}

func (p *SQLite) StoreInBatches(table string, val any, size int) error {
	return processBatchInsert(p.client, table, val, size)
}

func (p *SQLite) GetFields(table string, database ...string) (fields []Field, err error) {
	var fieldMaps []map[string]any
	err = p.client.Select(&fieldMaps, `PRAGMA table_info(`+table+`)`)
	if err != nil {
		return
	}

	// Convert pragma info to Field struct format
	for _, fieldMap := range fieldMaps {
		field := Field{
			Name:       fieldMap["name"].(string),
			DataType:   strings.ToLower(fieldMap["type"].(string)),
			IsNullable: "YES",
			Default:    fieldMap["dflt_value"],
		}

		if fieldMap["notnull"].(int64) == 1 {
			field.IsNullable = "NO"
		}

		if fieldMap["pk"].(int64) == 1 {
			field.Key = "PRI"
		}

		// Parse type for length and precision
		baseType, length, precision := parseDataTypeWithParameters(field.DataType)
		field.DataType = baseType
		if length > 0 {
			field.Length = length
		}
		if precision > 0 {
			field.Precision = precision
		}

		fields = append(fields, field)
	}
	return
}

func (p *SQLite) GetForeignKeys(table string, database ...string) (fields []ForeignKey, err error) {
	var fkMaps []map[string]any
	err = p.client.Select(&fkMaps, `PRAGMA foreign_key_list(`+table+`)`)
	if err != nil {
		return
	}

	for _, fkMap := range fkMaps {
		fk := ForeignKey{
			Name:             fkMap["from"].(string),
			ReferencedTable:  fkMap["table"].(string),
			ReferencedColumn: fkMap["to"].(string),
		}
		fields = append(fields, fk)
	}
	return
}

func (p *SQLite) GetIndices(table string, database ...string) (fields []Index, err error) {
	var indexNames []map[string]any
	err = p.client.Select(&indexNames, `PRAGMA index_list(`+table+`)`)
	if err != nil {
		return
	}

	for _, indexMap := range indexNames {
		indexName := indexMap["name"].(string)
		var indexInfo []map[string]any
		err = p.client.Select(&indexInfo, `PRAGMA index_info(`+indexName+`)`)
		if err != nil {
			continue
		}

		for _, info := range indexInfo {
			index := Index{
				Name:       indexName,
				ColumnName: info["name"].(string),
				Nullable:   true, // SQLite doesn't track this in pragma
			}
			fields = append(fields, index)
		}
	}
	return
}

func (p *SQLite) GetTheIndices(table string, database ...string) (fields []Indices, err error) {
	var indexNames []map[string]any
	err = p.client.Select(&indexNames, `PRAGMA index_list(`+table+`)`)
	if err != nil {
		return
	}

	for _, indexMap := range indexNames {
		indexName := indexMap["name"].(string)
		unique := indexMap["unique"].(int64) == 1

		var indexInfo []map[string]any
		err = p.client.Select(&indexInfo, `PRAGMA index_info(`+indexName+`)`)
		if err != nil {
			continue
		}

		var columns []string
		for _, info := range indexInfo {
			columns = append(columns, info["name"].(string))
		}

		if len(columns) > 0 {
			index := Indices{
				Name:    indexName,
				Unique:  unique,
				Columns: columns,
			}
			fields = append(fields, index)
		}
	}
	return
}

func (p *SQLite) LastInsertedID() (id any, err error) {
	err = p.client.Select(&id, "SELECT last_insert_rowid();")
	return
}

func (p *SQLite) MaxID(table, field string) (id any, err error) {
	err = p.client.Select(&id, fmt.Sprintf("SELECT MAX(`%s`) FROM `%s`;", field, table))
	return
}

func (p *SQLite) GetCollection(table string) ([]map[string]any, error) {
	var rows []map[string]any
	err := p.client.Select(&rows, "SELECT * FROM `"+table+"`")
	return rows, err
}

func (p *SQLite) Close() error {
	return p.client.Close()
}

func (p *SQLite) Exec(sql string, values ...any) error {
	_, err := p.client.Exec(sql, values...)
	return err
}

func (p *SQLite) Begin() (squealx.SQLTx, error) {
	return p.client.Begin()
}

func (p *SQLite) GetRawCollection(query string, params ...map[string]any) ([]map[string]any, error) {
	var rows []map[string]any
	if len(params) > 0 {
		param := params[0]
		if val, ok := param["preview"]; ok {
			preview := val.(bool)
			if preview {
				query = strings.Split(query, " LIMIT ")[0] + " LIMIT 10"
			}
		}
		if len(param) > 0 {
			if err := p.client.Select(&rows, query, param); err != nil {
				return nil, err
			}
		} else {
			if err := p.client.Select(&rows, query); err != nil {
				return nil, err
			}
		}
	} else if err := p.client.Select(&rows, query); err != nil {
		return nil, err
	}

	return rows, nil
}

func (p *SQLite) GetRawPaginatedCollection(query string, paging squealx.Paging, params ...map[string]any) squealx.PaginatedResponse {
	var rows []map[string]any
	return p.client.Paginate(query, &rows, paging, params...)
}

func (p *SQLite) GetPaginated(table string, paging squealx.Paging) squealx.PaginatedResponse {
	var rows []map[string]any
	return p.client.Paginate("SELECT * FROM `"+table+"`", &rows, paging)
}

func (p *SQLite) GetSingle(table string) (map[string]any, error) {
	var row map[string]any
	if err := p.client.Select(&row, fmt.Sprintf("SELECT * FROM `%s` LIMIT 1", table)); err != nil {
		return nil, err
	}
	return row, nil
}

func (p *SQLite) GetType() string {
	return "sqlite"
}

func getSQLiteFieldAlterDataType(table string, f Field) string {
	// SQLite has limited ALTER TABLE support, most changes require recreating the table
	dataTypes := sqliteDataTypes
	
	// Parse data type to handle cases like varchar(255), numeric(10,2), etc.
	baseDataType, parsedLength, parsedPrecision := parseDataTypeWithParameters(f.DataType)
	
	// Use parsed length and precision if field doesn't have them set
	if f.Length == 0 && parsedLength > 0 {
		f.Length = parsedLength
	}
	if f.Precision == 0 && parsedPrecision > 0 {
		f.Precision = parsedPrecision
	}
	
	defaultVal := ""
	if f.Default != nil {
		switch def := f.Default.(type) {
		case string:
			if def == "CURRENT_TIMESTAMP" || strings.ToLower(def) == "true" || strings.ToLower(def) == "false" {
				defaultVal = fmt.Sprintf("DEFAULT %s", def)
			} else {
				defaultVal = fmt.Sprintf("DEFAULT '%s'", def)
			}
		default:
			defaultVal = "DEFAULT " + fmt.Sprintf("%v", def)
		}
	}
	nullable := ""
	if strings.ToUpper(f.IsNullable) == "NO" {
		nullable = "NOT NULL"
	}

	return fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN `%s` %s %s %s;", table, f.Name, dataTypes[baseDataType], nullable, defaultVal)
}

func (p *SQLite) alterFieldSQL(table string, f, existingField Field) string {
	// SQLite doesn't support modifying existing columns directly
	// This would require a more complex table recreation process
	return ""
}

func (p *SQLite) createSQL(table string, newFields []Field, indices ...Indices) (string, error) {
	var sql string
	var query, indexQuery, primaryKeys []string
	for _, newField := range newFields {
		if strings.ToUpper(newField.Key) == "PRI" {
			primaryKeys = append(primaryKeys, "`"+newField.Name+"`")
		}
		query = append(query, p.FieldAsString(newField, "column"))
	}
	if len(indices) > 0 {
		for _, index := range indices {
			if index.Name == "" {
				index.Name = "idx_" + table + "_" + strings.Join(index.Columns, "_")
			}
			switch index.Unique {
			case true:
				query := fmt.Sprintf(sqliteQueries["create_unique_index"], index.Name, "`"+table+"`",
					"`"+strings.Join(index.Columns, "`, `")+"`")
				indexQuery = append(indexQuery, query)
			case false:
				query := fmt.Sprintf(sqliteQueries["create_index"], index.Name, "`"+table+"`",
					"`"+strings.Join(index.Columns, "`, `")+"`")
				indexQuery = append(indexQuery, query)
			}
		}
	}
	if len(primaryKeys) > 0 {
		query = append(query, " PRIMARY KEY ("+strings.Join(primaryKeys, ", ")+")")
	}
	if len(query) > 0 {
		fieldsToUpdate := strings.Join(query, ", ")
		sql = fmt.Sprintf(sqliteQueries["create_table"], "`"+table+"`") + " (" + fieldsToUpdate + ");"
	}
	if len(indexQuery) > 0 {
		sql += strings.Join(indexQuery, "")
	}
	return sql, nil
}

func (p *SQLite) alterSQL(table string, newFields []Field, indices ...Indices) (string, error) {
	var sql []string
	existingFields, err := p.GetFields(table)
	if err != nil {
		return "", err
	}

	for _, newField := range newFields {
		if newField.IsNullable == "" {
			newField.IsNullable = "YES"
		}
		fieldExists := false
		for _, existingField := range existingFields {
			if existingField.Name == newField.Name {
				fieldExists = true
				break
			}
		}

		if !fieldExists {
			qry := getSQLiteFieldAlterDataType(table, newField)
			if qry != "" {
				sql = append(sql, qry)
			}
		}
	}

	if len(sql) > 0 {
		return strings.Join(sql, ""), nil
	}
	return "", nil
}

func (p *SQLite) GenerateSQL(table string, newFields []Field, indices ...Indices) (string, error) {
	sources, err := p.GetSources()
	if err != nil {
		return "", err
	}
	sourceExists := false
	for _, source := range sources {
		if source.Name == table {
			sourceExists = true
			break
		}
	}
	if !sourceExists {
		return p.createSQL(table, newFields, indices...)
	}
	return p.alterSQL(table, newFields, indices...)
}

func (p *SQLite) Migrate(table string, dst DataSource) error {
	fields, err := p.GetFields(table)
	if err != nil {
		return err
	}
	sql, err := dst.GenerateSQL(table, fields)
	if err != nil {
		return err
	}
	fmt.Println(sql)
	return nil
}

func (p *SQLite) FieldAsString(f Field, action string) string {
	sqlPattern := sqliteQueries
	dataTypes := sqliteDataTypes
	nullable := ""
	defaultVal := ""
	primaryKey := ""
	autoIncrement := ""

	// Parse data type to handle cases like varchar(255), numeric(10,2), etc.
	baseDataType, parsedLength, parsedPrecision := parseDataTypeWithParameters(f.DataType)

	// Use parsed length and precision if field doesn't have them set
	if f.Length == 0 && parsedLength > 0 {
		f.Length = parsedLength
	}
	if f.Precision == 0 && parsedPrecision > 0 {
		f.Precision = parsedPrecision
	}

	// Use the base data type for mapping
	actualDataType := baseDataType

	// Check if data type exists in mapping, provide fallback
	mappedDataType, exists := dataTypes[actualDataType]
	if !exists {
		// Fallback to TEXT for unknown types
		mappedDataType = "TEXT"
		actualDataType = "text"
	}

	// Handle PostgreSQL serial types specially - convert to INTEGER with PRIMARY KEY AUTOINCREMENT
	if actualDataType == "serial" || actualDataType == "serial2" || actualDataType == "serial4" ||
		actualDataType == "serial8" || actualDataType == "bigserial" || actualDataType == "smallserial" {
		mappedDataType = "INTEGER"
		primaryKey = "PRIMARY KEY"
		autoIncrement = "AUTOINCREMENT"
	}

	if strings.ToUpper(f.IsNullable) == "NO" {
		nullable = "NOT NULL"
	}
	if f.Default != nil {
		switch def := f.Default.(type) {
		case string:
			if contains(builtInFunctions, strings.ToLower(def)) {
				defaultVal = fmt.Sprintf("DEFAULT %s", def)
			} else {
				defaultVal = fmt.Sprintf("DEFAULT '%s'", def)
			}
		default:
			defaultVal = "DEFAULT " + fmt.Sprintf("%v", def)
		}
	}

	if f.Key != "" && strings.ToUpper(f.Key) == "PRI" && action != "column" {
		primaryKey = "PRIMARY KEY"
	}
	if f.Extra != "" && strings.ToUpper(f.Extra) == "AUTO_INCREMENT" {
		autoIncrement = "AUTOINCREMENT"
	}

	// SQLite doesn't use length specifiers for most types
	switch actualDataType {
	case "string", "varchar", "text", "char":
		changeColumn := sqlPattern[action] + " %s %s %s %s %s"
		return strings.TrimSpace(space.ReplaceAllString(fmt.Sprintf(changeColumn, f.Name, mappedDataType, nullable, primaryKey, autoIncrement, defaultVal), " "))
	case "int", "integer", "big_integer", "bigInteger", "tinyint":
		changeColumn := sqlPattern[action] + " %s %s %s %s %s"
		return strings.TrimSpace(space.ReplaceAllString(fmt.Sprintf(changeColumn, f.Name, mappedDataType, nullable, primaryKey, autoIncrement, defaultVal), " "))
	case "float", "double", "decimal":
		changeColumn := sqlPattern[action] + " %s %s %s %s %s"
		return strings.TrimSpace(space.ReplaceAllString(fmt.Sprintf(changeColumn, f.Name, mappedDataType, nullable, primaryKey, autoIncrement, defaultVal), " "))
	default:
		changeColumn := sqlPattern[action] + " %s %s %s %s %s"
		return strings.TrimSpace(space.ReplaceAllString(fmt.Sprintf(changeColumn, f.Name, mappedDataType, nullable, primaryKey, autoIncrement, defaultVal), " "))
	}
}

func NewSQLite(id, dsn, database string, disableLog bool, pooling ConnectionPooling) *SQLite {
	return &SQLite{
		schema:     database,
		dsn:        dsn,
		id:         id,
		client:     nil,
		disableLog: disableLog,
		pooling:    pooling,
	}
}
