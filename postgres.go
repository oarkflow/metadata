package metadata

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/dbresolver"
	"github.com/oarkflow/squealx/drivers/postgres"
	"github.com/oarkflow/squealx/orm"
)

type Postgres struct {
	schema     string
	dsn        string
	id         string
	client     dbresolver.DBResolver
	disableLog bool
	pooling    ConnectionPooling
	config     Config
}

var postgresQueries = map[string]string{
	"create_table":        "CREATE TABLE IF NOT EXISTS %s",
	"alter_table":         "ALTER TABLE %s",
	"column":              `"%s" %s`,
	"add_column":          "ADD COLUMN %s %s",        // {{length}} NOT NULL DEFAULT 1
	"change_column":       "ALTER COLUMN %s TYPE %s", // {{length}} NOT NULL DEFAULT 1
	"remove_column":       "ALTER COLUMN % TYPE %s",  // {{length}} NOT NULL DEFAULT 1
	"create_unique_index": "CREATE UNIQUE INDEX %s ON %s (%s);",
	"create_index":        "CREATE INDEX %s ON %s (%s);",
}

var postgresDataTypes = map[string]string{
	// Integer types
	"smallint":    "SMALLINT",
	"int2":        "SMALLINT",
	"int":         "INTEGER",
	"int4":        "INTEGER",
	"integer":     "INTEGER",
	"bigint":      "BIGINT",
	"int8":        "BIGINT",
	"serial":      "SERIAL",
	"serial4":     "SERIAL",
	"bigserial":   "BIGSERIAL",
	"serial8":     "BIGSERIAL",
	"smallserial": "SMALLSERIAL",
	"serial2":     "SMALLSERIAL",

	// MySQL integer types -> PostgreSQL equivalents
	"tinyint":   "SMALLINT", // MySQL tinyint -> PostgreSQL smallint
	"mediumint": "INTEGER",  // MySQL mediumint -> PostgreSQL integer

	// Floating point types
	"real":             "REAL",
	"float4":           "REAL",
	"double precision": "DOUBLE PRECISION",
	"float8":           "DOUBLE PRECISION",
	"float":            "REAL",
	"double":           "DOUBLE PRECISION",
	"numeric":          "NUMERIC",
	"decimal":          "NUMERIC",
	"money":            "MONEY",
	"smallmoney":       "NUMERIC", // SQL Server smallmoney -> NUMERIC

	// String types
	"char":              "CHAR",
	"character":         "CHAR",
	"varchar":           "VARCHAR",
	"character varying": "VARCHAR",
	"string":            "VARCHAR",
	"text":              "TEXT",
	"longtext":          "TEXT",
	"longText":          "TEXT",
	"LongText":          "TEXT",

	// MySQL string types -> PostgreSQL equivalents
	"tinytext":   "TEXT",
	"mediumtext": "TEXT",

	// SQL Server Unicode string types -> PostgreSQL equivalents
	"nchar":    "CHAR",
	"nvarchar": "VARCHAR",
	"ntext":    "TEXT",

	// Date and time types
	"date":                     "DATE",
	"time":                     "TIME",
	"timetz":                   "TIME WITH TIME ZONE",
	"time with time zone":      "TIME WITH TIME ZONE",
	"timestamp":                "TIMESTAMP",
	"timestamptz":              "TIMESTAMP WITH TIME ZONE",
	"timestamp with time zone": "TIMESTAMP WITH TIME ZONE",
	"datetime":                 "TIMESTAMP",                // MySQL/SQL Server datetime
	"datetime2":                "TIMESTAMP",                // SQL Server datetime2
	"smalldatetime":            "TIMESTAMP",                // SQL Server smalldatetime
	"datetimeoffset":           "TIMESTAMP WITH TIME ZONE", // SQL Server datetimeoffset
	"interval":                 "INTERVAL",
	"year":                     "SMALLINT", // MySQL year

	// Boolean types
	"bool":    "BOOLEAN",
	"boolean": "BOOLEAN",

	// Binary types
	"bytea":      "BYTEA",
	"binary":     "BYTEA", // MySQL/SQL Server binary
	"varbinary":  "BYTEA", // MySQL/SQL Server varbinary
	"blob":       "BYTEA", // MySQL blob types
	"tinyblob":   "BYTEA",
	"mediumblob": "BYTEA",
	"longblob":   "BYTEA",
	"image":      "BYTEA", // SQL Server image

	// Network address types
	"cidr":     "CIDR",
	"inet":     "INET",
	"macaddr":  "MACADDR",
	"macaddr8": "MACADDR8",

	// Bit string types
	"bit varying": "BIT VARYING",
	"varbit":      "BIT VARYING",

	// UUID type
	"uuid":             "UUID",
	"uniqueidentifier": "UUID", // SQL Server GUID
	"guid":             "UUID",

	// JSON types
	"json":  "JSON",
	"jsonb": "JSONB",

	// Array types
	"array": "ARRAY",

	// Range types
	"int4range": "INT4RANGE",
	"int8range": "INT8RANGE",
	"numrange":  "NUMRANGE",
	"tsrange":   "TSRANGE",
	"tstzrange": "TSTZRANGE",
	"daterange": "DATERANGE",

	// Geometric types
	"point":   "POINT",
	"line":    "LINE",
	"lseg":    "LSEG",
	"box":     "BOX",
	"path":    "PATH",
	"polygon": "POLYGON",
	"circle":  "CIRCLE",

	// MySQL geometric types -> PostgreSQL equivalents
	"geometry":           "GEOMETRY", // PostGIS extension
	"linestring":         "GEOMETRY", // PostGIS extension
	"multipoint":         "GEOMETRY", // PostGIS extension
	"multilinestring":    "GEOMETRY", // PostGIS extension
	"multipolygon":       "GEOMETRY", // PostGIS extension
	"geometrycollection": "GEOMETRY", // PostGIS extension

	// SQL Server geometric types -> PostgreSQL equivalents
	"geography": "GEOMETRY", // PostGIS extension

	// Text search types
	"tsvector": "TSVECTOR",
	"tsquery":  "TSQUERY",

	// XML type
	"xml": "XML",

	// SQL Server specific types -> PostgreSQL equivalents
	"hierarchyid": "TEXT",
	"sql_variant": "TEXT",
	"cursor":      "TEXT", // Not applicable
	"table":       "TEXT", // Not applicable

	// SQLite affinity types -> PostgreSQL equivalents
	"clob":              "TEXT",
	"varying character": "VARCHAR",
	"native character":  "CHAR",
	"unsigned big int":  "BIGINT",

	// MySQL specific types -> PostgreSQL equivalents
	"enum": "TEXT", // PostgreSQL uses ENUM differently
	"set":  "TEXT", // PostgreSQL doesn't have SET type
}

func (p *Postgres) Connect() (DataSource, error) {
	if p.client == nil {
		db1, err := postgres.Open(p.dsn, p.id)
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
}

func (p *Postgres) GetSources(database ...string) (tables []Source, err error) {
	db := p.schema
	if len(database) > 0 {
		db = database[0]
	}
	sq := "SELECT table_name as name, table_type FROM information_schema.tables WHERE table_catalog = :catalog AND table_schema = 'public'"
	err = p.client.Select(&tables, sq, map[string]any{
		"catalog": db,
	})
	return
}

func (p *Postgres) GetDataTypeMap(dataType string) string {
	// Parse data type to handle cases like varchar(255), numeric(10,2), etc.
	baseDataType, _, _ := parseDataTypeWithParameters(dataType)

	if v, ok := postgresDataTypes[baseDataType]; ok {
		return v
	}
	return "VARCHAR"
}

func (p *Postgres) GetTables(database ...string) (tables []Source, err error) {
	db := p.schema
	if len(database) > 0 {
		db = database[0]
	}
	sq := "SELECT table_name as name, table_type FROM information_schema.tables WHERE table_catalog = :catalog AND table_schema = 'public' AND table_type='BASE TABLE'"
	err = p.client.Select(&tables, sq, map[string]any{
		"catalog": db,
	})
	return
}

func (p *Postgres) GetViews(database ...string) (tables []Source, err error) {
	db := p.schema
	if len(database) > 0 {
		db = database[0]
	}
	sq := "SELECT table_name as name, view_definition FROM information_schema.views WHERE table_catalog = :catalog AND table_schema = 'public' AND table_type='VIEW'"
	err = p.client.Select(&tables, sq, map[string]any{
		"catalog": db,
	})
	return
}

func (p *Postgres) Client() any {
	return p.client
}

func (p *Postgres) GetDBName(database ...string) string {
	db := p.schema
	if len(database) > 0 {
		db = database[0]
	}
	return db
}

func (p *Postgres) Config() Config {
	return p.config
}

func (p *Postgres) GetFields(table string, database ...string) (fields []Field, err error) {
	db := p.schema
	if len(database) > 0 {
		db = database[0]
	}
	var fieldMaps []map[string]any
	err = p.client.Select(&fieldMaps, `
SELECT c.column_name as "name", column_default as "default", is_nullable as "is_nullable", data_type as "type", CASE WHEN numeric_precision IS NOT NULL THEN numeric_precision ELSE character_maximum_length END as "length", numeric_scale as "precision",a.column_key as "key", b.comment, '' as extra
FROM INFORMATION_SCHEMA.COLUMNS c
LEFT JOIN (
select kcu.table_name,        'PRI' as column_key,        kcu.ordinal_position as position,        kcu.column_name as column_name
from information_schema.table_constraints tco
join information_schema.key_column_usage kcu       on kcu.constraint_name = tco.constraint_name      and kcu.constraint_schema = tco.constraint_schema      and kcu.constraint_name = tco.constraint_name where tco.constraint_type = 'PRIMARY KEY' and kcu.table_catalog = :catalog AND kcu.table_schema = 'public' AND kcu.table_name = :table_name order by kcu.table_schema,          kcu.table_name,          position          ) a
ON c.table_name = a.table_name AND a.column_name = c.column_name
LEFT JOIN (
select
    c.table_catalog,
    c.table_schema,
    c.table_name,
    c.column_name,
    pgd.description as "comment"
from pg_catalog.pg_statio_all_tables as st
inner join pg_catalog.pg_description pgd on (
    pgd.objoid = st.relid
)
inner join information_schema.columns c on (
    pgd.objsubid   = c.ordinal_position and
    c.table_schema = st.schemaname and
    c.table_name   = st.relname
)
WHERE table_catalog = :catalog AND table_schema = 'public' AND c.table_name =  :table_name
) b ON c.table_name = b.table_name AND b.column_name = c.column_name
          WHERE c.table_catalog = :catalog AND c.table_schema = 'public' AND c.table_name =  :table_name
;`, map[string]any{
		"catalog":    db,
		"table_name": table,
	})
	if err != nil {
		return
	}
	bt, err := json.Marshal(fieldMaps)
	if err != nil {
		return
	}
	err = json.Unmarshal(bt, &fields)
	return
}

func (p *Postgres) Store(table string, val any) error {
	_, err := p.client.Exec(orm.InsertQuery(table, val), val)
	return err
}

func (p *Postgres) StoreInBatches(table string, val any, size int) error {
	return processBatchInsert(p.client, table, val, size)
}

func (p *Postgres) LastInsertedID() (id any, err error) {
	err = p.client.Select(&id, "SELECT LASTVAL();")
	return
}

func (p *Postgres) MaxID(table, field string) (id any, err error) {
	err = p.client.Select(&id, fmt.Sprintf("SELECT MAX(%s) FROM %s;", field, table))
	return
}

func (p *Postgres) GetForeignKeys(table string, database ...string) (fields []ForeignKey, err error) {
	db := p.schema
	if len(database) > 0 {
		db = database[0]
	}
	err = p.client.Select(&fields, `select kcu.constraint_name as "name", kcu.column_name as "column", rel_kcu.table_name as referenced_table, rel_kcu.column_name as referenced_column from information_schema.table_constraints tco join information_schema.key_column_usage kcu           on tco.constraint_schema = kcu.constraint_schema           and tco.constraint_name = kcu.constraint_name join information_schema.referential_constraints rco           on tco.constraint_schema = rco.constraint_schema           and tco.constraint_name = rco.constraint_name join information_schema.key_column_usage rel_kcu           on rco.unique_constraint_schema = rel_kcu.constraint_schema           and rco.unique_constraint_name = rel_kcu.constraint_name           and kcu.ordinal_position = rel_kcu.ordinal_position where tco.constraint_type = 'FOREIGN KEY' and kcu.table_catalog = :catalog AND kcu.table_schema = 'public' AND kcu.table_name = :table_name order by kcu.table_schema,          kcu.table_name,          kcu.ordinal_position;`, map[string]any{
		"catalog":    db,
		"table_name": table,
	})
	return
}

func (p *Postgres) GetIndices(table string, database ...string) (fields []Index, err error) {
	db := p.schema
	if len(database) > 0 {
		db = database[0]
	}
	err = p.client.Select(&fields, `select DISTINCT kcu.constraint_name as "name", kcu.column_name as "column_name", enforced as "nullable" from information_schema.table_constraints tco join information_schema.key_column_usage kcu       on kcu.constraint_name = tco.constraint_name      and kcu.constraint_schema = tco.constraint_schema      and kcu.constraint_name = tco.constraint_name      WHERE tco.table_catalog = :catalog AND tco.table_schema = 'public' AND tco.table_name = :table_name;`, map[string]any{
		"catalog":    db,
		"table_name": table,
	})
	return
}

// GetTheIndices gets the indices for a table other than the primary key.
// This has only been implemented for postgres.
func (p *Postgres) GetTheIndices(table string, database ...string) (indices []Indices, err error) {
	err = p.client.Select(&indices, `
SELECT
	i.relname AS name,
	json_agg(a.attname) AS columns,
	ix.indisunique AS unique
FROM
	pg_class t,
	pg_class i,
	pg_index ix,
	pg_attribute a
WHERE
	t.oid = ix.indrelid
	AND i.oid = ix.indexrelid
	AND a.attrelid = t.oid
	AND a.attnum = ANY (ix.indkey)
	AND t.relkind = 'r'
	AND NOT ix.indisprimary
	AND t.relname = :table_name
GROUP BY
	i.relname,
	ix.indisunique
ORDER BY
	i.relname;`, map[string]any{
		"table_name": table,
	})
	return
}

func (p *Postgres) GetCollection(table string) ([]map[string]any, error) {
	var rows []map[string]any
	err := p.client.Select(&rows, "SELECT * FROM "+table)
	return rows, err
}

func (p *Postgres) Exec(sql string, values ...any) error {
	sql = strings.ReplaceAll(sql, "`", `"`)
	sql = strings.ReplaceAll(sql, `"/"`, `'/'`)
	_, err := p.client.Exec(sql, values...)
	return err
}

func (p *Postgres) GetRawCollection(query string, params ...map[string]any) ([]map[string]any, error) {
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

func (p *Postgres) GetRawPaginatedCollection(query string, paging squealx.Paging, params ...map[string]any) squealx.PaginatedResponse {
	var rows []map[string]any
	return p.client.Paginate(query, &rows, paging, params...)
}

func (p *Postgres) GetPaginated(table string, paging squealx.Paging) squealx.PaginatedResponse {
	var rows []map[string]any
	return p.client.Paginate("SELECT * FROM "+table, &rows, paging)
}

// GetSingle - Get a single row
func (p *Postgres) GetSingle(table string) (map[string]any, error) {
	var row map[string]any
	if err := p.client.Select(&row, fmt.Sprintf("SELECT * FROM %s LIMIT 1", table)); err != nil {
		return nil, err
	}
	return row, nil
}

func (p *Postgres) GetType() string {
	return "postgres"
}

func getPostgresFieldAlterDataType(table string, f Field) string {
	dataTypes := postgresDataTypes

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
		if v, ok := dataTypes[baseDataType]; ok {
			if v == "BOOLEAN" {
				switch f.Default {
				case "0":
					f.Default = "FALSE"
				case "1":
					f.Default = "TRUE"
				}
			}
		}
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
	if f.Extra != "" && strings.ToUpper(f.Extra) == "AUTO_INCREMENT" {
		if strings.ToUpper(f.Extra) == "AUTO_INCREMENT" {
			baseDataType = "serial"
		}
	}
	fieldName := f.Name
	switch baseDataType {
	case "int", "integer", "smallint", "bigint", "int2", "int4", "int8":
		sql := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s USING %s::%s;", table, fieldName, dataTypes[baseDataType], fieldName, dataTypes[baseDataType])
		if defaultVal != "" {
			sql += fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET %s;", table, fieldName, defaultVal)
		}
		return sql
	case "float", "double", "decimal", "numeric":
		if f.Length == 0 {
			f.Length = 11
		}
		if f.Precision == 0 {
			f.Precision = 2
		}
		sql := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s(%d,%d) USING %s::%s;", table, fieldName, dataTypes[baseDataType], f.Length, f.Precision, fieldName, dataTypes[baseDataType])
		if defaultVal != "" {
			sql += fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET %s;", table, fieldName, defaultVal)
		}
		return sql
	case "string", "varchar", "character varying", "char", "character":
		if f.Length == 0 {
			f.Length = 255
		}
		sql := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s(%d) USING %s::%s;", table, fieldName, dataTypes[baseDataType], f.Length, fieldName, dataTypes[baseDataType])
		if defaultVal != "" {
			sql += fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET %s;", table, fieldName, defaultVal)
		}
		return sql
	case "serial":
		sql := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s USING %s::integer;", table, fieldName, "integer", fieldName)
		sql += fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET %s;", table, fieldName, "DEFAULT nextval('"+table+"_"+fieldName+"_seq'::regclass)")
		return sql
	case "bigserial":
		sql := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s USING %s::bigint;", table, fieldName, "bigint", fieldName)
		sql += fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET %s;", table, fieldName, "DEFAULT nextval('"+table+"_"+fieldName+"_seq'::regclass)")
		return sql
	default:
		sql := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s USING %s::%s;", table, fieldName, dataTypes[baseDataType], fieldName, dataTypes[baseDataType])
		if defaultVal != "" {
			sql += fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET %s;", table, fieldName, defaultVal)
		}
		return sql
	}
}

// normalizePostgresDataType normalizes PostgreSQL data types to their canonical form for comparison
func normalizePostgresDataType(dataType string) string {
	// Parse the data type to get the base type
	baseType, _, _ := parseDataTypeWithParameters(dataType)

	// Map common aliases to canonical forms
	switch strings.ToLower(baseType) {
	case "varchar", "character varying":
		return "varchar"
	case "char", "character":
		return "char"
	case "int", "integer":
		return "integer"
	case "int2":
		return "smallint"
	case "int4":
		return "integer"
	case "int8":
		return "bigint"
	case "float", "real":
		return "real"
	case "double", "double precision":
		return "double precision"
	case "decimal", "numeric":
		return "numeric"
	case "bool", "boolean":
		return "boolean"
	case "timestamp", "timestamp without time zone":
		return "timestamp"
	case "timestamptz", "timestamp with time zone":
		return "timestamp with time zone"
	case "time", "time without time zone":
		return "time"
	case "timetz", "time with time zone":
		return "time with time zone"
	default:
		return strings.ToLower(baseType)
	}
}

// postgresFieldsEqual compares two fields for PostgreSQL, handling data type normalization
func postgresFieldsEqual(newField, existingField Field) bool {
	// Compare basic properties
	if newField.Name != existingField.Name {
		return false
	}
	if !strings.EqualFold(newField.IsNullable, existingField.IsNullable) {
		return false
	}

	// Compare keys (PRI, UNI, MUL) - but be more lenient
	newKey := strings.ToUpper(strings.TrimSpace(newField.Key))
	existingKey := strings.ToUpper(strings.TrimSpace(existingField.Key))

	if newKey != "" && existingKey != "" && newKey != existingKey {
		return false
	}

	// Parse and normalize data types for comparison
	newNormalizedType := normalizePostgresDataType(newField.DataType)
	existingNormalizedType := normalizePostgresDataType(existingField.DataType)

	if newNormalizedType != existingNormalizedType {
		return false
	}

	// Compare lengths (only for types that use length)
	_, newLength, newPrecision := parseDataTypeWithParameters(newField.DataType)
	_, existingLength, existingPrecision := parseDataTypeWithParameters(existingField.DataType)

	// Compare lengths (only for types that use length)
	if newLength > 0 && existingLength > 0 && newLength != existingLength {
		// For varchar/char types, length differences matter
		if newNormalizedType == "varchar" || newNormalizedType == "char" {
			return false
		}
	}

	// Compare precision (for decimal/numeric types)
	if newPrecision != existingPrecision && (newNormalizedType == "numeric" || newNormalizedType == "decimal") {
		return false
	}

	// Compare defaults (normalize for comparison)
	newDefault := normalizeDefault(newField.Default)
	existingDefault := normalizeDefault(existingField.Default)
	if newDefault != existingDefault {
		return false
	}

	// Compare comments (only if both have comments)
	newHasComment := strings.TrimSpace(newField.Comment) != ""
	existingHasComment := strings.TrimSpace(existingField.Comment) != ""

	if existingHasComment && newHasComment {
		if newField.Comment != existingField.Comment {
			return false
		}
	} else if existingHasComment && !newHasComment {
		return false
	}

	// Compare extra properties (like AUTO_INCREMENT)
	if !strings.EqualFold(newField.Extra, existingField.Extra) {
		return false
	}

	return true
}

// getExistingConstraints retrieves all existing constraint names for a table
func (p *Postgres) getExistingConstraints(table string) ([]string, error) {
	var constraints []string
	err := p.client.Select(&constraints, `
		SELECT conname
		FROM pg_constraint c
		JOIN pg_class t ON c.conrelid = t.oid
		WHERE t.relname = $1 AND t.relkind = 'r'
		AND c.contype IN ('c', 'f', 'p', 'u')`, table) // Only check, foreign key, primary key, and unique constraints
	return constraints, err
}

func (p *Postgres) alterFieldSQL(table string, f, existingField Field) string {
	// First check if fields are functionally equivalent using PostgreSQL-specific comparison
	if postgresFieldsEqual(f, existingField) {
		return ""
	}

	// Fields are different, generate alter SQL
	return getPostgresFieldAlterDataType(table, f)
}

func (p *Postgres) createSQL(table string, newFields []Field, constraints *Constraint) (string, error) {
	var sql string
	var query, comments, indexQuery, constraintQuery, primaryKeys []string

	// Deterministic: sort fields by name
	sort.SliceStable(newFields, func(i, j int) bool {
		return strings.ToLower(newFields[i].Name) < strings.ToLower(newFields[j].Name)
	})

	for _, field := range newFields {
		fieldName := field.Name
		if strings.ToUpper(field.Key) == "PRI" {
			primaryKeys = append(primaryKeys, fieldName)
		}
		query = append(query, p.FieldAsString(field, "column"))
		if field.Comment != "" {
			comment := "COMMENT ON COLUMN " + table + "." + fieldName + " IS '" + strings.ReplaceAll(field.Comment, "'", `"`) + "';"
			comments = append(comments, comment)
		}
	}

	// Handle constraints
	if constraints != nil {
		// Handle indices
		if len(constraints.Indices) > 0 {
			tmp := make([]Indices, 0, len(constraints.Indices))
			for _, idx := range constraints.Indices {
				cpy := idx
				if cpy.Name == "" {
					cpy.Name = "idx_" + table + "_" + strings.Join(cpy.Columns, "_")
				}
				tmp = append(tmp, cpy)
			}
			sort.SliceStable(tmp, func(i, j int) bool {
				return strings.ToLower(tmp[i].Name) < strings.ToLower(tmp[j].Name)
			})
			for _, index := range tmp {
				if index.Unique {
					q := fmt.Sprintf(postgresQueries["create_unique_index"], index.Name, table, strings.Join(index.Columns, ", "))
					indexQuery = append(indexQuery, q)
				} else {
					q := fmt.Sprintf(postgresQueries["create_index"], index.Name, table, strings.Join(index.Columns, ", "))
					indexQuery = append(indexQuery, q)
				}
			}
		}

		// Handle unique constraints
		for _, unique := range constraints.UniqueKeys {
			if unique.Name == "" {
				unique.Name = "uk_" + table + "_" + strings.Join(unique.Columns, "_")
			}
			q := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s);", table, unique.Name, strings.Join(unique.Columns, ", "))
			constraintQuery = append(constraintQuery, q)
		}

		// Handle check constraints
		for _, check := range constraints.CheckKeys {
			if check.Name == "" {
				check.Name = "ck_" + table + "_" + strings.ReplaceAll(check.Expression, " ", "_")
			}
			q := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s);", table, check.Name, check.Expression)
			constraintQuery = append(constraintQuery, q)
		}

		// Handle primary key constraints
		for _, pk := range constraints.PrimaryKeys {
			if pk.Name == "" {
				pk.Name = "pk_" + table + "_" + strings.Join(pk.Columns, "_")
			}
			q := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s);", table, pk.Name, strings.Join(pk.Columns, ", "))
			constraintQuery = append(constraintQuery, q)
		}

		// Handle foreign key constraints
		for _, fk := range constraints.ForeignKeys {
			if fk.Name == "" {
				fk.Name = "fk_" + table + "_" + fk.ReferencedTable + "_" + strings.Join(fk.ReferencedColumn, "_")
			}
			q := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
				table, fk.Name, strings.Join(fk.Column, ", "), fk.ReferencedTable, strings.Join(fk.ReferencedColumn, ", "))
			if fk.OnDelete != "" {
				q += " ON DELETE " + strings.ToUpper(fk.OnDelete)
			}
			if fk.OnUpdate != "" {
				q += " ON UPDATE " + strings.ToUpper(fk.OnUpdate)
			}
			q += ";"
			constraintQuery = append(constraintQuery, q)
		}
	}

	if len(primaryKeys) > 0 {
		query = append(query, " PRIMARY KEY ("+strings.Join(primaryKeys, ", ")+")")
	}
	if len(query) > 0 {
		fieldsToUpdate := strings.Join(query, ", ")
		sql = fmt.Sprintf(postgresQueries["create_table"], table) + " (" + fieldsToUpdate + ");"
	}
	if len(comments) > 0 {
		sql += strings.Join(comments, "")
	}
	if len(indexQuery) > 0 {
		sql += strings.Join(indexQuery, "")
	}
	return sql, nil
}

func (p *Postgres) alterSQL(table string, newFields []Field, constraints *Constraint) (string, error) {
	var sql []string
	alterTable := "ALTER TABLE " + table
	existingFields, err := p.GetFields(table)
	if err != nil {
		return "", err
	}
	existingIndices, err := p.GetTheIndices(table)
	if err != nil {
		return "", err
	}

	// If no constraints are provided, don't generate any constraint-related SQL
	if constraints == nil {
		constraints = &Constraint{}
	}

	// Deterministic: sort fields by name
	sort.SliceStable(newFields, func(i, j int) bool {
		return strings.ToLower(newFields[i].Name) < strings.ToLower(newFields[j].Name)
	})

	// First pass: add/modify fields (excluding renames)
	for _, nf := range newFields {
		newField := nf
		if newField.IsNullable == "" {
			newField.IsNullable = "YES"
		}
		if newField.OldName != "" {
			continue
		}
		fieldName := newField.Name
		fieldExists := false
		for _, existingField := range existingFields {
			if existingField.Name == fieldName {
				fieldExists = true

				// Prefer canonical SQL comparison for data type/length/default/etc.
				qry := p.alterFieldSQL(table, newField, existingField)
				if qry != "" {
					sql = append(sql, qry)
				}

				// Nullability delta (handled separately)
				if existingField.IsNullable != newField.IsNullable {
					if newField.IsNullable == "YES" {
						sql = append(sql, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL;", table, fieldName))
					} else {
						sql = append(sql, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL;", table, fieldName))
					}
				}

				// Comment delta (deterministic)
				if existingField.Comment != newField.Comment {
					sql = append(sql, "COMMENT ON COLUMN "+table+"."+fieldName+" IS '"+strings.ReplaceAll(newField.Comment, "'", `"`)+"';")
				}
				break
			}
		}
		if !fieldExists {
			qry := alterTable + " " + p.FieldAsString(newField, "add_column") + ";"
			if qry != "" {
				sql = append(sql, qry)
			}
		}
	}

	// Second pass: handle renames deterministically by OldName
	var renames []Field
	for _, nf := range newFields {
		if nf.OldName != "" {
			renames = append(renames, nf)
		}
	}
	sort.SliceStable(renames, func(i, j int) bool {
		return strings.ToLower(renames[i].OldName) < strings.ToLower(renames[j].OldName)
	})
	for _, newField := range renames {
		fieldName := newField.Name
		sql = append(sql, alterTable+` RENAME COLUMN "`+newField.OldName+`" TO "`+fieldName+`";`)
	}

	// Handle constraints
	if constraints != nil {
		// Handle indices
		if len(constraints.Indices) > 0 {
			tmp := make([]Indices, 0, len(constraints.Indices))
			for _, idx := range constraints.Indices {
				cpy := idx
				if cpy.Name == "" {
					cpy.Name = "idx_" + table + "_" + strings.Join(cpy.Columns, "_")
				}
				tmp = append(tmp, cpy)
			}
			sort.SliceStable(tmp, func(i, j int) bool {
				return strings.ToLower(tmp[i].Name) < strings.ToLower(tmp[j].Name)
			})
			for _, newIndex := range tmp {
				// Check if index already exists with same name and properties
				indexExists := false
				for _, existingIndex := range existingIndices {
					if strings.EqualFold(existingIndex.Name, newIndex.Name) {
						if reflect.DeepEqual(existingIndex.Columns, newIndex.Columns) && existingIndex.Unique == newIndex.Unique {
							indexExists = true
						} else {
							// Index exists but is different - drop and recreate
							sql = append(sql, fmt.Sprintf("DROP INDEX %s;", existingIndex.Name))
						}
						break
					}
				}

				if !indexExists {
					// New index or needs recreation
					if newIndex.Unique {
						sql = append(sql, fmt.Sprintf(postgresQueries["create_unique_index"], newIndex.Name, table, strings.Join(newIndex.Columns, ", ")))
					} else {
						sql = append(sql, fmt.Sprintf(postgresQueries["create_index"], newIndex.Name, table, strings.Join(newIndex.Columns, ", ")))
					}
				}
			}
		}

		// Get existing foreign keys to check for duplicates
		existingForeignKeys, err := p.GetForeignKeys(table)
		if err != nil {
			// If we can't get existing foreign keys, skip constraint checks
			existingForeignKeys = []ForeignKey{}
		}

		// Get existing constraints to check for duplicates
		existingConstraints, err := p.getExistingConstraints(table)
		if err != nil {
			// If we can't get existing constraints, skip constraint checks
			existingConstraints = []string{}
		}

		// Handle unique constraints - check if they already exist
		for _, unique := range constraints.UniqueKeys {
			if unique.Name == "" {
				unique.Name = "uk_" + table + "_" + strings.Join(unique.Columns, "_")
			}
			// Check if constraint already exists
			constraintExists := false
			for _, existingConstraint := range existingConstraints {
				if strings.EqualFold(existingConstraint, unique.Name) {
					constraintExists = true
					break
				}
			}
			if !constraintExists {
				q := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s);", table, unique.Name, strings.Join(unique.Columns, ", "))
				sql = append(sql, q)
			}
		}

		// Handle check constraints - check if they already exist
		for _, check := range constraints.CheckKeys {
			if check.Name == "" {
				check.Name = "ck_" + table + "_" + strings.ReplaceAll(check.Expression, " ", "_")
			}
			// Check if constraint already exists
			constraintExists := false
			for _, existingConstraint := range existingConstraints {
				if strings.EqualFold(existingConstraint, check.Name) {
					constraintExists = true
					break
				}
			}
			if !constraintExists {
				q := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s);", table, check.Name, check.Expression)
				sql = append(sql, q)
			}
		}

		// Handle primary key constraints - check if they already exist
		for _, pk := range constraints.PrimaryKeys {
			if pk.Name == "" {
				pk.Name = "pk_" + table + "_" + strings.Join(pk.Columns, "_")
			}
			// Check if constraint already exists
			constraintExists := false
			for _, existingConstraint := range existingConstraints {
				if strings.EqualFold(existingConstraint, pk.Name) {
					constraintExists = true
					break
				}
			}
			if !constraintExists {
				q := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s);", table, pk.Name, strings.Join(pk.Columns, ", "))
				sql = append(sql, q)
			}
		}

		// Handle foreign key constraints - check if they already exist
		for _, fk := range constraints.ForeignKeys {
			if fk.Name == "" {
				fk.Name = "fk_" + table + "_" + fk.ReferencedTable + "_" + strings.Join(fk.ReferencedColumn, "_")
			}

			// Check if foreign key already exists
			fkExists := false
			for _, existingFK := range existingForeignKeys {
				if strings.EqualFold(existingFK.Name, fk.Name) ||
					(strings.EqualFold(existingFK.ReferencedTable, fk.ReferencedTable) &&
						reflect.DeepEqual(existingFK.ReferencedColumn, fk.ReferencedColumn)) {
					fkExists = true
					break
				}
			}

			if !fkExists {
				q := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
					table, fk.Name, strings.Join(fk.Column, ", "), fk.ReferencedTable, strings.Join(fk.ReferencedColumn, ", "))
				if fk.OnDelete != "" {
					q += " ON DELETE " + strings.ToUpper(fk.OnDelete)
				}
				if fk.OnUpdate != "" {
					q += " ON UPDATE " + strings.ToUpper(fk.OnUpdate)
				}
				q += ";"
				sql = append(sql, q)
			}
		}
	}

	if len(sql) > 0 {
		return strings.Join(sql, ""), nil
	}
	return "", nil
}

func (p *Postgres) GenerateSQL(table string, newFields []Field, constraints *Constraint) (string, error) {
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
		return p.createSQL(table, newFields, constraints)
	}
	return p.alterSQL(table, newFields, constraints)
}

func (p *Postgres) Migrate(table string, dst DataSource) error {
	fields, err := p.GetFields(table)
	if err != nil {
		return err
	}
	_, err = dst.GenerateSQL(table, fields, nil)
	if err != nil {
		return err
	}
	return nil
}

func (p *Postgres) Close() error {
	return p.client.Close()
}

func (p *Postgres) Begin() (squealx.SQLTx, error) {
	return p.client.Begin()
}

func (p *Postgres) FieldAsString(f Field, action string) string {
	sqlPattern := postgresQueries
	dataTypes := postgresDataTypes
	nullable := "NULL"
	defaultVal := ""
	comment := ""
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
		// Fallback to VARCHAR for unknown types
		mappedDataType = "VARCHAR"
		actualDataType = "varchar"
	}

	if strings.ToUpper(f.IsNullable) == "NO" {
		nullable = "NOT NULL"
	}
	if f.Default != nil {
		if mappedDataType == "BOOLEAN" {
			switch f.Default {
			case "0":
				f.Default = "FALSE"
			case "1":
				f.Default = "TRUE"
			}
		}
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
	if defaultVal == "DEFAULT '0000-00-00 00:00:00'" {
		nullable = "NULL"
		defaultVal = "DEFAULT NULL"
	}
	if f.Key != "" && strings.ToUpper(f.Key) == "PRI" && action != "column" {
		primaryKey = "PRIMARY KEY"
	}
	if f.Extra != "" && strings.ToUpper(f.Extra) == "AUTO_INCREMENT" {
		if strings.ToUpper(f.Extra) == "AUTO_INCREMENT" {
			actualDataType = "serial"
			if action != "column" {
				primaryKey = "PRIMARY KEY"
			}
		}
	}
	fieldName := f.Name
	switch actualDataType {
	case "string", "varchar", "character varying", "char", "character":
		if f.Length == 0 {
			f.Length = 255
		}
		changeColumn := sqlPattern[action] + "(%d) %s %s %s %s %s"
		return strings.TrimSpace(space.ReplaceAllString(fmt.Sprintf(changeColumn, fieldName, mappedDataType, f.Length, nullable, primaryKey, autoIncrement, defaultVal, comment), " "))
	case "smallint", "int", "integer", "bigint", "big_integer", "bigInteger", "int2", "int4", "int8":
		changeColumn := sqlPattern[action] + " %s %s %s %s %s"
		return strings.TrimSpace(space.ReplaceAllString(fmt.Sprintf(changeColumn, fieldName, mappedDataType, nullable, primaryKey, autoIncrement, defaultVal, comment), " "))
	case "float", "double", "decimal", "numeric":
		if f.Length == 0 {
			f.Length = 11
		}
		if f.Precision == 0 {
			f.Precision = 2
		}
		changeColumn := sqlPattern[action] + "(%d, %d) %s %s %s %s %s"
		return strings.TrimSpace(space.ReplaceAllString(fmt.Sprintf(changeColumn, fieldName, mappedDataType, f.Length, f.Precision, nullable, primaryKey, autoIncrement, defaultVal, comment), " "))
	default:
		changeColumn := sqlPattern[action] + " %s %s %s %s %s"
		return strings.TrimSpace(space.ReplaceAllString(fmt.Sprintf(changeColumn, fieldName, mappedDataType, nullable, primaryKey, autoIncrement, defaultVal, comment), " "))
	}
}

func NewPostgres(id, dsn, database string, disableLog bool, pooling ConnectionPooling) *Postgres {
	return &Postgres{
		schema:     database,
		id:         id,
		dsn:        dsn,
		client:     nil,
		disableLog: disableLog,
		pooling:    pooling,
	}
}
