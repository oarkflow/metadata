package metadata

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/dbresolver"
	"github.com/oarkflow/squealx/drivers/mssql"
	"github.com/oarkflow/squealx/orm"
)

type MsSQL struct {
	schema     string
	dsn        string
	id         string
	client     dbresolver.DBResolver
	disableLog bool
	pooling    ConnectionPooling
	config     Config
}

var mssqlQueries = map[string]string{
	"create_table":        "CREATE TABLE %s",
	"alter_table":         "ALTER TABLE %s",
	"column":              "[%s] %s",
	"add_column":          "ADD [%s] %s",
	"change_column":       "ALTER COLUMN [%s] %s",
	"remove_column":       "ALTER COLUMN [%s] %s",
	"create_unique_index": "CREATE UNIQUE INDEX %s ON %s (%s);",
	"create_index":        "CREATE INDEX %s ON %s (%s);",
}

var mssqlDataTypes = map[string]string{
	// Integer types
	"bit":      "BIT",
	"tinyint":  "TINYINT",
	"smallint": "SMALLINT",
	"int":      "INT",
	"integer":  "INT",
	"int4":     "INT", // PostgreSQL int4
	"bigint":   "BIGINT",
	"int8":     "BIGINT",   // PostgreSQL int8
	"int2":     "SMALLINT", // PostgreSQL int2

	// MySQL integer types -> SQL Server equivalents
	"mediumint": "INT", // MySQL mediumint -> SQL Server int

	// PostgreSQL serial types -> SQL Server IDENTITY equivalents
	"serial":      "INT",      // Will be handled specially for IDENTITY
	"serial4":     "INT",      // Will be handled specially for IDENTITY
	"bigserial":   "BIGINT",   // Will be handled specially for IDENTITY
	"serial8":     "BIGINT",   // Will be handled specially for IDENTITY
	"smallserial": "SMALLINT", // Will be handled specially for IDENTITY
	"serial2":     "SMALLINT", // Will be handled specially for IDENTITY

	// Floating point types
	"float":            "FLOAT",
	"float4":           "REAL",  // PostgreSQL float4
	"float8":           "FLOAT", // PostgreSQL float8
	"real":             "REAL",
	"double":           "FLOAT",
	"double precision": "FLOAT",
	"decimal":          "DECIMAL",
	"numeric":          "NUMERIC",
	"money":            "MONEY",
	"smallmoney":       "SMALLMONEY",

	// String types (Unicode)
	"nchar":    "NCHAR",
	"nvarchar": "NVARCHAR",
	"ntext":    "NTEXT",
	"string":   "NVARCHAR",
	"varchar":  "NVARCHAR",
	"text":     "NTEXT",

	// String types (Non-Unicode)
	"char":              "CHAR",
	"character":         "CHAR", // PostgreSQL character
	"varchar_ansi":      "VARCHAR",
	"text_ansi":         "TEXT",
	"character varying": "NVARCHAR", // PostgreSQL character varying -> Unicode

	// MySQL string types -> SQL Server equivalents
	"tinytext":   "NTEXT",
	"mediumtext": "NTEXT",
	"longtext":   "NTEXT",
	"longText":   "NTEXT",
	"LongText":   "NTEXT",

	// Binary types
	"binary":     "BINARY",
	"varbinary":  "VARBINARY",
	"image":      "IMAGE",
	"bytea":      "VARBINARY(MAX)", // PostgreSQL bytea
	"blob":       "VARBINARY(MAX)", // MySQL blob types
	"tinyblob":   "VARBINARY(MAX)",
	"mediumblob": "VARBINARY(MAX)",
	"longblob":   "VARBINARY(MAX)",

	// Date and time types
	"date":                     "DATE",
	"time":                     "TIME",
	"datetime":                 "DATETIME2",
	"datetime2":                "DATETIME2",
	"smalldatetime":            "SMALLDATETIME",
	"timestamp":                "DATETIME2",
	"timestamptz":              "DATETIMEOFFSET", // PostgreSQL timestamptz
	"timestamp with time zone": "DATETIMEOFFSET",
	"time with time zone":      "DATETIMEOFFSET", // Closest equivalent
	"timetz":                   "DATETIMEOFFSET", // PostgreSQL timetz
	"datetimeoffset":           "DATETIMEOFFSET",
	"interval":                 "NVARCHAR(50)", // PostgreSQL interval -> VARCHAR
	"year":                     "SMALLINT",     // MySQL year

	// Boolean types
	"bool":    "BIT",
	"boolean": "BIT",

	// XML type
	"xml": "XML",

	// Spatial types
	"geometry":  "GEOMETRY",
	"geography": "GEOGRAPHY",

	// PostgreSQL geometric types -> SQL Server equivalents
	"point":   "GEOMETRY",
	"line":    "GEOMETRY",
	"lseg":    "GEOMETRY",
	"box":     "GEOMETRY",
	"path":    "GEOMETRY",
	"polygon": "GEOMETRY",
	"circle":  "GEOMETRY",

	// MySQL geometric types -> SQL Server equivalents
	"linestring":         "GEOMETRY",
	"multipoint":         "GEOMETRY",
	"multilinestring":    "GEOMETRY",
	"multipolygon":       "GEOMETRY",
	"geometrycollection": "GEOMETRY",

	// Network types (PostgreSQL) -> NVARCHAR equivalents
	"inet":     "NVARCHAR(45)", // IP address
	"cidr":     "NVARCHAR(43)", // CIDR notation
	"macaddr":  "NVARCHAR(17)", // MAC address
	"macaddr8": "NVARCHAR(23)", // EUI-64 MAC address

	// Array and range types (PostgreSQL) -> NVARCHAR equivalents
	"array":     "NVARCHAR(MAX)", // Store as JSON
	"int4range": "NVARCHAR(255)",
	"int8range": "NVARCHAR(255)",
	"numrange":  "NVARCHAR(255)",
	"tsrange":   "NVARCHAR(255)",
	"tstzrange": "NVARCHAR(255)",
	"daterange": "NVARCHAR(255)",

	// Text search types (PostgreSQL) -> NVARCHAR equivalents
	"tsvector": "NVARCHAR(MAX)",
	"tsquery":  "NVARCHAR(MAX)",

	// Bit string types (PostgreSQL)
	"bit varying": "VARBINARY",
	"varbit":      "VARBINARY",

	// Other types
	"uniqueidentifier": "UNIQUEIDENTIFIER",
	"guid":             "UNIQUEIDENTIFIER",
	"uuid":             "UNIQUEIDENTIFIER", // PostgreSQL UUID
	"sql_variant":      "SQL_VARIANT",
	"hierarchyid":      "HIERARCHYID",
	"cursor":           "CURSOR",
	"table":            "TABLE",

	// SQLite affinity types -> SQL Server equivalents
	"clob":              "NTEXT",
	"varying character": "NVARCHAR",
	"native character":  "NCHAR",
	"unsigned big int":  "BIGINT",

	// MySQL specific types -> SQL Server equivalents
	"enum": "NVARCHAR(255)", // MySQL ENUM -> NVARCHAR
	"set":  "NVARCHAR(255)", // MySQL SET -> NVARCHAR

	// JSON types
	"json":  "NVARCHAR(MAX)", // JSON is stored as NVARCHAR in SQL Server
	"jsonb": "NVARCHAR(MAX)", // PostgreSQL binary JSON
}

func (p *MsSQL) Connect() (DataSource, error) {
	if p.client == nil {
		db1, err := mssql.Open(p.dsn, p.id)
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

func (p *MsSQL) GetSources(database ...string) (tables []Source, err error) {
	db := p.schema
	if len(database) > 0 {
		db = database[0]
	}
	err = p.client.Select(&tables, `
		SELECT
			TABLE_NAME as name,
			TABLE_TYPE as type
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_CATALOG = ?`, db)
	return
}

func (p *MsSQL) Config() Config {
	return p.config
}

func (p *MsSQL) GetDataTypeMap(dataType string) string {
	// Parse data type to handle cases like varchar(255), numeric(10,2), etc.
	baseDataType, _, _ := parseDataTypeWithParameters(dataType)

	if v, ok := mssqlDataTypes[baseDataType]; ok {
		return v
	}
	return "NVARCHAR"
}

func (p *MsSQL) GetTables(database ...string) (tables []Source, err error) {
	db := p.schema
	if len(database) > 0 {
		db = database[0]
	}
	err = p.client.Select(&tables, `
		SELECT
			TABLE_NAME as name,
			TABLE_TYPE as type
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_CATALOG = ? AND TABLE_TYPE = 'BASE TABLE'`, db)
	return
}

func (p *MsSQL) GetViews(database ...string) (tables []Source, err error) {
	db := p.schema
	if len(database) > 0 {
		db = database[0]
	}
	err = p.client.Select(&tables, `
		SELECT
			TABLE_NAME as name,
			VIEW_DEFINITION as definition
		FROM INFORMATION_SCHEMA.VIEWS
		WHERE TABLE_CATALOG = ?`, db)
	return
}

func (p *MsSQL) Client() any {
	return p.client
}

func (p *MsSQL) GetDBName(database ...string) string {
	db := p.schema
	if len(database) > 0 {
		db = database[0]
	}
	return db
}

func (p *MsSQL) Store(table string, val any) error {
	_, err := p.client.Exec(orm.InsertQuery(table, val), val)
	return err
}

func (p *MsSQL) StoreInBatches(table string, val any, size int) error {
	return processBatchInsert(p.client, table, val, size)
}

func (p *MsSQL) GetFields(table string, database ...string) (fields []Field, err error) {
	db := p.schema
	if len(database) > 0 {
		db = database[0]
	}
	var fieldMaps []map[string]any
	err = p.client.Select(&fieldMaps, `
		SELECT
			COLUMN_NAME as [name],
			COLUMN_DEFAULT as [default],
			IS_NULLABLE as [is_nullable],
			DATA_TYPE as type,
			COALESCE(NUMERIC_PRECISION, CHARACTER_MAXIMUM_LENGTH) as [length],
			NUMERIC_SCALE as [precision],
			'' as [comment],
			CASE
				WHEN pk.COLUMN_NAME IS NOT NULL THEN 'PRI'
				ELSE ''
			END as [key],
			CASE
				WHEN COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA+'.'+TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1 THEN 'auto_increment'
				ELSE ''
			END as extra
		FROM INFORMATION_SCHEMA.COLUMNS c
		LEFT JOIN (
			SELECT ku.TABLE_CATALOG, ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME
			FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
			INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ku
				ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
				AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
		) pk ON c.TABLE_CATALOG = pk.TABLE_CATALOG
			AND c.TABLE_SCHEMA = pk.TABLE_SCHEMA
			AND c.TABLE_NAME = pk.TABLE_NAME
			AND c.COLUMN_NAME = pk.COLUMN_NAME
		WHERE c.TABLE_NAME = ? AND c.TABLE_CATALOG = ?
		ORDER BY c.ORDINAL_POSITION`, table, db)
	if err != nil {
		return
	}
	bt, err := json.Marshal(fieldMaps)
	if err != nil {
		return
	}
	err = json.Unmarshal(bt, &fields)
	if err != nil {
		return
	}
	// Normalize default values for existing fields
	for i := range fields {
		if def, ok := fields[i].Default.(string); ok {
			if len(def) > 2 && def[0] == '\'' && def[len(def)-1] == '\'' {
				fields[i].Default = def[1 : len(def)-1]
			}
		}
	}
	return
}

func (p *MsSQL) GetForeignKeys(table string, database ...string) (fields []ForeignKey, err error) {
	db := p.schema
	if len(database) > 0 {
		db = database[0]
	}
	err = p.client.Select(&fields, `
		SELECT
			kcu.CONSTRAINT_NAME as [name],
			kcu.COLUMN_NAME as [column],
			kcu.REFERENCED_TABLE_NAME as [referenced_table],
			kcu.REFERENCED_COLUMN_NAME as [referenced_column]
		FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
		LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
			ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
		WHERE kcu.TABLE_NAME = ? AND kcu.TABLE_CATALOG = ?`, table, db)
	return
}

func (p *MsSQL) GetIndices(table string, database ...string) (fields []Index, err error) {
	err = p.client.Select(&fields, `
		SELECT DISTINCT
			i.name as name,
			c.name as column_name,
			CASE WHEN c.is_nullable = 1 THEN 1 ELSE 0 END as [nullable]
		FROM sys.indexes i
		INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
		INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
		INNER JOIN sys.tables t ON i.object_id = t.object_id
		WHERE t.name = ?`, table)
	return
}

func (p *MsSQL) GetTheIndices(table string, database ...string) (fields []Indices, err error) {
	err = p.client.Select(&fields, `
		SELECT
			i.name as name,
			CASE WHEN i.is_unique = 1 THEN 0 ELSE 1 END as uniq,
			'[' + STRING_AGG('"' + c.name + '"', ',') WITHIN GROUP (ORDER BY ic.key_ordinal) + ']' as columns
		FROM sys.indexes i
		INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
		INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
		INNER JOIN sys.tables t ON i.object_id = t.object_id
		WHERE t.name = ? AND i.type > 0
		GROUP BY i.name, i.is_unique`, table)
	return
}

func (p *MsSQL) LastInsertedID() (id any, err error) {
	err = p.client.Select(&id, "SELECT SCOPE_IDENTITY();")
	return
}

func (p *MsSQL) MaxID(table, field string) (id any, err error) {
	err = p.client.Select(&id, fmt.Sprintf("SELECT MAX([%s]) FROM [%s];", field, table))
	return
}

func (p *MsSQL) GetCollection(table string) ([]map[string]any, error) {
	var rows []map[string]any
	err := p.client.Select(&rows, "SELECT * FROM ["+table+"]")
	return rows, err
}

func (p *MsSQL) Close() error {
	return p.client.Close()
}

func (p *MsSQL) Exec(sql string, values ...any) error {
	_, err := p.client.Exec(sql, values...)
	return err
}

func (p *MsSQL) Begin() (squealx.SQLTx, error) {
	return p.client.Begin()
}

func (p *MsSQL) GetRawCollection(query string, params ...map[string]any) ([]map[string]any, error) {
	var rows []map[string]any
	if len(params) > 0 {
		param := params[0]
		if val, ok := param["preview"]; ok {
			preview := val.(bool)
			if preview {
				query = strings.Split(query, " ORDER BY ")[0] + " ORDER BY (SELECT NULL) OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY"
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

func (p *MsSQL) GetRawPaginatedCollection(query string, paging squealx.Paging, params ...map[string]any) squealx.PaginatedResponse {
	var rows []map[string]any
	return p.client.Paginate(query, &rows, paging, params...)
}

func (p *MsSQL) GetPaginated(table string, paging squealx.Paging) squealx.PaginatedResponse {
	var rows []map[string]any
	return p.client.Paginate("SELECT * FROM ["+table+"]", &rows, paging)
}

func (p *MsSQL) GetSingle(table string) (map[string]any, error) {
	var row map[string]any
	if err := p.client.Select(&row, fmt.Sprintf("SELECT TOP 1 * FROM [%s]", table)); err != nil {
		return nil, err
	}
	return row, nil
}

func (p *MsSQL) GetType() string {
	return "mssql"
}

func getMSSQLFieldAlterDataType(table string, f Field) string {
	dataTypes := mssqlDataTypes

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
	nullable := "NULL"
	if strings.ToUpper(f.IsNullable) == "NO" {
		nullable = "NOT NULL"
	}

	switch baseDataType {
	case "float", "double", "decimal", "numeric":
		if f.Length == 0 {
			f.Length = 18
		}
		if f.Precision == 0 {
			f.Precision = 2
		}
		if f.OldName != "" {
			return fmt.Sprintf("ALTER TABLE [%s] ALTER COLUMN [%s] %s(%d,%d) %s %s;", table, f.Name, dataTypes[baseDataType], f.Length, f.Precision, nullable, defaultVal)
		}
		return fmt.Sprintf("ALTER TABLE [%s] ALTER COLUMN [%s] %s(%d,%d) %s %s;", table, f.Name, dataTypes[baseDataType], f.Length, f.Precision, nullable, defaultVal)
	case "int", "integer":
		if f.OldName != "" {
			return fmt.Sprintf("ALTER TABLE [%s] ALTER COLUMN [%s] %s %s %s;", table, f.Name, dataTypes[baseDataType], nullable, defaultVal)
		}
		return fmt.Sprintf("ALTER TABLE [%s] ALTER COLUMN [%s] %s %s %s;", table, f.Name, dataTypes[baseDataType], nullable, defaultVal)
	case "string", "varchar", "text", "character varying", "char":
		if f.Length == 0 {
			f.Length = 255
		}
		if f.OldName != "" {
			return fmt.Sprintf("ALTER TABLE [%s] ALTER COLUMN [%s] %s(%d) %s %s;", table, f.Name, dataTypes[baseDataType], f.Length, nullable, defaultVal)
		}
		return fmt.Sprintf("ALTER TABLE [%s] ALTER COLUMN [%s] %s(%d) %s %s;", table, f.Name, dataTypes[baseDataType], f.Length, nullable, defaultVal)
	default:
		if f.OldName != "" {
			return fmt.Sprintf("ALTER TABLE [%s] ALTER COLUMN [%s] %s %s %s;", table, f.Name, dataTypes[baseDataType], nullable, defaultVal)
		}
		return fmt.Sprintf("ALTER TABLE [%s] ALTER COLUMN [%s] %s %s %s;", table, f.Name, dataTypes[baseDataType], nullable, defaultVal)
	}
}

// getExistingConstraints retrieves all existing constraint names for a table
func (p *MsSQL) getExistingConstraints(table string) ([]string, error) {
	var constraints []string
	err := p.client.Select(&constraints, `
		SELECT name
		FROM sys.objects
		WHERE parent_object_id = OBJECT_ID(?) AND type IN ('C', 'F', 'PK', 'UQ')`, table)
	return constraints, err
}

func (p *MsSQL) alterFieldSQL(table string, f, existingField Field) string {
	// First check if fields are functionally equivalent
	if fieldsEqual(f, existingField) {
		return ""
	}

	// Fields are different, generate alter SQL
	return getMSSQLFieldAlterDataType(table, f)
}

func (p *MsSQL) createSQL(table string, newFields []Field, constraints *Constraint) (string, error) {
	var sql string
	var query, indexQuery, primaryKeys []string
	for _, newField := range newFields {
		if strings.ToUpper(newField.Key) == "PRI" {
			primaryKeys = append(primaryKeys, "["+newField.Name+"]")
		}
		query = append(query, p.FieldAsString(newField, "column"))
	}
	// Handle constraints
	if constraints != nil {
		// Handle indices
		if len(constraints.Indices) > 0 {
			for _, index := range constraints.Indices {
				if index.Name == "" {
					index.Name = "idx_" + table + "_" + strings.Join(index.Columns, "_")
				}
				switch index.Unique {
				case true:
					query := fmt.Sprintf(mssqlQueries["create_unique_index"], index.Name, "["+table+"]",
						"["+strings.Join(index.Columns, "], [")+"]")
					indexQuery = append(indexQuery, query)
				case false:
					query := fmt.Sprintf(mssqlQueries["create_index"], index.Name, "["+table+"]",
						"["+strings.Join(index.Columns, "], [")+"]")
					indexQuery = append(indexQuery, query)
				}
			}
		}

		// Handle unique constraints
		for _, unique := range constraints.UniqueKeys {
			if unique.Name == "" {
				unique.Name = "uk_" + table + "_" + strings.Join(unique.Columns, "_")
			}
			q := fmt.Sprintf("ALTER TABLE [%s] ADD CONSTRAINT %s UNIQUE (%s);", table, unique.Name, strings.Join(unique.Columns, ", "))
			indexQuery = append(indexQuery, q)
		}

		// Handle check constraints
		for _, check := range constraints.CheckKeys {
			if check.Name == "" {
				check.Name = "ck_" + table + "_" + strings.ReplaceAll(check.Expression, " ", "_")
			}
			q := fmt.Sprintf("ALTER TABLE [%s] ADD CONSTRAINT %s CHECK (%s);", table, check.Name, check.Expression)
			indexQuery = append(indexQuery, q)
		}

		// Handle primary key constraints
		for _, pk := range constraints.PrimaryKeys {
			if pk.Name == "" {
				pk.Name = "pk_" + table + "_" + strings.Join(pk.Columns, "_")
			}
			q := fmt.Sprintf("ALTER TABLE [%s] ADD CONSTRAINT %s PRIMARY KEY (%s);", table, pk.Name, strings.Join(pk.Columns, ", "))
			indexQuery = append(indexQuery, q)
		}

		// Handle foreign key constraints
		for _, fk := range constraints.ForeignKeys {
			if fk.Name == "" {
				fk.Name = "fk_" + table + "_" + fk.ReferencedTable + "_" + strings.Join(fk.ReferencedColumn, "_")
			}
			q := fmt.Sprintf("ALTER TABLE [%s] ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES [%s] (%s)",
				table, fk.Name, strings.Join(fk.Column, ", "), fk.ReferencedTable, strings.Join(fk.ReferencedColumn, ", "))
			if fk.OnDelete != "" {
				q += " ON DELETE " + strings.ToUpper(fk.OnDelete)
			}
			if fk.OnUpdate != "" {
				q += " ON UPDATE " + strings.ToUpper(fk.OnUpdate)
			}
			q += ";"
			indexQuery = append(indexQuery, q)
		}
	}
	if len(primaryKeys) > 0 {
		query = append(query, " PRIMARY KEY ("+strings.Join(primaryKeys, ", ")+")")
	}
	if len(query) > 0 {
		fieldsToUpdate := strings.Join(query, ", ")
		sql = fmt.Sprintf(mssqlQueries["create_table"], "["+table+"]") + " (" + fieldsToUpdate + ");"
	}
	if len(indexQuery) > 0 {
		sql += strings.Join(indexQuery, "")
	}
	return sql, nil
}

func (p *MsSQL) alterSQL(table string, newFields []Field, constraints *Constraint) (string, error) {
	var sql []string
	alterTable := "ALTER TABLE [" + table + "]"
	existingFields, err := p.GetFields(table)
	if err != nil {
		return "", err
	}

	// Get existing indices
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
		fieldExists := false
		for _, existingField := range existingFields {
			if existingField.Name == newField.Name {
				fieldExists = true
				qry := p.alterFieldSQL(table, newField, existingField)
				if qry != "" {
					sql = append(sql, qry)
				} else if existingField.IsNullable != newField.IsNullable {
					// Only nullability changed
					sql = append(sql, fmt.Sprintf("%s ALTER COLUMN %s;", alterTable, p.FieldAsString(newField, "column")))
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

	// Second pass: handle renames deterministically by old name
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
		for _, existingField := range existingFields {
			if existingField.Name == newField.OldName {
				qry := p.alterFieldSQL(table, newField, existingField)
				if qry != "" {
					sql = append(sql, qry)
				}
				break
			}
		}
	}

	// Third pass: handle constraints - only create constraints that don't already exist
	if constraints != nil {
		// Handle indices
		if len(constraints.Indices) > 0 {
			// Deterministic: ensure index names then sort indices by name
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
				indexExists := false

				// Check if index already exists (by name or by columns)
				for _, existingIndex := range existingIndices {
					// First check by name
					if strings.EqualFold(existingIndex.Name, newIndex.Name) {
						if indicesEqual(existingIndex, newIndex) {
							indexExists = true
							break
						}
					}
					// Also check by columns (for unnamed indices that might match)
					if indicesEqual(existingIndex, newIndex) {
						indexExists = true
						break
					}
				}

				// Only create index if it doesn't exist
				if !indexExists {
					var indexSQL string
					if newIndex.Unique {
						indexSQL = fmt.Sprintf(mssqlQueries["create_unique_index"], newIndex.Name, "["+table+"]", "["+strings.Join(newIndex.Columns, "], [")+"]")
					} else {
						indexSQL = fmt.Sprintf(mssqlQueries["create_index"], newIndex.Name, "["+table+"]", "["+strings.Join(newIndex.Columns, "], [")+"]")
					}
					sql = append(sql, indexSQL)
				}
			}
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
				q := fmt.Sprintf("ALTER TABLE [%s] ADD CONSTRAINT %s UNIQUE (%s);", table, unique.Name, strings.Join(unique.Columns, ", "))
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
				q := fmt.Sprintf("ALTER TABLE [%s] ADD CONSTRAINT %s CHECK (%s);", table, check.Name, check.Expression)
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
				q := fmt.Sprintf("ALTER TABLE [%s] ADD CONSTRAINT %s PRIMARY KEY (%s);", table, pk.Name, strings.Join(pk.Columns, ", "))
				sql = append(sql, q)
			}
		}

		// Handle foreign key constraints - check if they already exist
		for _, fk := range constraints.ForeignKeys {
			if fk.Name == "" {
				fk.Name = "fk_" + table + "_" + fk.ReferencedTable + "_" + strings.Join(fk.ReferencedColumn, "_")
			}

			// Check if constraint already exists
			constraintExists := false
			for _, existingConstraint := range existingConstraints {
				if strings.EqualFold(existingConstraint, fk.Name) {
					constraintExists = true
					break
				}
			}

			if !constraintExists {
				q := fmt.Sprintf("ALTER TABLE [%s] ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES [%s] (%s)",
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

func (p *MsSQL) GenerateSQL(table string, newFields []Field, constraints *Constraint) (string, error) {
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

func (p *MsSQL) Migrate(table string, dst DataSource) error {
	fields, err := p.GetFields(table)
	if err != nil {
		return err
	}
	sql, err := dst.GenerateSQL(table, fields, nil)
	if err != nil {
		return err
	}
	fmt.Println(sql)
	return nil
}

func (p *MsSQL) FieldAsString(f Field, action string) string {
	sqlPattern := mssqlQueries
	dataTypes := mssqlDataTypes
	nullable := "NULL"
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
		// Fallback to NVARCHAR for unknown types
		mappedDataType = "NVARCHAR"
		actualDataType = "nvarchar"
	}

	// Handle PostgreSQL serial types specially - convert to IDENTITY
	switch actualDataType {
	case "serial", "serial4":
		mappedDataType = "INT"
		autoIncrement = "IDENTITY(1,1)"
	case "bigserial", "serial8":
		mappedDataType = "BIGINT"
		autoIncrement = "IDENTITY(1,1)"
	case "smallserial", "serial2":
		mappedDataType = "SMALLINT"
		autoIncrement = "IDENTITY(1,1)"
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
		autoIncrement = "IDENTITY(1,1)"
	}

	changeColumn := sqlPattern[action] + " %s %s %s %s"
	return strings.TrimSpace(space.ReplaceAllString(fmt.Sprintf(changeColumn, f.Name, mappedDataType, nullable, primaryKey, autoIncrement, defaultVal), " "))
}

func NewMsSQL(id, dsn, database string, disableLog bool, pooling ConnectionPooling) *MsSQL {
	return &MsSQL{
		schema:     database,
		dsn:        dsn,
		id:         id,
		client:     nil,
		disableLog: disableLog,
		pooling:    pooling,
	}
}
