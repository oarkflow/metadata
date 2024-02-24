package metadata

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/oarkflow/db"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type Postgres struct {
	schema     string
	dsn        string
	client     *gorm.DB
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
	"smallint":                 "SMALLINT",
	"int2":                     "SMALLINT",
	"int":                      "INT",
	"int4":                     "INT",
	"integer":                  "INT",
	"bigint":                   "BIGINT",
	"int8":                     "BIGINT",
	"float":                    "NUMERIC",
	"numeric":                  "NUMERIC",
	"double":                   "NUMERIC",
	"decimal":                  "NUMERIC",
	"tinyint":                  "BOOLEAN",
	"bool":                     "BOOLEAN",
	"boolean":                  "BOOLEAN",
	"string":                   "VARCHAR",
	"varchar":                  "VARCHAR",
	"character varying":        "VARCHAR",
	"year":                     "SMALLINT",
	"char":                     "CHAR",
	"character":                "CHAR",
	"text":                     "TEXT",
	"longText":                 "TEXT",
	"longtext":                 "TEXT",
	"LongText":                 "TEXT",
	"serial":                   "SERIAL",
	"serial4":                  "SERIAL",
	"bigserial":                "BIGSERIAL",
	"serial8":                  "BIGSERIAL",
	"datetime":                 "TIMESTAMPTZ",
	"date":                     "DATE",
	"time":                     "TIME",
	"timestamp":                "TIMESTAMP",
	"timestamptz":              "TIMESTAMPTZ",
	"timestamp with time zone": "TIMESTAMPTZ",
	"jsonb":                    "JSONB",
	"json":                     "JSON",
}

func (p *Postgres) Connect() (DataSource, error) {
	if p.client == nil {
		var logLevel logger.LogLevel
		if p.disableLog {
			logLevel = logger.Silent
		} else {
			logLevel = logger.Error
		}
		config := &gorm.Config{
			PrepareStmt:                              true,
			Logger:                                   logger.Default.LogMode(logLevel),
			DisableForeignKeyConstraintWhenMigrating: true,
		}
		db1, err := gorm.Open(postgres.Open(p.dsn), config)
		if err != nil {
			return nil, err
		}
		clientDB, err := db1.DB()
		if err != nil {
			return nil, err
		}
		clientDB.SetConnMaxLifetime(time.Duration(p.pooling.MaxLifetime) * time.Second)
		clientDB.SetConnMaxIdleTime(time.Duration(p.pooling.MaxIdleTime) * time.Second)
		clientDB.SetMaxOpenConns(p.pooling.MaxOpenCons)
		clientDB.SetMaxIdleConns(p.pooling.MaxIdleCons)
		p.client = db1
	}
	return p, nil
}

func (p *Postgres) GetSources() (tables []Source, err error) {
	err = p.client.Table("information_schema.tables").Select("table_name as name, table_type").Where("table_catalog = ? AND table_schema = 'public'", p.schema).Find(&tables).Error
	return
}

func (p *Postgres) GetDataTypeMap(dataType string) string {
	if v, ok := postgresDataTypes[dataType]; ok {
		return v
	}
	return "VARCHAR"
}

func (p *Postgres) GetTables() (tables []Source, err error) {
	err = p.client.Table("information_schema.tables").Select("table_name as name, table_type").Where("table_catalog = ? AND table_schema = 'public' AND table_type='BASE TABLE'", p.schema).Find(&tables).Error
	return
}

func (p *Postgres) GetViews() (tables []Source, err error) {
	err = p.client.Table("information_schema.views").Select("table_name as name, view_definition").Where("table_catalog = ? AND table_schema = 'public' AND table_type='VIEW'", p.schema).Find(&tables).Error
	return
}

func (p *Postgres) DB() (*sql.DB, error) {
	return p.client.DB()
}

func (p *Postgres) GetDBName() string {
	return p.schema
}

func (p *Postgres) Config() Config {
	return p.config
}

func (p *Postgres) GetFields(table string) (fields []Field, err error) {
	var fieldMaps []map[string]any
	err = p.client.Raw(`
SELECT c.column_name as "name", column_default as "default", is_nullable as "is_nullable", data_type as "type", CASE WHEN numeric_precision IS NOT NULL THEN numeric_precision ELSE character_maximum_length END as "length", numeric_scale as "precision",a.column_key as "key", b.comment, '' as extra
FROM INFORMATION_SCHEMA.COLUMNS c
LEFT JOIN (
select kcu.table_name,        'PRI' as column_key,        kcu.ordinal_position as position,        kcu.column_name as column_name
from information_schema.table_constraints tco
join information_schema.key_column_usage kcu       on kcu.constraint_name = tco.constraint_name      and kcu.constraint_schema = tco.constraint_schema      and kcu.constraint_name = tco.constraint_name where tco.constraint_type = 'PRIMARY KEY' and kcu.table_catalog = ? AND kcu.table_schema = 'public' AND kcu.table_name = ? order by kcu.table_schema,          kcu.table_name,          position          ) a
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
WHERE table_catalog = ? AND table_schema = 'public' AND c.table_name =  ?
) b ON c.table_name = b.table_name AND b.column_name = c.column_name
          WHERE c.table_catalog = ? AND c.table_schema = 'public' AND c.table_name =  ?
;`, p.schema, table, p.schema, table, p.schema, table).Scan(&fieldMaps).Error
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
	return p.client.Table(table).Create(val).Error
}

func (p *Postgres) StoreInBatches(table string, val any, size int) error {
	if size <= 0 {
		size = 100
	}
	return p.client.Table(table).CreateInBatches(val, size).Error
}

func (p *Postgres) LastInsertedID() (id any, err error) {
	err = p.client.Raw("SELECT LASTVAL();").Scan(&id).Error
	return
}

func (p *Postgres) MaxID(table, field string) (id any, err error) {
	err = p.client.Raw(fmt.Sprintf("SELECT MAX(%s) FROM %s;", field, table)).Scan(&id).Error
	return
}

func (p *Postgres) GetForeignKeys(table string) (fields []ForeignKey, err error) {
	err = p.client.Raw(`select kcu.column_name as "name", rel_kcu.table_name as referenced_table, rel_kcu.column_name as referenced_column from information_schema.table_constraints tco join information_schema.key_column_usage kcu           on tco.constraint_schema = kcu.constraint_schema           and tco.constraint_name = kcu.constraint_name join information_schema.referential_constraints rco           on tco.constraint_schema = rco.constraint_schema           and tco.constraint_name = rco.constraint_name join information_schema.key_column_usage rel_kcu           on rco.unique_constraint_schema = rel_kcu.constraint_schema           and rco.unique_constraint_name = rel_kcu.constraint_name           and kcu.ordinal_position = rel_kcu.ordinal_position where tco.constraint_type = 'FOREIGN KEY' and kcu.table_catalog = ? AND kcu.table_schema = 'public' AND kcu.table_name = ? order by kcu.table_schema,          kcu.table_name,          kcu.ordinal_position;`, p.schema, table).Scan(&fields).Error
	return
}

func (p *Postgres) GetIndices(table string) (fields []Index, err error) {
	err = p.client.Raw(`select DISTINCT kcu.constraint_name as "name", kcu.column_name as "column_name", enforced as "nullable" from information_schema.table_constraints tco join information_schema.key_column_usage kcu       on kcu.constraint_name = tco.constraint_name      and kcu.constraint_schema = tco.constraint_schema      and kcu.constraint_name = tco.constraint_name      WHERE tco.table_catalog = ? AND tco.table_schema = 'public' AND tco.table_name = ?;`, p.schema, table).Scan(&fields).Error
	return
}

// GetTheIndices gets the indices for a table other than the primary key.
// This has only been implemented for postgres.
func (p *Postgres) GetTheIndices(table string) (incides []Indices, err error) {
	err = p.client.Raw(`
SELECT
	i.relname AS name,
	array_agg(a.attname) AS columns,
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
	AND t.relkind = 'r' -- ordinary table
	-- AND ix.indisunique -- is unique
	AND NOT ix.indisprimary -- is not primary
	AND t.relname = ? -- name of table 
GROUP BY
	i.relname,
	ix.indisunique
ORDER BY
	i.relname;`, table).Scan(&incides).Error
	return
}

func (p *Postgres) GetCollection(table string) ([]map[string]any, error) {
	var rows []map[string]any
	if err := p.client.Table(table).Find(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

func (p *Postgres) Exec(sql string, values ...any) error {
	sql = strings.ToLower(sql)
	sql = strings.ReplaceAll(sql, "`", `"`)
	sql = strings.ReplaceAll(sql, `"/"`, `'/'`)
	return p.client.Exec(sql, values...).Error
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
			if err := p.client.Raw(query, param).Find(&rows).Error; err != nil {
				return nil, err
			}
		} else {
			if err := p.client.Raw(query).Find(&rows).Error; err != nil {
				return nil, err
			}
		}
	} else if err := p.client.Raw(query).Find(&rows).Error; err != nil {
		return nil, err
	}

	return rows, nil
}

func (p *Postgres) GetRawPaginatedCollection(query string, paging db.Paging, params ...map[string]any) db.PaginatedResponse {
	var rows []map[string]any
	paging.Raw = true
	return db.PaginateRaw(p.client, query, &rows, paging, params...)
}

func (p *Postgres) GetPaginated(table string, paging db.Paging) db.PaginatedResponse {
	var rows []map[string]any
	return db.Paginate(p.client.Table(table), &rows, paging)
}

func (p *Postgres) GetSingle(table string) (map[string]any, error) {
	var row map[string]any
	if err := p.client.Table(table).Limit(1).Find(&row).Error; err != nil {
		return nil, err
	}
	return row, nil
}

func (p *Postgres) GetType() string {
	return "postgres"
}

func getPostgresFieldAlterDataType(table string, f Field) string {
	dataTypes := postgresDataTypes
	defaultVal := ""
	if f.Default != nil {
		if v, ok := dataTypes[f.DataType]; ok {
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
			if def == "CURRENT_TIMESTAMP" || strings.ToLower(def) == "true" || strings.ToLower(def) == "false" {
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
			f.DataType = "serial"
		}
	}
	fieldName := strings.ToLower(f.Name)
	switch f.DataType {
	case "int", "integer", "smallint", "bigint", "int2", "int4", "int8":
		sql := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s USING %s::%s;", table, fieldName, dataTypes[f.DataType], fieldName, dataTypes[f.DataType])
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
		sql := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s(%d,%d) USING %s::%s;", table, fieldName, dataTypes[f.DataType], f.Length, f.Precision, fieldName, dataTypes[f.DataType])
		if defaultVal != "" {
			sql += fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET %s;", table, fieldName, defaultVal)
		}
		return sql
	case "string", "varchar", "character varying", "char", "character":
		if f.Length == 0 {
			f.Length = 255
		}
		sql := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s(%d) USING %s::%s;", table, fieldName, dataTypes[f.DataType], f.Length, fieldName, dataTypes[f.DataType])
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
		sql := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s USING %s::%s;", table, fieldName, dataTypes[f.DataType], fieldName, dataTypes[f.DataType])
		if defaultVal != "" {
			sql += fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET %s;", table, fieldName, defaultVal)
		}
		return sql
	}
}

func (p *Postgres) alterFieldSQL(table string, f, existingField Field) string {
	newSQL := getPostgresFieldAlterDataType(table, f)
	existingSQL := getPostgresFieldAlterDataType(table, existingField)
	if newSQL != existingSQL {
		return newSQL
	}
	return ""
}

func (p *Postgres) createSQL(table string, newFields []Field, indices ...Indices) (string, error) {
	var sql string
	var query, comments, indexQuery, primaryKeys []string
	for _, field := range newFields {
		fieldName := strings.ToLower(field.Name)
		if strings.ToUpper(field.Key) == "PRI" {
			primaryKeys = append(primaryKeys, fieldName)
		}
		query = append(query, p.FieldAsString(field, "column"))
		if field.Comment != "" {
			comment := "COMMENT ON COLUMN " + table + "." + fieldName + " IS '" + strings.ReplaceAll(field.Comment, "'", `"`) + "';"
			comments = append(comments, comment)
		}
	}
	if len(indices) > 0 {
		for _, index := range indices {
			if index.Name == "" {
				index.Name = "idx_" + table + "_" + strings.Join(index.Columns, "_")
			}
			switch index.Unique {
			case true:
				query := fmt.Sprintf(postgresQueries["create_unique_index"], index.Name, table,
					strings.Join(index.Columns, ", "))
				indexQuery = append(indexQuery, query)
			case false:
				query := fmt.Sprintf(postgresQueries["create_index"], index.Name, table,
					strings.Join(index.Columns, ", "))
				indexQuery = append(indexQuery, query)
			}
		}
	}
	if len(primaryKeys) > 0 {
		query = append(query, " PRIMARY KEY ("+strings.Join(primaryKeys, ", ")+")")
	}
	if len(query) > 0 {
		fieldsToUpdate := strings.Join(query, ", ")
		sql = fmt.Sprintf(postgresQueries["create_table"], table) + " (" + fieldsToUpdate + ");"
		sql += strings.Join(comments, "")
		sql += strings.Join(indexQuery, "")
	}
	return sql, nil
}

func (p *Postgres) alterSQL(table string, newFields []Field, newIndices ...Indices) (string, error) {
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
	for _, newField := range newFields {
		if newField.IsNullable == "" {
			newField.IsNullable = "YES"
		}
		fieldExists := false
		if newField.OldName == "" {
			fieldName := strings.ToLower(newField.Name)
			for _, existingField := range existingFields {
				if strings.ToLower(existingField.Name) == fieldName {
					fieldExists = true
					if postgresDataTypes[existingField.DataType] != postgresDataTypes[newField.DataType] ||
						existingField.Length != newField.Length ||
						existingField.Default != newField.Default {
						qry := p.alterFieldSQL(table, newField, existingField)
						if qry != "" {
							sql = append(sql, qry)
						}
					}
					if existingField.IsNullable != newField.IsNullable {
						if newField.IsNullable == "YES" {
							sql = append(sql, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL;", table, fieldName))
						} else {
							sql = append(sql, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL;", table, fieldName))
						}
					}

					if existingField.Comment != newField.Comment {
						sql = append(sql, "COMMENT ON COLUMN "+table+"."+fieldName+" IS '"+strings.ReplaceAll(newField.Comment, "'", `"`)+"';")
					}
				}
			}
		}
		if !fieldExists {
			qry := alterTable + " " + p.FieldAsString(newField, "add_column") + ";"
			if qry != "" {
				sql = append(sql, qry)
			}
		}
	}
	for _, newField := range newFields {
		fieldName := strings.ToLower(newField.Name)
		if newField.OldName != "" {
			sql = append(sql, alterTable+` RENAME COLUMN "`+newField.OldName+`" TO "`+fieldName+`";`)
		}
	}
	// create a map to keep track of existing indices by name
	existingIndicesMap := make(map[string]Indices)
	for _, existingIndex := range existingIndices {
		existingIndicesMap[existingIndex.Name] = existingIndex
	}
	for _, newIndex := range newIndices {
		// if new index has no name, generate one
		if newIndex.Name == "" {
			newIndex.Name = "idx_" + table + "_" + strings.Join(newIndex.Columns, "_")
		}
		existingIndex, indexExists := existingIndicesMap[newIndex.Name]
		if indexExists {
			// compare the columns
			// if they are different, drop the index and create a new one
			if !reflect.DeepEqual(existingIndex.Columns, newIndex.Columns) {
				sql = append(sql, fmt.Sprintf("DROP INDEX %s;", existingIndex.Name))
				switch newIndex.Unique {
				case true:
					sql = append(sql, fmt.Sprintf(postgresQueries["create_unique_index"], newIndex.Name, table, strings.Join(newIndex.Columns, ", ")))
				case false:
					sql = append(sql, fmt.Sprintf(postgresQueries["create_index"], newIndex.Name, table, strings.Join(newIndex.Columns, ", ")))
				}
			}
			// Remove existing index from map
			delete(existingIndicesMap, newIndex.Name)
		} else {
			// New index with provided name and columns
			switch newIndex.Unique {
			case true:
				sql = append(sql, fmt.Sprintf(postgresQueries["create_unique_index"], newIndex.Name, table, strings.Join(newIndex.Columns, ", ")))
			case false:
				sql = append(sql, fmt.Sprintf(postgresQueries["create_index"], newIndex.Name, table, strings.Join(newIndex.Columns, ", ")))
			}
		}
	}
	// drop any remaining indices in the map
	for _, existingIndex := range existingIndicesMap {
		sql = append(sql, fmt.Sprintf("DROP INDEX %s;", existingIndex.Name))
	}
	if len(sql) > 0 {
		return strings.Join(sql, ""), nil
	}
	return "", nil
}

func (p *Postgres) GenerateSQL(table string, newFields []Field, indices ...Indices) (string, error) {
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

func (p *Postgres) Migrate(table string, dst DataSource) error {
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

func (p *Postgres) Error() error {
	return p.client.Error
}

func (p *Postgres) Close() error {
	clientDB, err := p.client.DB()
	if err != nil {
		return err
	}
	return clientDB.Close()
}

func (p *Postgres) Begin() DataSource {
	tx := p.client.Begin()
	return NewFromClient(tx)
}

func (p *Postgres) Commit() DataSource {
	tx := p.client.Commit()
	return NewFromClient(tx)
}

func (p *Postgres) FieldAsString(f Field, action string) string {
	sqlPattern := postgresQueries
	dataTypes := postgresDataTypes
	nullable := "NULL"
	defaultVal := ""
	comment := ""
	primaryKey := ""
	autoIncrement := ""
	if strings.ToUpper(f.IsNullable) == "NO" {
		nullable = "NOT NULL"
	}
	if f.Default != nil {
		if v, ok := dataTypes[f.DataType]; ok {
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

	if defaultVal == "DEFAULT '0000-00-00 00:00:00'" {
		nullable = "NULL"
		defaultVal = "DEFAULT NULL"
	}
	if f.Key != "" && strings.ToUpper(f.Key) == "PRI" && action != "column" {
		primaryKey = "PRIMARY KEY"
	}
	if f.Extra != "" && strings.ToUpper(f.Extra) == "AUTO_INCREMENT" {
		if strings.ToUpper(f.Extra) == "AUTO_INCREMENT" {
			f.DataType = "serial"
			if action != "column" {
				primaryKey = "PRIMARY KEY"
			}
		}
	}
	fieldName := strings.ToLower(f.Name)
	switch f.DataType {
	case "string", "varchar", "character varying", "char", "character":
		if f.Length == 0 {
			f.Length = 255
		}
		changeColumn := sqlPattern[action] + "(%d) %s %s %s %s %s"
		return strings.TrimSpace(space.ReplaceAllString(fmt.Sprintf(changeColumn, fieldName, dataTypes[f.DataType], f.Length, nullable, primaryKey, autoIncrement, defaultVal, comment), " "))
	case "smallint", "int", "integer", "bigint", "big_integer", "bigInteger", "int2", "int4", "int8":
		changeColumn := sqlPattern[action] + " %s %s %s %s %s"
		return strings.TrimSpace(space.ReplaceAllString(fmt.Sprintf(changeColumn, fieldName, dataTypes[f.DataType], nullable, primaryKey, autoIncrement, defaultVal, comment), " "))
	case "float", "double", "decimal", "numeric":
		if f.Length == 0 {
			f.Length = 11
		}
		if f.Precision == 0 {
			f.Precision = 2
		}
		changeColumn := sqlPattern[action] + "(%d, %d) %s %s %s %s %s"
		return strings.TrimSpace(space.ReplaceAllString(fmt.Sprintf(changeColumn, fieldName, dataTypes[f.DataType], f.Length, f.Precision, nullable, primaryKey, autoIncrement, defaultVal, comment), " "))
	default:
		changeColumn := sqlPattern[action] + " %s %s %s %s %s"
		return strings.TrimSpace(space.ReplaceAllString(fmt.Sprintf(changeColumn, fieldName, dataTypes[f.DataType], nullable, primaryKey, autoIncrement, defaultVal, comment), " "))
	}
}

func NewPostgres(dsn, database string, disableLog bool, pooling ConnectionPooling) *Postgres {
	return &Postgres{
		schema:     database,
		dsn:        dsn,
		client:     nil,
		disableLog: disableLog,
		pooling:    pooling,
	}
}
