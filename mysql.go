package metadata

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/dbresolver"
	"github.com/oarkflow/squealx/drivers/mysql"
	"github.com/oarkflow/squealx/orm"
)

type MySQL struct {
	schema     string
	dsn        string
	id         string
	client     dbresolver.DBResolver
	disableLog bool
	pooling    ConnectionPooling
	config     Config
}

var mysqlQueries = map[string]string{
	"create_table":        "CREATE TABLE IF NOT EXISTS %s",
	"alter_table":         "ALTER TABLE %s",
	"column":              "%s %s",
	"add_column":          "ADD COLUMN %s %s",    // {{length}} NOT NULL DEFAULT 1
	"change_column":       "MODIFY COLUMN %s %s", // {{length}} NOT NULL DEFAULT 1
	"remove_column":       "MODIFY COLUMN % %s",  // {{length}} NOT NULL DEFAULT 1
	"create_unique_index": "CREATE UNIQUE INDEX %s ON %s (%s);",
	"create_index":        "CREATE INDEX %s ON %s (%s);",
}

var mysqlDataTypes = map[string]string{
	"int":       "INTEGER",
	"integer":   "INTEGER",
	"bigint":    "BIGINT",
	"float":     "FLOAT",
	"double":    "DOUBLE",
	"decimal":   "DECIMAL",
	"char":      "CHAR",
	"tinyint":   "TINYINT",
	"string":    "VARCHAR",
	"varchar":   "VARCHAR",
	"text":      "TEXT",
	"datetime":  "DATETIME",
	"date":      "DATE",
	"time":      "TIME",
	"timestamp": "TIMESTAMP",
	"bool":      "TINYINT",
	"boolean":   "TINYINT",
}

func (p *MySQL) Connect() (DataSource, error) {
	if p.client == nil {
		db1, err := mysql.Open(p.dsn, p.id)
		if err != nil {
			return nil, err
		}
		db1.SetConnMaxLifetime(time.Duration(p.pooling.MaxLifetime) * time.Second)
		db1.SetConnMaxIdleTime(time.Duration(p.pooling.MaxIdleTime) * time.Second)
		db1.SetMaxOpenConns(p.pooling.MaxOpenCons)
		db1.SetMaxIdleConns(p.pooling.MaxIdleCons)
		primaryDBsCfg := &dbresolver.MasterConfig{
			DBs:             []*squealx.DB{db1},
			ReadWritePolicy: dbresolver.ReadWrite,
		}
		p.client = dbresolver.MustNewDBResolver(primaryDBsCfg)
	}
	return p, nil
}

func (p *MySQL) GetSources() (tables []Source, err error) {
	err = p.client.Select(&tables, "SELECT table_name as name, table_type FROM information_schema.tables WHERE table_schema = :schema", map[string]any{
		"schema": p.schema,
	})
	return
}

func (p *MySQL) Config() Config {
	return p.config
}

func (p *MySQL) GetDataTypeMap(dataType string) string {
	if v, ok := mysqlDataTypes[dataType]; ok {
		return v
	}
	return "VARCHAR"
}

func (p *MySQL) GetTables() (tables []Source, err error) {
	err = p.client.Select(&tables, "SELECT table_name as name, table_type FROM information_schema.tables WHERE table_schema = :schema AND table_type='BASE TABLE'", map[string]any{
		"schema": p.schema,
	})
	return
}

func (p *MySQL) GetViews() (tables []Source, err error) {
	err = p.client.Select(&tables, "SELECT table_name as name, view_definition FROM information_schema.views WHERE table_schema = :schema", map[string]any{
		"schema": p.schema,
	})
	return
}

func (p *MySQL) Client() any {
	return p.client
}

func (p *MySQL) GetDBName() string {
	return p.schema
}

func (p *MySQL) Store(table string, val any) error {
	_, err := p.client.Exec(orm.InsertQuery(table, val), val)
	return err
}

func (p *MySQL) StoreInBatches(table string, val any, size int) error {
	return processBatchInsert(p.client, table, val, size)
}

func (p *MySQL) GetFields(table string) (fields []Field, err error) {
	var fieldMaps []map[string]any
	err = p.client.Select(&fieldMaps, "SELECT column_name as `name`, column_default as `default`, is_nullable as `is_nullable`, data_type as type, CASE WHEN numeric_precision IS NOT NULL THEN numeric_precision ELSE character_maximum_length END as `length`, numeric_scale as `precision`, column_comment as `comment`, column_key as `key`, extra as extra FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME =  :table_name AND TABLE_SCHEMA = :schema;", map[string]any{
		"schema":     p.schema,
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

func (p *MySQL) GetForeignKeys(table string) (fields []ForeignKey, err error) {
	err = p.client.Select(&fields, "SELECT distinct cu.column_name as `name`, cu.referenced_table_name as `referenced_table`, cu.referenced_column_name as `referenced_column` FROM information_schema.key_column_usage cu INNER JOIN information_schema.referential_constraints rc ON rc.constraint_schema = cu.table_schema AND rc.table_name = cu.table_name AND rc.constraint_name = cu.constraint_name WHERE cu.table_name=:table_name AND TABLE_SCHEMA=:schema;", map[string]any{
		"schema":     p.schema,
		"table_name": table,
	})
	return
}

func (p *MySQL) GetIndices(table string) (fields []Index, err error) {
	err = p.client.Select(&fields, "SELECT DISTINCT s.index_name as name, s.column_name as column_name, s.nullable as `nullable` FROM INFORMATION_SCHEMA.STATISTICS s LEFT OUTER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS t ON t.TABLE_SCHEMA = s.TABLE_SCHEMA AND t.TABLE_NAME = s.TABLE_NAME AND s.INDEX_NAME = t.CONSTRAINT_NAME WHERE s.TABLE_NAME=:table_name AND s.TABLE_SCHEMA = :schema;", map[string]any{
		"schema":     p.schema,
		"table_name": table,
	})
	return
}

func (p *MySQL) GetTheIndices(table string) (fields []Indices, err error) {
	err = p.client.Select(&fields, `SELECT INDEX_NAME AS name, NON_UNIQUE as uniq, CONCAT('[', GROUP_CONCAT(CONCAT('"',COLUMN_NAME,'"') ORDER BY SEQ_IN_INDEX) ,']') AS columns FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table_name GROUP BY INDEX_NAME, NON_UNIQUE;`, map[string]any{
		"schema":     p.schema,
		"table_name": table,
	})
	return
}

func (p *MySQL) LastInsertedID() (id any, err error) {
	err = p.client.Select(&id, "SELECT LAST_INSERT_ID();")
	return
}

func (p *MySQL) MaxID(table, field string) (id any, err error) {
	err = p.client.Select(&id, fmt.Sprintf("SELECT MAX(%s) FROM %s;", field, table))
	return
}

func (p *MySQL) GetCollection(table string) ([]map[string]any, error) {
	var rows []map[string]any
	err := p.client.Select(&rows, "SELECT * FROM "+table)
	return rows, err
}

func (p *MySQL) Close() error {
	return p.client.Close()
}

func (p *MySQL) Exec(sql string, values ...any) error {
	sql = strings.ReplaceAll(sql, `"`, "`")
	_, err := p.client.Exec(sql, values...)
	return err
}

func (p *MySQL) Begin() (squealx.SQLTx, error) {
	return p.client.Begin()
}

func (p *MySQL) GetRawCollection(query string, params ...map[string]any) ([]map[string]any, error) {
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

func (p *MySQL) GetRawPaginatedCollection(query string, paging squealx.Paging, params ...map[string]any) squealx.PaginatedResponse {
	var rows []map[string]any
	return p.client.Paginate(query, &rows, paging, params...)
}

func (p *MySQL) GetPaginated(table string, paging squealx.Paging) squealx.PaginatedResponse {
	var rows []map[string]any
	return p.client.Paginate("SELECT * FROM "+table, &rows, paging)
}

func (p *MySQL) GetSingle(table string) (map[string]any, error) {
	var row map[string]any
	if err := p.client.Select(&row, fmt.Sprintf("SELECT * FROM %s LIMIT 1", table)); err != nil {
		return nil, err
	}
	return row, nil
}

func (p *MySQL) GetType() string {
	return "mysql"
}

func getMySQLFieldAlterDataType(table string, f Field) string {
	dataTypes := mysqlDataTypes
	defaultVal := ""
	if f.Default != nil {
		if v, ok := dataTypes[f.DataType]; ok {
			switch v1 := f.Default.(type) {
			case bool:
				if v1 {
					f.Default = 1
				} else {
					f.Default = 0
				}
			}
			if v == "BOOLEAN" {
				def := fmt.Sprintf("%v", f.Default)
				if strings.ToUpper(def) == "FALSE" || strings.ToUpper(def) == "NO" {
					f.Default = "0"
				} else if strings.ToUpper(def) == "TRUE" || strings.ToUpper(def) == "YES" {
					f.Default = "1"
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
	f.Comment = "COMMENT '" + f.Comment + "'"
	nullable := "NULL"
	if strings.ToUpper(f.IsNullable) == "NO" {
		nullable = "NOT NULL"
	}
	if defaultVal == "DEFAULT '0000-00-00 00:00:00'" {
		nullable = "NULL"
		defaultVal = "DEFAULT NULL"
	}
	switch f.DataType {
	case "float", "double", "decimal", "numeric":
		if f.Length == 0 {
			f.Length = 11
		}
		if f.Precision == 0 {
			f.Precision = 2
		}
		if f.OldName != "" {
			return fmt.Sprintf("ALTER TABLE %s CHANGE %s %s %s(%d,%d) %s %s %s;", table, f.OldName, f.Name, dataTypes[f.DataType], f.Length, f.Precision, nullable, defaultVal, f.Comment)
		}
		return fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s(%d,%d) %s %s %s;", table, f.Name, dataTypes[f.DataType], f.Length, f.Precision, nullable, defaultVal, f.Comment)
	case "int", "integer":
		if f.Length == 0 {
			f.Length = 11
		}
		if f.OldName != "" {
			return fmt.Sprintf("ALTER TABLE %s CHANGE %s %s %s(%d) %s %s %s;", table, f.OldName, f.Name, dataTypes[f.DataType], f.Length, nullable, defaultVal, f.Comment)
		}
		return fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s(%d) %s %s %s;", table, f.Name, dataTypes[f.DataType], f.Length, nullable, defaultVal, f.Comment)
	case "string", "varchar", "text", "character varying", "char":
		if f.Length == 0 {
			f.Length = 255
		}
		if f.OldName != "" {
			return fmt.Sprintf("ALTER TABLE %s CHANGE %s %s %s(%d) %s %s %s;", table, f.OldName, f.Name, dataTypes[f.DataType], f.Length, nullable, defaultVal, f.Comment)
		}
		return fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s(%d) %s %s %s;", table, f.Name, dataTypes[f.DataType], f.Length, nullable, defaultVal, f.Comment)
	default:
		if f.OldName != "" {
			return fmt.Sprintf("ALTER TABLE %s CHANGE %s %s %s %s %s %s;", table, f.OldName, f.Name, dataTypes[f.DataType], nullable, defaultVal, f.Comment)
		}
		return fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s %s %s %s;", table, f.Name, dataTypes[f.DataType], nullable, defaultVal, f.Comment)
	}
}

func (p *MySQL) alterFieldSQL(table string, f, existingField Field) string {
	newSQL := getMySQLFieldAlterDataType(table, f)
	existingSQL := getMySQLFieldAlterDataType(table, existingField)
	if newSQL != existingSQL {
		return newSQL
	}
	return ""
}

func (p *MySQL) createSQL(table string, newFields []Field, indices ...Indices) (string, error) {
	var sql string
	var query, indexQuery, primaryKeys []string
	for _, newField := range newFields {
		if strings.ToUpper(newField.Key) == "PRI" {
			primaryKeys = append(primaryKeys, newField.Name)
		}
		query = append(query, p.FieldAsString(newField, "column"))
	}
	if len(indices) > 0 {
		existingIndices, err := p.GetTheIndices(table)
		if err != nil {
			panic(err)
			return "", err
		}
		fmt.Println(existingIndices)
		for _, index := range indices {
			if index.Name == "" {
				index.Name = "idx_" + table + "_" + strings.Join(index.Columns, "_")
			}
			switch index.Unique {
			case true:
				query := fmt.Sprintf(mysqlQueries["create_unique_index"], index.Name, table,
					strings.Join(index.Columns, ", "))
				indexQuery = append(indexQuery, query)
			case false:
				query := fmt.Sprintf(mysqlQueries["create_index"], index.Name, table,
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
		sql = fmt.Sprintf(mysqlQueries["create_table"], table) + " (" + fieldsToUpdate + ");"
	}
	if len(indexQuery) > 0 {
		sql += strings.Join(indexQuery, "")
	}
	return sql, nil
}

func (p *MySQL) alterSQL(table string, newFields []Field, indices ...Indices) (string, error) {
	var sql []string
	alterTable := "ALTER TABLE " + table
	existingFields, err := p.GetFields(table)
	if err != nil {
		return "", err
	}
	for _, newField := range newFields {
		if newField.IsNullable == "" {
			newField.IsNullable = "YES"
		}
		fieldExists := false
		if newField.OldName == "" {
			fieldName := newField.Name
			for _, existingField := range existingFields {
				if existingField.Name == newField.Name {
					fieldExists = true
					if mysqlDataTypes[existingField.DataType] != mysqlDataTypes[newField.DataType] ||
						existingField.Length != newField.Length ||
						existingField.Default != newField.Default ||
						existingField.Comment != newField.Comment {
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
		if newField.IsNullable == "" {
			newField.IsNullable = "YES"
		}
		if newField.OldName != "" {
			for _, existingField := range existingFields {
				if existingField.Name == newField.Name {
					qry := p.alterFieldSQL(table, newField, existingField)
					if qry != "" {
						sql = append(sql, qry)
					}
				}
			}
		}
	}

	if len(sql) > 0 {
		return strings.Join(sql, ""), nil
	}
	return "", nil
}

func (p *MySQL) GenerateSQL(table string, newFields []Field, indices ...Indices) (string, error) {
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

func (p *MySQL) Migrate(table string, dst DataSource) error {
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

func (p *MySQL) FieldAsString(f Field, action string) string {
	sqlPattern := mysqlQueries
	dataTypes := mysqlDataTypes
	nullable := "NULL"
	defaultVal := ""
	comment := ""
	primaryKey := ""
	autoIncrement := ""
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

	if defaultVal == "DEFAULT '0000-00-00 00:00:00'" {
		nullable = "NULL"
		defaultVal = "DEFAULT NULL"
	}
	if f.Comment != "" {
		comment = "COMMENT '" + f.Comment + "'"
	}
	if f.Key != "" && strings.ToUpper(f.Key) == "PRI" && action != "column" {
		primaryKey = "PRIMARY KEY"
	}
	if f.Extra != "" && strings.ToUpper(f.Extra) == "AUTO_INCREMENT" {
		if strings.ToUpper(f.Extra) == "AUTO_INCREMENT" {
			autoIncrement = "AUTO_INCREMENT"
		}
	}
	switch f.DataType {
	case "string", "varchar", "text", "char":
		if f.Length == 0 {
			f.Length = 255
		}
		changeColumn := sqlPattern[action] + "(%d) %s %s %s %s %s"
		return strings.TrimSpace(space.ReplaceAllString(fmt.Sprintf(changeColumn, f.Name, dataTypes[f.DataType], f.Length, nullable, primaryKey, autoIncrement, defaultVal, comment), " "))
	case "int", "integer", "big_integer", "bigInteger", "tinyint":
		if f.Length == 0 {
			f.Length = 11
		}
		if f.DataType == "tinyint" {
			f.Length = 1
		}
		changeColumn := sqlPattern[action] + "(%d) %s %s %s %s %s"
		return strings.TrimSpace(space.ReplaceAllString(fmt.Sprintf(changeColumn, f.Name, dataTypes[f.DataType], f.Length, nullable, primaryKey, autoIncrement, defaultVal, comment), " "))
	case "float", "double", "decimal":
		if f.Length == 0 {
			f.Length = 11
		}
		if f.Precision == 0 {
			f.Precision = 2
		}
		changeColumn := sqlPattern[action] + "(%d, %d) %s %s %s %s %s"
		return strings.TrimSpace(space.ReplaceAllString(fmt.Sprintf(changeColumn, f.Name, dataTypes[f.DataType], f.Length, f.Precision, nullable, primaryKey, autoIncrement, defaultVal, comment), " "))
	default:
		changeColumn := sqlPattern[action] + " %s %s %s %s %s"
		return strings.TrimSpace(space.ReplaceAllString(fmt.Sprintf(changeColumn, f.Name, dataTypes[f.DataType], nullable, primaryKey, autoIncrement, defaultVal, comment), " "))
	}
}

func NewMySQL(id, dsn, database string, disableLog bool, pooling ConnectionPooling) *MySQL {
	return &MySQL{
		schema:     database,
		dsn:        dsn,
		id:         id,
		client:     nil,
		disableLog: disableLog,
		pooling:    pooling,
	}
}
