package metadata

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/oarkflow/db"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type MySQL struct {
	schema     string
	dsn        string
	client     *gorm.DB
	disableLog bool
}

var mysqlQueries = map[string]string{
	"create_table":  "CREATE TABLE IF NOT EXISTS %s",
	"alter_table":   "ALTER TABLE %s",
	"column":        "%s %s",
	"add_column":    "ADD COLUMN %s %s",    // {{length}} NOT NULL DEFAULT 1
	"change_column": "MODIFY COLUMN %s %s", // {{length}} NOT NULL DEFAULT 1
	"remove_column": "MODIFY COLUMN % %s",  // {{length}} NOT NULL DEFAULT 1
}

var mysqlDataTypes = map[string]string{
	"int":       "INTEGER",
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

		var logLevel logger.LogLevel
		if p.disableLog {
			logLevel = logger.Silent
		} else {
			logLevel = logger.Error
		}
		config := &gorm.Config{
			DisableForeignKeyConstraintWhenMigrating: true,
			Logger:                                   logger.Default.LogMode(logLevel),
		}
		db1, err := gorm.Open(mysql.Open(p.dsn), config)
		if err != nil {
			return nil, err
		}
		p.client = db1
	}
	return p, nil
}

func (p *MySQL) GetSources() (tables []Source, err error) {
	err = p.client.Table("information_schema.tables").Select("table_name as name, table_type").Where("table_schema = ?", p.schema).Find(&tables).Error
	return
}

func (p *MySQL) GetDataTypeMap(dataType string) string {
	if v, ok := mysqlDataTypes[dataType]; ok {
		return v
	}
	return "VARCHAR"
}

func (p *MySQL) GetTables() (tables []Source, err error) {
	err = p.client.Table("information_schema.tables").Select("table_name as name, table_type").Where("table_schema = ? AND table_type='BASE TABLE'", p.schema).Find(&tables).Error
	return
}

func (p *MySQL) GetViews() (tables []Source, err error) {
	err = p.client.Table("information_schema.views").Select("table_name as name, view_definition").Where("table_schema = ?", p.schema).Find(&tables).Error
	return
}

func (p *MySQL) DB() (*sql.DB, error) {
	return p.client.DB()
}

func (p *MySQL) GetDBName() string {
	return p.schema
}

func (p *MySQL) Store(table string, val any) error {
	return p.client.Table(table).Create(val).Error
}

func (p *MySQL) StoreInBatches(table string, val any, size int) error {
	if size <= 0 {
		size = 100
	}
	return p.client.Table(table).CreateInBatches(val, size).Error
}

func (p *MySQL) GetFields(table string) (fields []Field, err error) {
	var fieldMaps []map[string]any
	err = p.client.Raw("SELECT column_name as `name`, column_default as `default`, is_nullable as `is_nullable`, data_type as type, CASE WHEN numeric_precision IS NOT NULL THEN numeric_precision ELSE character_maximum_length END as `length`, numeric_scale as `precision`, column_comment as `comment`, column_key as `key`, extra as extra FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME =  ? AND TABLE_SCHEMA = ?;", table, p.schema).Scan(&fieldMaps).Error
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
	err = p.client.Raw("SELECT distinct cu.column_name as `name`, cu.referenced_table_name as `referenced_table`, cu.referenced_column_name as `referenced_column` FROM information_schema.key_column_usage cu INNER JOIN information_schema.referential_constraints rc ON rc.constraint_schema = cu.table_schema AND rc.table_name = cu.table_name AND rc.constraint_name = cu.constraint_name WHERE cu.table_name=? AND TABLE_SCHEMA=?;", table, p.schema).Scan(&fields).Error
	return
}

func (p *MySQL) GetIndices(table string) (fields []Index, err error) {
	err = p.client.Raw("SELECT DISTINCT s.index_name as name, s.column_name as column_name, s.nullable as `nullable` FROM INFORMATION_SCHEMA.STATISTICS s LEFT OUTER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS t ON t.TABLE_SCHEMA = s.TABLE_SCHEMA AND t.TABLE_NAME = s.TABLE_NAME AND s.INDEX_NAME = t.CONSTRAINT_NAME WHERE s.TABLE_NAME=? AND s.TABLE_SCHEMA = ?;", table, p.schema).Scan(&fields).Error
	return
}

func (p *MySQL) LastInsertedID() (id any, err error) {
	err = p.client.Raw("SELECT LAST_INSERT_ID();").Scan(&id).Error
	return
}

func (p *MySQL) MaxID(table, field string) (id any, err error) {
	err = p.client.Raw(fmt.Sprintf("SELECT MAX(%s) FROM %s;", field, table)).Scan(&id).Error
	return
}

func (p *MySQL) GetCollection(table string) ([]map[string]any, error) {
	var rows []map[string]any
	if err := p.client.Table(table).Find(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

func (p *MySQL) Exec(sql string, values ...any) error {
	sql = strings.ToLower(sql)
	sql = strings.ReplaceAll(sql, `"`, "`")
	return p.client.Exec(sql, values...).Error
}

func (p *MySQL) Begin() *gorm.DB {
	return p.client.Begin()
}

func (p *MySQL) Commit() *gorm.DB {
	return p.client.Commit()
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

func (p *MySQL) GetRawPaginatedCollection(query string, paging db.Paging, params ...map[string]any) db.PaginatedResponse {
	var rows []map[string]any
	paging.Raw = true
	return db.PaginateRaw(p.client, query, &rows, paging, params...)
}

func (p *MySQL) GetPaginated(table string, paging db.Paging) db.PaginatedResponse {
	var rows []map[string]any
	return db.Paginate(p.client.Table(table), &rows, paging)
}

func (p *MySQL) GetSingle(table string) (map[string]any, error) {
	var row map[string]any
	if err := p.client.Table(table).Limit(1).Find(&row).Error; err != nil {
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
	case "float", "double", "decimal", "numeric", "int", "integer":
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

func (p *MySQL) createSQL(table string, newFields []Field) (string, error) {
	var sql string
	var query, primaryKeys []string
	for _, newField := range newFields {
		if strings.ToUpper(newField.Key) == "PRI" {
			primaryKeys = append(primaryKeys, newField.Name)
		}
		query = append(query, p.FieldAsString(newField, "column"))
	}
	if len(primaryKeys) > 0 {
		query = append(query, " PRIMARY KEY ("+strings.Join(primaryKeys, ", ")+")")
	}
	if len(query) > 0 {
		fieldsToUpdate := strings.Join(query, ", ")
		sql = fmt.Sprintf(mysqlQueries["create_table"], table) + " (" + fieldsToUpdate + ")"
	}
	return sql, nil
}

func (p *MySQL) alterSQL(table string, newFields []Field) (string, error) {
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
			for _, existingField := range existingFields {
				if existingField.Name == newField.Name {
					fieldExists = true
					if mysqlDataTypes[existingField.DataType] != mysqlDataTypes[newField.DataType] ||
						existingField.Length != newField.Length ||
						existingField.Default != newField.Default ||
						existingField.Comment != newField.Comment ||
						existingField.IsNullable != newField.IsNullable {
						qry := p.alterFieldSQL(table, newField, existingField)
						if qry != "" {
							sql = append(sql, qry)
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
		return p.createSQL(table, newFields)
	}
	return p.alterSQL(table, newFields)
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
			if def == "CURRENT_TIMESTAMP" || strings.ToLower(def) == "true" || strings.ToLower(def) == "false" {
				defaultVal = fmt.Sprintf("DEFAULT %s", def)
			} else {
				defaultVal = fmt.Sprintf("DEFAULT '%s'", def)
			}
		default:
			defaultVal = "DEFAULT " + fmt.Sprintf("%v", def)
		}
	} else {
		defaultVal = "DEFAULT NULL"
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

func NewMySQL(dsn, database string, disableLog bool) *MySQL {
	return &MySQL{
		schema:     database,
		dsn:        dsn,
		client:     nil,
		disableLog: disableLog,
	}
}
