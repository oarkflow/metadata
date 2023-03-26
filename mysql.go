package metadata

import (
	"fmt"
	"strings"

	"github.com/oarkflow/db"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type MySQL struct {
	schema string
	dsn    string
	client *gorm.DB
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
	db1, err := gorm.Open(mysql.Open(p.dsn), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
	})
	if err != nil {
		return nil, err
	}
	p.client = db1
	return p, nil
}

func (p *MySQL) GetSources() (tables []Source, err error) {
	err = p.client.Table("information_schema.tables").Select("table_name as name").Where("table_schema = ?", p.schema).Find(&tables).Error
	return
}

func (p *MySQL) GetFields(table string) (fields []Field, err error) {
	err = p.client.Raw("SELECT column_name as `name`, column_default as `default`, is_nullable as `is_nullable`, data_type as data_type, CASE WHEN numeric_precision IS NOT NULL THEN numeric_precision ELSE character_maximum_length END as `length`, numeric_scale as `precision`, column_comment as `comment`, column_key as `key`, extra as extra FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME =  ? AND TABLE_SCHEMA = ?;", table, p.schema).Scan(&fields).Error
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

func (p *MySQL) GetCollection(table string) ([]map[string]any, error) {
	var rows []map[string]any
	if err := p.client.Table(table).Find(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
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
		if err := p.client.Raw(query).Find(&rows, param).Error; err != nil {
			return nil, err
		}
	}

	return rows, nil
}
func (p *MySQL) GetRawPaginatedCollection(query string, params ...map[string]any) db.PaginatedResponse {
	return db.PaginatedResponse{}
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

func (p *MySQL) GenerateSQL(table string, existingFields, newFields []Field) (string, error) {
	var sql string
	var query []string
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
	for _, newField := range newFields {
		var fieldExists bool
		for _, existingField := range existingFields {
			if newField.Name == existingField.Name {
				existingSql := p.FieldAsString(existingField, "mysql", "change_column")
				newSql := p.FieldAsString(newField, "mysql", "change_column")
				if newSql != existingSql {
					query = append(query, newSql)
				}
				fieldExists = true
				break
			}
		}
		if !fieldExists {
			if sourceExists {
				query = append(query, p.FieldAsString(newField, "mysql", "add_column"))
			} else {
				query = append(query, p.FieldAsString(newField, "mysql", "column"))
			}
		}
	}
	if len(query) > 0 {
		fieldsToUpdate := strings.Join(query, ", ")
		if sourceExists {
			sql = fmt.Sprintf(mysqlQueries["alter_table"], table) + " " + fieldsToUpdate
		} else {
			sql = fmt.Sprintf(mysqlQueries["create_table"], table) + " (" + fieldsToUpdate + ")"
		}
	}
	return sql, nil
}

func (p *MySQL) Migrate(table string, dst DataSource) error {
	fields, err := p.GetFields(table)
	if err != nil {
		return err
	}
	sql, err := dst.GenerateSQL(table, nil, fields)
	if err != nil {
		return err
	}
	fmt.Println(sql)
	return nil
}

func (p *MySQL) FieldAsString(f Field, driver, action string) string {

	var dataTypes map[string]string
	var sqlPattern map[string]string
	switch driver {
	case "mysql":
		sqlPattern = mysqlQueries
		dataTypes = mysqlDataTypes
	case "postgres":
		sqlPattern = postgresQueries
		dataTypes = postgresDataTypes
	}
	switch f.DataType {
	case "string", "varchar", "text":
		if f.Length == 0 {
			f.Length = 255
		}
		nullable := "NULL"
		defaultVal := ""
		comment := ""
		primaryKey := ""
		autoIncrement := ""
		changeColumn := sqlPattern[action] + "(%d) %s %s %s %s %s"
		if strings.ToUpper(f.IsNullable) == "NO" {
			nullable = "NOT NULL"
		}
		if f.Default != "" {
			defaultVal = "DEFAULT " + f.Default
		}
		if f.Comment != "" {
			comment = "COMMENT " + f.Comment
		}
		if f.Key != "" && strings.ToUpper(f.Key) == "PRI" {
			primaryKey = "PRIMARY KEY"
		}
		if f.Extra != "" && strings.ToUpper(f.Extra) == "AUTO_INCREMENT" {
			if strings.ToUpper(f.Extra) == "AUTO_INCREMENT" {
				autoIncrement = "AUTO_INCREMENT"
			}
		}
		return strings.TrimSpace(space.ReplaceAllString(fmt.Sprintf(changeColumn, f.Name, dataTypes[f.DataType], f.Length, nullable, primaryKey, autoIncrement, defaultVal, comment), " "))
	case "int", "integer", "big_integer", "bigInteger", "tinyint":
		if f.Length == 0 {
			f.Length = 11
		}
		if f.DataType == "tinyint" {
			f.Length = 1
		}
		nullable := "NULL"
		defaultVal := ""
		comment := ""
		primaryKey := ""
		autoIncrement := ""
		changeColumn := sqlPattern[action] + "(%d) %s %s %s %s %s"
		if strings.ToUpper(f.IsNullable) == "NO" {
			nullable = "NOT NULL"
		}
		if f.Default != "" {
			defaultVal = "DEFAULT " + f.Default
		}
		if f.Comment != "" {
			comment = "COMMENT " + f.Comment
		}

		if f.Key != "" && strings.ToUpper(f.Key) == "PRI" {
			primaryKey = "PRIMARY KEY"
		}
		if f.Extra != "" && strings.ToUpper(f.Extra) == "AUTO_INCREMENT" {
			if strings.ToUpper(f.Extra) == "AUTO_INCREMENT" {
				autoIncrement = "AUTO_INCREMENT"
			}
		}
		return strings.TrimSpace(space.ReplaceAllString(fmt.Sprintf(changeColumn, f.Name, dataTypes[f.DataType], f.Length, nullable, primaryKey, autoIncrement, defaultVal, comment), " "))
	case "float", "double", "decimal":
		if f.Length == 0 {
			f.Length = 11
		}
		if f.Precision == 0 {
			f.Precision = 2
		}
		nullable := "NULL"
		defaultVal := ""
		comment := ""
		primaryKey := ""
		autoIncrement := ""
		changeColumn := sqlPattern[action] + "(%d, %d) %s %s %s"
		if strings.ToUpper(f.IsNullable) == "NO" {
			nullable = "NOT NULL"
		}
		if f.Default != "" {
			defaultVal = "DEFAULT " + f.Default
		}
		if f.Comment != "" {
			comment = "COMMENT " + f.Comment
		}

		if f.Key != "" && strings.ToUpper(f.Key) == "PRI" {
			primaryKey = "PRIMARY KEY"
		}
		if f.Extra != "" && strings.ToUpper(f.Extra) == "AUTO_INCREMENT" {
			if strings.ToUpper(f.Extra) == "AUTO_INCREMENT" {
				autoIncrement = "AUTO_INCREMENT"
			}
		}
		return strings.TrimSpace(space.ReplaceAllString(fmt.Sprintf(changeColumn, f.Name, dataTypes[f.DataType], f.Length, f.Precision, nullable, primaryKey, autoIncrement, defaultVal, comment), " "))
	default:
		nullable := "NULL"
		defaultVal := ""
		comment := ""
		primaryKey := ""
		autoIncrement := ""
		changeColumn := sqlPattern[action] + " %s %s %s %s %s"
		if strings.ToUpper(f.IsNullable) == "NO" {
			nullable = "NOT NULL"
		}
		if f.Default != "" {
			defaultVal = "DEFAULT " + f.Default
		}
		if f.Comment != "" {
			comment = "COMMENT " + f.Comment
		}

		if f.Key != "" && strings.ToUpper(f.Key) == "PRI" {
			primaryKey = "PRIMARY KEY"
		}
		if f.Extra != "" && strings.ToUpper(f.Extra) == "AUTO_INCREMENT" {
			if strings.ToUpper(f.Extra) == "AUTO_INCREMENT" {
				autoIncrement = "AUTO_INCREMENT"
			}
		}
		return strings.TrimSpace(space.ReplaceAllString(fmt.Sprintf(changeColumn, f.Name, dataTypes[f.DataType], nullable, primaryKey, autoIncrement, defaultVal, comment), " "))
	}
}

func NewMySQL(dsn, database string) *MySQL {
	return &MySQL{
		schema: database,
		dsn:    dsn,
		client: nil,
	}
}
