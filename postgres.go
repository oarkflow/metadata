package metadata

import (
	"fmt"
	"strings"

	json "github.com/bytedance/sonic"
	"github.com/oarkflow/db"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Postgres struct {
	schema string
	dsn    string
	client *gorm.DB
}

var postgresQueries = map[string]string{
	"create_table":  "CREATE TABLE IF NOT EXISTS %s",
	"alter_table":   "ALTER TABLE %s",
	"column":        "%s %s",
	"add_column":    "ADD COLUMN %s %s",        // {{length}} NOT NULL DEFAULT 1
	"change_column": "ALTER COLUMN %s TYPE %s", // {{length}} NOT NULL DEFAULT 1
	"remove_column": "ALTER COLUMN % TYPE %s",  // {{length}} NOT NULL DEFAULT 1
}

var postgresDataTypes = map[string]string{
	"int":                      "NUMERIC",
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
	"text":                     "TEXT",
	"serial":                   "SERIAL",
	"datetime":                 "TIMESTAMPTZ",
	"date":                     "DATE",
	"time":                     "TIME",
	"timestamp":                "TIMESTAMP",
	"timestamp with time zone": "TIMESTAMPTZ",
}

func (p *Postgres) Connect() (DataSource, error) {
	db1, err := gorm.Open(postgres.Open(p.dsn), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
	})
	if err != nil {
		return nil, err
	}
	p.client = db1
	return p, nil
}

func (p *Postgres) GetSources() (tables []Source, err error) {
	err = p.client.Table("information_schema.tables").Select("table_name as name").Where("table_catalog = ? AND table_schema = 'public'", p.schema).Find(&tables).Error
	return
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

func (p *Postgres) GetForeignKeys(table string) (fields []ForeignKey, err error) {
	err = p.client.Raw(`select kcu.column_name as "name", rel_kcu.table_name as referenced_table, rel_kcu.column_name as referenced_column from information_schema.table_constraints tco join information_schema.key_column_usage kcu           on tco.constraint_schema = kcu.constraint_schema           and tco.constraint_name = kcu.constraint_name join information_schema.referential_constraints rco           on tco.constraint_schema = rco.constraint_schema           and tco.constraint_name = rco.constraint_name join information_schema.key_column_usage rel_kcu           on rco.unique_constraint_schema = rel_kcu.constraint_schema           and rco.unique_constraint_name = rel_kcu.constraint_name           and kcu.ordinal_position = rel_kcu.ordinal_position where tco.constraint_type = 'FOREIGN KEY' and kcu.table_catalog = ? AND kcu.table_schema = 'public' AND kcu.table_name = ? order by kcu.table_schema,          kcu.table_name,          kcu.ordinal_position;`, p.schema, table).Scan(&fields).Error
	return
}

func (p *Postgres) GetIndices(table string) (fields []Index, err error) {
	err = p.client.Raw(`select DISTINCT kcu.constraint_name as "name", kcu.column_name as "column_name", enforced as "nullable" from information_schema.table_constraints tco join information_schema.key_column_usage kcu       on kcu.constraint_name = tco.constraint_name      and kcu.constraint_schema = tco.constraint_schema      and kcu.constraint_name = tco.constraint_name      WHERE tco.table_catalog = ? AND tco.table_schema = 'public' AND tco.table_name = ?;`, p.schema, table).Scan(&fields).Error
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
		if err := p.client.Raw(query).Find(&rows, param).Error; err != nil {
			return nil, err
		}
	}

	return rows, nil
}
func (p *Postgres) GetRawPaginatedCollection(query string, params ...map[string]any) db.PaginatedResponse {
	// TODO implement me
	panic("implement me")
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
				if f.Default == "0" {
					f.Default = "FALSE"
				} else if f.Default == "1" {
					f.Default = "TRUE"
				}
			}
		}
		defaultVal = "DEFAULT " + fmt.Sprintf("%v", f.Default)
	}
	if f.Extra != "" && strings.ToUpper(f.Extra) == "AUTO_INCREMENT" {
		if strings.ToUpper(f.Extra) == "AUTO_INCREMENT" {
			f.DataType = "serial"
		}
	}
	switch f.DataType {
	case "float", "double", "decimal", "numeric", "int", "integer":
		if f.Length == 0 {
			f.Length = 11
		}
		if f.Precision == 0 {
			f.Precision = 2
		}
		sql := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s(%d,%d);", table, f.Name, dataTypes[f.DataType], f.Length, f.Precision)
		if defaultVal != "" {
			sql += fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET %s;", table, f.Name, defaultVal)
		}
		return sql
	case "string", "varchar", "text", "character varying":
		if f.Length == 0 {
			f.Length = 255
		}
		sql := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s(%d);", table, f.Name, dataTypes[f.DataType], f.Length)
		if defaultVal != "" {
			sql += fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET %s;", table, f.Name, defaultVal)
		}
		return sql
	default:
		sql := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s;", table, f.Name, dataTypes[f.DataType])
		if defaultVal != "" {
			sql += fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET %s;", table, f.Name, defaultVal)
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

func (p *Postgres) createSQL(table string, newFields []Field) (string, error) {
	var sql string
	var query []string
	var comments []string
	for _, field := range newFields {
		query = append(query, p.FieldAsString(field, "column"))
		if field.Comment != "" {
			comment := "COMMENT ON COLUMN " + table + "." + field.Name + " IS '" + field.Comment + "';"
			comments = append(comments, comment)
		}
	}
	if len(query) > 0 {
		fieldsToUpdate := strings.Join(query, ", ")
		sql = fmt.Sprintf(postgresQueries["create_table"], table) + " (" + fieldsToUpdate + ");"
		sql += strings.Join(comments, "")
	}
	return sql, nil
}

func (p *Postgres) alterSQL(table string, newFields []Field) (string, error) {
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
		if newField.OldName == "" {
			for _, existingField := range existingFields {
				if existingField.Name == newField.Name {
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
							sql = append(sql, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL;", table, newField.Name))
						} else {
							sql = append(sql, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL;", table, newField.Name))
						}
					}

					if existingField.Comment != newField.Comment {
						sql = append(sql, "COMMENT ON COLUMN "+table+"."+newField.Name+" IS '"+newField.Comment+"';")
					}
				}
			}
		}
	}
	for _, newField := range newFields {
		if newField.OldName != "" {
			sql = append(sql, alterTable+` RENAME COLUMN "`+newField.OldName+`" TO "`+newField.Name+`";`)
		}
	}

	if len(sql) > 0 {
		return strings.Join(sql, ""), nil
	}
	return "", nil
}

func (p *Postgres) GenerateSQL(table string, newFields []Field) (string, error) {
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
				if f.Default == "0" {
					f.Default = "FALSE"
				} else if f.Default == "1" {
					f.Default = "TRUE"
				}
			}
		}
		defaultVal = "DEFAULT " + fmt.Sprintf("%v", f.Default)
	}
	if f.Key != "" && strings.ToUpper(f.Key) == "PRI" {
		primaryKey = "PRIMARY KEY"
	}
	if f.Extra != "" && strings.ToUpper(f.Extra) == "AUTO_INCREMENT" {
		if strings.ToUpper(f.Extra) == "AUTO_INCREMENT" {
			f.DataType = "serial"
			primaryKey = "PRIMARY KEY"
		}
	}
	switch f.DataType {
	case "string", "varchar", "text", "character varying":
		if f.Length == 0 {
			f.Length = 255
		}
		changeColumn := sqlPattern[action] + "(%d) %s %s %s %s %s"
		return strings.TrimSpace(space.ReplaceAllString(fmt.Sprintf(changeColumn, f.Name, dataTypes[f.DataType], f.Length, nullable, primaryKey, autoIncrement, defaultVal, comment), " "))
	case "int", "integer", "big_integer", "bigInteger":
		changeColumn := sqlPattern[action] + " %s %s %s %s %s"
		return strings.TrimSpace(space.ReplaceAllString(fmt.Sprintf(changeColumn, f.Name, dataTypes[f.DataType], nullable, primaryKey, autoIncrement, defaultVal, comment), " "))
	case "float", "double", "decimal", "numeric":
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

func NewPostgres(dsn, database string) *Postgres {
	return &Postgres{
		schema: database,
		dsn:    dsn,
		client: nil,
	}
}
