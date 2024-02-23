package metadata

import (
	"database/sql"
	"database/sql/driver"
	stdJson "encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/lib/pq"
	"github.com/oarkflow/db"
	"github.com/oarkflow/errors"
	"github.com/oarkflow/json"
	"github.com/oarkflow/pkg/str"
	"gorm.io/gorm"
)

var builtInFunctions = []string{
	"current_timestamp",
	"now()",
	"true",
	"false",
}

type Any stdJson.RawMessage

// Scan scan value into Jsonb, implements sql.Scanner interface
func (j *Any) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("Failed to unmarshal JSONB value:", value))
	}

	result := stdJson.RawMessage{}
	err := stdJson.Unmarshal(bytes, &result)
	*j = Any(result)
	return err
}

// Value return json value, implement driver.Valuer interface
func (j Any) Value() (driver.Value, error) {
	if len(j) == 0 {
		return nil, nil
	}
	return stdJson.RawMessage(j).MarshalJSON()
}

type Config struct {
	Name          string `json:"name"`
	Key           string `json:"key"`
	Host          string `json:"host"`
	Port          int    `json:"port"`
	Driver        string `json:"driver"`
	Username      string `json:"username"`
	Password      string `json:"password"`
	Database      string `json:"database"`
	SslMode       string `json:"ssl_mode"`
	Timezone      string `json:"timezone"`
	Charset       string `json:"charset"`
	Location      string `json:"location"`
	DisableLogger bool   `json:"disable_logger"`
}

type Source struct {
	Name       string `json:"name" gorm:"column:name"`
	Type       string `json:"type" gorm:"column:table_type"`
	Definition string `json:"definition" gorm:"column:view_definition"`
	Title      string `json:"title" gorm:"-"`
}

type Field struct {
	Name       string `json:"name" gorm:"column:name"`
	OldName    string `json:"old_name" gorm:"column:old_name"`
	Key        string `json:"key" gorm:"column:key"`
	IsNullable string `json:"is_nullable" gorm:"column:is_nullable"`
	DataType   string `json:"type" gorm:"column:type"`
	Precision  int    `json:"precision" gorm:"column:precision"`
	Comment    string `json:"comment" gorm:"column:comment"`
	Default    any    `json:"default" gorm:"column:default"`
	Length     int    `json:"length" gorm:"column:length"`
	Extra      string `json:"extra" gorm:"column:extra"`
}

var space = regexp.MustCompile(`\s+`)

type ForeignKey struct {
	Name             string `json:"name" gorm:"column:name"`
	ReferencedTable  string `json:"referenced_table" gorm:"column:referenced_table"`
	ReferencedColumn string `json:"referenced_column" gorm:"column:referenced_column"`
}

type Index struct {
	Name       string `json:"name" gorm:"column:name"`
	ColumnName string `json:"column_name" gorm:"column:column_name"`
	Nullable   bool   `json:"nullable" gorm:"column:nullable"`
}

type Indices struct {
	Name    string         `json:"name" gorm:"column:name"`
	Unique  bool           `json:"unique" gorm:"column:unique"`
	Columns pq.StringArray `json:"columns" gorm:"type:text[] column:columns"`
}

type SourceFields struct {
	Name   string  `json:"name" gorm:"column:table_name"`
	Title  string  `json:"title" gorm:"-"`
	Fields []Field `json:"fields"`
}

type Schema struct {
	Type                 string             `json:"type"`
	Description          string             `json:"description,omitempty"`
	Default              string             `json:"default,omitempty"`
	Pattern              string             `json:"pattern,omitempty"`
	Format               string             `json:"format,omitempty"`
	Properties           map[string]*Schema `json:"properties,omitempty"`
	Required             []string           `json:"required,omitempty"`
	AdditionalProperties bool               `json:"additionalProperties,omitempty"`
	PrimaryKeys          []string           `json:"primaryKeys,omitempty"`
	MaxLength            int                `json:"maxLength,omitempty"`
}

func (s *Schema) Bytes() []byte {
	bt, _ := json.Marshal(s)
	return bt
}

func (s *Schema) String() string {
	bt, _ := json.Marshal(s)
	return str.FromByte(bt)
}

func AsJsonSchema(fields []Field, additionalProperties bool, source ...string) *Schema {
	schema := &Schema{
		Type:                 "object",
		Properties:           make(map[string]*Schema),
		AdditionalProperties: additionalProperties,
	}
	if len(source) > 0 {
		schema.Description = source[0]
	}
	for _, field := range fields {
		prop := &Schema{
			Type: "string",
		}
		if field.Default != nil && !strings.Contains(fmt.Sprintf("%v", field.Default), "nextval") {
			prop.Default = fmt.Sprintf("%v", field.Default)
		}

		if field.Length != 0 {
			prop.MaxLength = field.Length
		}
		if field.Key == "PRI" {
			schema.PrimaryKeys = append(schema.PrimaryKeys, field.Name)
		}
		if field.IsNullable == "NO" {
			def := fmt.Sprintf("%v", field.Default)
			if !(def == "now()" || strings.ToUpper(field.DataType) == "TIMESTAMP" || def == "CURRENT_TIMESTAMP" || field.Key == "PRI") {
				schema.Required = append(schema.Required, field.Name)
			}

		}
		switch strings.ToUpper(field.DataType) {
		case "BOOL", "BOOLEAN":
			prop.Type = "boolean"
		case "FLOAT", "FLOAT32", "DECIMAL", "DOUBLE":
			prop.Type = "number"
		case "DATETIME", "TIMESTAMP":
			prop.Format = "date-time"
		case "DATE":
			prop.Format = "date"
		case "NUMERIC":
			if field.Precision == 0 {
				prop.Type = "integer"
			} else {
				prop.Type = "number"
			}
		case "INT", "INT2", "INT4", "INTEGER", "BIGINT", "INT8", "SERIAL", "BIGSERIAL":
			prop.Type = "integer"
		}
		schema.Properties[field.Name] = prop
	}
	return schema
}

func (s *SourceFields) AsJsonSchema(additionalProperties bool) *Schema {
	return AsJsonSchema(s.Fields, additionalProperties, s.Title)
}

type DB interface{}

type DataSource interface {
	DB() (*sql.DB, error)
	GetDBName() string
	GetSources() (tables []Source, err error)
	GetDataTypeMap(dataType string) string
	GetTables() ([]Source, error)
	GetViews() ([]Source, error)
	GetForeignKeys(table string) (fields []ForeignKey, err error)
	GetIndices(table string) (fields []Index, err error)
	Begin() DataSource
	Commit() DataSource
	Error() error
	Exec(sql string, values ...any) error
	GenerateSQL(table string, newFields []Field, indices ...Indices) (string, error)
	LastInsertedID() (id any, err error)
	MaxID(table, field string) (id any, err error)
	Connect() (DataSource, error)
	GetFields(table string) (fields []Field, err error)
	GetCollection(table string) ([]map[string]any, error)
	GetRawCollection(query string, params ...map[string]any) ([]map[string]any, error)
	GetRawPaginatedCollection(query string, paging db.Paging, params ...map[string]any) db.PaginatedResponse
	GetPaginated(table string, paging db.Paging) db.PaginatedResponse
	GetSingle(table string) (map[string]any, error)
	Migrate(table string, dst DataSource) error
	GetType() string
	Store(table string, val any) error
	StoreInBatches(table string, val any, size int) error
}

func NewFromClient(client *gorm.DB) DataSource {
	switch client.Dialector.Name() {
	case "mysql", "mariadb":
		return &MySQL{client: client}
	case "postgres", "psql", "postgresql":
		return &Postgres{client: client}
	case "sql-server", "sqlserver", "mssql", "ms-sql":
		return &MsSQL{client: client}
	}
	return nil
}

func New(config Config) DataSource {
	switch config.Driver {
	case "mysql", "mariadb":
		if config.Host == "" {
			config.Host = "0.0.0.0"
		}
		if config.Port == 0 {
			config.Port = 3306
		}
		if config.Charset == "" {
			config.Charset = "utf8mb4"
		}
		if config.Location == "" {
			config.Location = "Local"
		}
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s&parseTime=%t&loc=%s", config.Username, config.Password, config.Host, config.Port, config.Database, config.Charset, true, config.Location)
		return NewMySQL(dsn, config.Database, config.DisableLogger)
	case "postgres", "psql", "postgresql":
		if config.Host == "" {
			config.Host = "0.0.0.0"
		}
		if config.Port == 0 {
			config.Port = 5432
		}
		if config.SslMode == "" {
			config.SslMode = "disable"
		}
		if config.Timezone == "" {
			config.Timezone = "UTC"
		}
		dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d sslmode=%s TimeZone=%s", config.Host, config.Username, config.Password, config.Database, config.Port, config.SslMode, config.Timezone)
		return NewPostgres(dsn, config.Database, config.DisableLogger)
	case "sql-server", "sqlserver", "mssql", "ms-sql":
		if config.Host == "" {
			config.Host = "0.0.0.0"
		}
		dsn := fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s", config.Username, config.Password, config.Host, config.Port, config.Database)
		return NewMsSQL(dsn, config.Database, config.DisableLogger)
	}
	return nil
}

func MigrateDB(srcCon, destCon DataSource, srcTables ...string) error {
	err := connect(srcCon, destCon)
	if err != nil {
		return err
	}
	err = MigrateTables(srcCon, destCon, srcTables...)
	if err != nil {
		return err
	}
	return MigrateViews(srcCon, destCon, srcTables...)
}

func MigrateTables(srcCon, destCon DataSource, srcTables ...string) error {
	err := connect(srcCon, destCon)
	if err != nil {
		return err
	}
	t, err := srcCon.GetTables()
	if err != nil {
		return err
	}
	for _, ta := range t {
		if len(srcTables) > 0 {
			if contains(srcTables, ta.Name) {
				err := CloneTable(srcCon, destCon, ta.Name, "")
				if err != nil {
					return err
				}
			}
		} else {
			err := CloneTable(srcCon, destCon, ta.Name, "")
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func MigrateViews(srcCon, destCon DataSource, srcTables ...string) error {
	err := connect(srcCon, destCon)
	if err != nil {
		return err
	}
	views, err := srcCon.GetViews()
	if err != nil {
		return err
	}
	for _, view := range views {
		if len(srcTables) > 0 {
			if contains(srcTables, view.Name) {
				err := CloneView(srcCon, destCon, view.Name, "", view.Definition)
				if err != nil {
					return err
				}
			}
		} else {
			err := CloneView(srcCon, destCon, view.Name, "", view.Definition)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func CloneTable(srcCon, destCon DataSource, src, dest string) error {
	err := connect(srcCon, destCon)
	if err != nil {
		return err
	}
	fields, err := srcCon.GetFields(src)
	if err != nil {
		return errors.NewE(err, fmt.Sprintf("Unable to get fields for %s", src), "CloneTable")
	}
	if dest == "" {
		dest = src
	}
	sql, err := destCon.GenerateSQL(dest, fields)
	if err != nil {
		return errors.NewE(err, fmt.Sprintf("Unable to get generate SQL for %s", dest), "CloneTable")
	}
	err = destCon.Exec(sql)
	if err != nil {
		return errors.NewE(err, fmt.Sprintf("Unable to clone table %s", dest), "CloneTable")
	}
	return nil
}

func CloneView(srcCon, destCon DataSource, src, dest, definition string) error {
	err := connect(srcCon, destCon)
	if err != nil {
		return err
	}
	switch destCon.GetType() {
	case "postgres":
		definition = strings.ReplaceAll(definition, fmt.Sprintf("`%s`.", srcCon.GetDBName()), "")
	case "mysql":
		definition = strings.ReplaceAll(definition, fmt.Sprintf(`"%s".`, srcCon.GetDBName()), "")
	}
	if dest == "" {
		dest = src
	}
	if definition == "" {
		return errors.New("View definition not provided")
	}
	sql := "DROP VIEW IF EXISTS " + src + ";"
	sql += "CREATE VIEW " + dest + " AS " + definition + ";"
	err = destCon.Exec(sql)
	if err != nil {
		fmt.Println(err.Error())
		// return errors.NewE(err, fmt.Sprintf("Unable to clone view %s", dest), "CloneTable")
	}
	return nil
}

func connect(srcCon, destCon DataSource) error {
	var err error
	if srcCon == nil {
		return errors.New("No source connection")
	}
	if destCon == nil {
		return errors.New("No destination connection")
	}
	srcCon, err = srcCon.Connect()
	if err != nil {
		return err
	}
	_, err = destCon.Connect()
	return err
}

func contains[T comparable](s []T, v T) bool {
	for _, vv := range s {
		if vv == v {
			return true
		}
	}
	return false
}
