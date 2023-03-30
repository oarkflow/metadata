package metadata

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/oarkflow/db"
	"github.com/oarkflow/errors"
)

type Any json.RawMessage

// Scan scan value into Jsonb, implements sql.Scanner interface
func (j *Any) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("Failed to unmarshal JSONB value:", value))
	}

	result := json.RawMessage{}
	err := json.Unmarshal(bytes, &result)
	*j = Any(result)
	return err
}

// Value return json value, implement driver.Valuer interface
func (j Any) Value() (driver.Value, error) {
	if len(j) == 0 {
		return nil, nil
	}
	return json.RawMessage(j).MarshalJSON()
}

type Config struct {
	Name     string `json:"name"`
	Key      string `json:"key"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Driver   string `json:"driver"`
	Username string `json:"username"`
	Password string `json:"password"`
	Database string `json:"database"`
	SslMode  string `json:"ssl_mode"`
	Timezone string `json:"timezone"`
	Charset  string `json:"charset"`
	Location string `json:"location"`
}

type Source struct {
	Name  string `json:"name" gorm:"column:name"`
	Title string `json:"title" gorm:"-"`
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

type SourceFields struct {
	Name  string  `json:"name" gorm:"column:table_name"`
	Title string  `json:"title" gorm:"-"`
	Field []Field `json:"fields"`
}

type DataSource interface {
	Connect() (DataSource, error)
	GetSources() (tables []Source, err error)
	GetFields(table string) (fields []Field, err error)
	GetForeignKeys(table string) (fields []ForeignKey, err error)
	GetIndices(table string) (fields []Index, err error)
	GetCollection(table string) ([]map[string]any, error)
	Exec(sql string, values ...any) error
	GetRawCollection(query string, params ...map[string]any) ([]map[string]any, error)
	GetRawPaginatedCollection(query string, params ...map[string]any) db.PaginatedResponse
	GetPaginated(table string, paging db.Paging) db.PaginatedResponse
	GetSingle(table string) (map[string]any, error)
	GenerateSQL(table string, newFields []Field) (string, error)
	Migrate(table string, dst DataSource) error
	GetType() string
}

func New(config Config) DataSource {
	switch config.Driver {
	case "mysql":
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
		return NewMySQL(dsn, config.Database)
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
		return NewPostgres(dsn, config.Database)
	}
	return nil
}
