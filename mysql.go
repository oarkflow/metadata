package metadata

import (
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
	err = p.client.Raw("SELECT column_name as `name`, column_default as `default`, is_nullable as `is_nullable`, data_type as data_type, character_maximum_length as `length`, numeric_precision as `precision`, numeric_scale as `scale`, column_comment as `comment`, column_key as `key`, extra as extra FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME =  ? AND TABLE_SCHEMA = ?;", table, p.schema).Scan(&fields).Error
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
	// TODO implement me
	panic("implement me")
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

func NewMySQL(dsn, database string) *MySQL {
	return &MySQL{
		schema: database,
		dsn:    dsn,
		client: nil,
	}
}
