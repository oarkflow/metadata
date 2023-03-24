package metadata

import (
	"strings"

	"github.com/oarkflow/db"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Postgres struct {
	schema string
	dsn    string
	client *gorm.DB
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
	err = p.client.Table("information_schema.tables").Select("table_name as name").Where("table_schema = ?", p.schema).Find(&tables).Error
	return
}

func (p *Postgres) GetFields(table string) (fields []Field, err error) {
	err = p.client.Raw("SELECT c.column_name as `name`, column_default as `default`, is_nullable as `is_nullable`, data_type as `data_type`, character_maximum_length as `length`, numeric_precision as `precision`, numeric_scale as `scale`,a.column_key as `key`, '' as extra  FROM INFORMATION_SCHEMA.COLUMNS c  LEFT JOIN (  select kcu.table_name,        'PRI' as column_key,        kcu.ordinal_position as position,        kcu.column_name as column_name from information_schema.table_constraints tco join information_schema.key_column_usage kcu       on kcu.constraint_name = tco.constraint_name      and kcu.constraint_schema = tco.constraint_schema      and kcu.constraint_name = tco.constraint_name where tco.constraint_type = 'PRIMARY KEY' and kcu.table_catalog = ? AND kcu.table_schema = 'public' AND kcu.table_name = ? order by kcu.table_schema,          kcu.table_name,          position          ) a ON c.table_name = a.table_name AND a.column_name = c.column_name          WHERE table_catalog = ? AND table_schema = 'public' AND c.table_name = ?;", p.schema, table, p.schema, table).Scan(&fields).Error
	return
}

func (p *Postgres) GetForeignKeys(table string) (fields []ForeignKey, err error) {
	err = p.client.Raw("select kcu.column_name as `name`, rel_kcu.table_name as referenced_table, rel_kcu.column_name as referenced_column from information_schema.table_constraints tco join information_schema.key_column_usage kcu           on tco.constraint_schema = kcu.constraint_schema           and tco.constraint_name = kcu.constraint_name join information_schema.referential_constraints rco           on tco.constraint_schema = rco.constraint_schema           and tco.constraint_name = rco.constraint_name join information_schema.key_column_usage rel_kcu           on rco.unique_constraint_schema = rel_kcu.constraint_schema           and rco.unique_constraint_name = rel_kcu.constraint_name           and kcu.ordinal_position = rel_kcu.ordinal_position where tco.constraint_type = 'FOREIGN KEY' and kcu.table_catalog = ? AND kcu.table_schema = 'public' AND kcu.table_name = ? order by kcu.table_schema,          kcu.table_name,          kcu.ordinal_position;", p.schema, table).Scan(&fields).Error
	return
}

func (p *Postgres) GetIndices(table string) (fields []Index, err error) {
	err = p.client.Raw("select DISTINCT kcu.constraint_name as `name`, kcu.column_name as `column_name`, enforced as `nullable` from information_schema.table_constraints tco join information_schema.key_column_usage kcu       on kcu.constraint_name = tco.constraint_name      and kcu.constraint_schema = tco.constraint_schema      and kcu.constraint_name = tco.constraint_name      WHERE tco.table_catalog = ? AND tco.table_schema = 'public' AND tco.table_name = ?;", p.schema, table).Scan(&fields).Error
	return
}

func (p *Postgres) GetCollection(table string) ([]map[string]any, error) {
	var rows []map[string]any
	if err := p.client.Table(table).Find(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
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

func NewPostgres(dsn, database string) *Postgres {
	return &Postgres{
		schema: database,
		dsn:    dsn,
		client: nil,
	}
}
