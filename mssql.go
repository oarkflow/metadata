package metadata

import (
	"database/sql"

	"github.com/oarkflow/db"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
)

type MsSQL struct {
	schema string
	dsn    string
	client *gorm.DB
}

func (p *MsSQL) Connect() (DataSource, error) {
	if p.client == nil {
		db1, err := gorm.Open(sqlserver.Open(p.dsn), &gorm.Config{
			DisableForeignKeyConstraintWhenMigrating: true,
		})
		if err != nil {
			return nil, err
		}
		p.client = db1
	}
	return p, nil
}

func (p *MsSQL) GetDBName() string {
	return p.schema
}

func (p *MsSQL) GetSources() (tables []Source, err error) {
	// TODO implement me
	panic("implement me")
}

func (p *MsSQL) GetTables() (tables []Source, err error) {
	// TODO implement me
	panic("implement me")
}

func (p *MsSQL) GetViews() (tables []Source, err error) {
	// TODO implement me
	panic("implement me")
}

func (p *MsSQL) GetFields(table string) (fields []Field, err error) {
	// TODO implement me
	panic("implement me")
}

func (p *MsSQL) GetForeignKeys(table string) (fields []ForeignKey, err error) {
	// TODO implement me
	panic("implement me")
}

func (p *MsSQL) GetIndices(table string) (fields []Index, err error) {
	// TODO implement me
	panic("implement me")
}

func (p *MsSQL) GetCollection(table string) ([]map[string]any, error) {
	// TODO implement me
	panic("implement me")
}

func (p *MsSQL) Exec(sql string, values ...any) error {
	// TODO implement me
	panic("implement me")
}

func (p *MsSQL) GetRawCollection(query string, params ...map[string]any) ([]map[string]any, error) {
	// TODO implement me
	panic("implement me")
}

func (p *MsSQL) GetRawPaginatedCollection(query string, params ...map[string]any) db.PaginatedResponse {
	// TODO implement me
	panic("implement me")
}

func (p *MsSQL) GetPaginated(table string, paging db.Paging) db.PaginatedResponse {
	// TODO implement me
	panic("implement me")
}

func (p *MsSQL) GetSingle(table string) (map[string]any, error) {
	// TODO implement me
	panic("implement me")
}

func (p *MsSQL) GenerateSQL(table string, newFields []Field, indices ...Indices) (string, error) {
	// TODO implement me
	panic("implement me")
}

func (p *MsSQL) Migrate(table string, dst DataSource) error {
	// TODO implement me
	panic("implement me")
}

func (p *MsSQL) Store(table string, val any) error {
	return p.client.Table(table).Create(val).Error
}

func (p *MsSQL) StoreInBatches(table string, val any, size int) error {
	if size <= 0 {
		size = 100
	}
	return p.client.Table(table).CreateInBatches(val, size).Error
}

func (p *MsSQL) GetType() string {
	// TODO implement me
	panic("implement me")
}

func (p *MsSQL) DB() (*sql.DB, error) {
	return p.client.DB()
}

func NewMsSQL(dsn, database string) *MsSQL {
	return &MsSQL{
		schema: database,
		dsn:    dsn,
	}
}
