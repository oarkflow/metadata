package metadata

import (
	"database/sql"
	"fmt"

	"github.com/oarkflow/db"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type MsSQL struct {
	schema     string
	dsn        string
	client     *gorm.DB
	disableLog bool
}

func (p *MsSQL) Connect() (DataSource, error) {
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
		db1, err := gorm.Open(sqlserver.Open(p.dsn), config)
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

func (p *MsSQL) LastInsertedID() (id any, err error) {
	err = p.client.Raw("SELECT SCOPE_IDENTITY();").Scan(&id).Error
	return
}

func (p *MsSQL) MaxID(table, field string) (id any, err error) {
	err = p.client.Raw(fmt.Sprintf("SELECT MAX(%s) FROM %s;", field, table)).Scan(&id).Error
	return
}

func (p *MsSQL) GetSources() (tables []Source, err error) {
	// TODO implement me
	panic("implement me")
}

func (p *MsSQL) GetDataTypeMap(dataType string) string {
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

func (p *MsSQL) Begin() DataSource {
	tx := p.client.Begin()
	return NewFromClient(tx)
}

func (p *MsSQL) Error() error {
	return p.client.Error
}

func (p *MsSQL) Commit() DataSource {
	tx := p.client.Commit()
	return NewFromClient(tx)
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

func (p *MsSQL) GetRawPaginatedCollection(query string, paging db.Paging, params ...map[string]any) db.PaginatedResponse {
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

func NewMsSQL(dsn, database string, disableLog bool) *MsSQL {
	return &MsSQL{
		schema:     database,
		dsn:        dsn,
		disableLog: disableLog,
	}
}
