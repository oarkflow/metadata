package metadata

import (
	"fmt"
	"time"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/dbresolver"
	"github.com/oarkflow/squealx/drivers/mssql"
	"github.com/oarkflow/squealx/orm"
)

type MsSQL struct {
	schema     string
	dsn        string
	id         string
	client     dbresolver.DBResolver
	disableLog bool
	pooling    ConnectionPooling
	config     Config
}

func (p *MsSQL) Connect() (DataSource, error) {
	if p.client == nil {
		db1, err := mssql.Open(p.dsn, p.id)
		if err != nil {
			return nil, err
		}
		p.client, err = dbresolver.New(dbresolver.WithMasterDBs(db1), dbresolver.WithReadWritePolicy(dbresolver.ReadWrite))
		if err != nil {
			return nil, err
		}
		p.client.SetConnMaxLifetime(time.Duration(p.pooling.MaxLifetime) * time.Second)
		p.client.SetConnMaxIdleTime(time.Duration(p.pooling.MaxIdleTime) * time.Second)
		p.client.SetMaxOpenConns(p.pooling.MaxOpenCons)
		p.client.SetMaxIdleConns(p.pooling.MaxIdleCons)
	}
	return p, nil
}

func (p *MsSQL) GetDBName(database ...string) string {
	return p.schema
}

func (p *MsSQL) Config() Config {
	return p.config
}

func (p *MsSQL) LastInsertedID() (id any, err error) {
	err = p.client.Select(&id, "SELECT SCOPE_IDENTITY();")
	return
}

func (p *MsSQL) MaxID(table, field string) (id any, err error) {
	err = p.client.Select(&id, fmt.Sprintf("SELECT MAX(%s) FROM %s;", field, table))
	return
}

func (p *MsSQL) Close() error {
	return p.client.Close()
}

func (p *MsSQL) Client() any {
	return p.client
}

func (p *MsSQL) GetSources(database ...string) (tables []Source, err error) {
	// TODO implement me
	panic("implement me")
}

func (p *MsSQL) GetDataTypeMap(dataType string) string {
	panic("implement me")
}

func (p *MsSQL) GetTables(database ...string) (tables []Source, err error) {
	// TODO implement me
	panic("implement me")
}

func (p *MsSQL) GetViews(database ...string) (tables []Source, err error) {
	// TODO implement me
	panic("implement me")
}

func (p *MsSQL) GetFields(table string, database ...string) (fields []Field, err error) {
	// TODO implement me
	panic("implement me")
}

func (p *MsSQL) GetForeignKeys(table string, database ...string) (fields []ForeignKey, err error) {
	// TODO implement me
	panic("implement me")
}

func (p *MsSQL) Begin() (squealx.SQLTx, error) {
	return p.client.Begin()
}

func (p *MsSQL) GetIndices(table string, database ...string) (fields []Index, err error) {
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

func (p *MsSQL) GetRawPaginatedCollection(query string, paging squealx.Paging, params ...map[string]any) squealx.PaginatedResponse {
	// TODO implement me
	panic("implement me")
}

func (p *MsSQL) GetPaginated(table string, paging squealx.Paging) squealx.PaginatedResponse {
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
	_, err := p.client.Exec(orm.InsertQuery(table, val), val)
	return err
}

func (p *MsSQL) StoreInBatches(table string, val any, size int) error {
	return processBatchInsert(p.client, table, val, size)
}

func (p *MsSQL) GetType() string {
	// TODO implement me
	panic("implement me")
}

func NewMsSQL(id, dsn, database string, disableLog bool, pooling ConnectionPooling) *MsSQL {
	return &MsSQL{
		schema:     database,
		dsn:        dsn,
		id:         id,
		disableLog: disableLog,
		pooling:    pooling,
	}
}
