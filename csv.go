package metadata

import (
	"fmt"
	"os"
	"strings"

	"github.com/oarkflow/errors"
	"github.com/oarkflow/squealx"

	"github.com/oarkflow/metadata/sql"
)

type CSVDataSource struct {
	fileName string
	config   Config
}

func NewCSVDataSource(fileName string) *CSVDataSource {
	return &CSVDataSource{
		fileName: fileName,
		config: Config{
			Database: fmt.Sprintf("read_file('%s')", fileName),
			Driver:   "csv",
		},
	}
}

func (ds *CSVDataSource) Config() Config {
	return ds.config
}

func (ds *CSVDataSource) GetDBName(database ...string) string {
	if len(database) > 0 {
		return database[0]
	}
	return ds.fileName
}

func (ds *CSVDataSource) GetSources(database ...string) ([]Source, error) {
	return []Source{{Name: ds.fileName}}, nil
}

func (ds *CSVDataSource) GetDataTypeMap(dataType string) string {
	// For CSV, all data is string.
	return "string"
}

func (ds *CSVDataSource) GetTables(database ...string) ([]Source, error) {
	return ds.GetSources(database...)
}

func (ds *CSVDataSource) GetViews(database ...string) ([]Source, error) {
	return []Source{}, nil
}

func (ds *CSVDataSource) GetForeignKeys(table string, database ...string) ([]ForeignKey, error) {
	return nil, errors.New("foreign keys not supported for CSV datasource")
}

func (ds *CSVDataSource) GetIndices(table string, database ...string) ([]Index, error) {
	return nil, errors.New("indices not supported for CSV datasource")
}

func (ds *CSVDataSource) Begin() (squealx.SQLTx, error) {
	return nil, errors.New("transactions not supported for CSV datasource")
}

func (ds *CSVDataSource) Exec(sql string, values ...any) error {
	return errors.New("Exec not supported for CSV datasource")
}

func (ds *CSVDataSource) GenerateSQL(table string, newFields []Field, indices ...Indices) (string, error) {
	return "", errors.New("GenerateSQL not supported for CSV datasource")
}

func (ds *CSVDataSource) LastInsertedID() (any, error) {
	return nil, errors.New("LastInsertedID not supported for CSV datasource")
}

func (ds *CSVDataSource) MaxID(table, field string) (any, error) {
	return nil, errors.New("MaxID not supported for CSV datasource")
}

func (ds *CSVDataSource) Client() any {
	return nil
}

func (ds *CSVDataSource) Connect() (DataSource, error) {
	if _, err := os.Stat(ds.fileName); os.IsNotExist(err) {
		return nil, fmt.Errorf("file %s does not exist", ds.fileName)
	}
	return ds, nil
}

// Refactored GetFields for CSV: reads the header and a sample of rows to infer the field type.
func (ds *CSVDataSource) GetFields(table string, database ...string) ([]Field, error) {
	file, err := os.Open(ds.fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	fieldMap, err := InferCSVFileTypes(file)
	if err != nil {
		return nil, err
	}
	var fields []Field
	for key, val := range fieldMap {
		fields = append(fields, Field{Name: key, DataType: val})
	}
	return fields, nil
}

func (ds *CSVDataSource) GetCollection(table string) ([]map[string]any, error) {
	if !strings.Contains(table, "read_file") {
		table = fmt.Sprintf("read_file('%s')", ds.fileName)
	}
	query := fmt.Sprintf("SELECT * FROM %s", table)
	records, err := sql.Query(query)
	if err != nil {
		return nil, err
	}
	return records, nil
}

func (ds *CSVDataSource) GetRawCollection(query string, params ...map[string]any) ([]map[string]any, error) {
	if strings.Contains(query, "FROM collection") {
		table := fmt.Sprintf("read_file('%s')", ds.fileName)
		query = strings.Replace(query, "FROM collection", fmt.Sprintf("FROM %s", table), 1)
	}
	records, err := sql.Query(query)
	if err != nil {
		return nil, err
	}
	return records, nil
}

func (ds *CSVDataSource) GetRawPaginatedCollection(query string, paging squealx.Paging, params ...map[string]any) squealx.PaginatedResponse {
	if strings.Contains(query, "FROM collection") {
		table := fmt.Sprintf("read_file('%s')", ds.fileName)
		query = strings.Replace(query, "FROM collection", fmt.Sprintf("FROM %s", table), 1)
	}
	records, err := ds.GetCollection(query)
	if err != nil {
		return squealx.PaginatedResponse{Error: err}
	}
	total := len(records)
	start := paging.Limit * (paging.Page - 1)
	end := start + paging.Limit
	if start > total {
		start = total
	}
	if end > total {
		end = total
	}
	pagedRecords := records[start:end]
	pagination := &squealx.Pagination{
		TotalRecords: int64(total),
		TotalPage:    (total + paging.Limit - 1) / paging.Limit,
		Offset:       start,
		Limit:        paging.Limit,
		Page:         paging.Page,
		PrevPage:     paging.Page - 1,
		NextPage:     paging.Page + 1,
	}
	return squealx.PaginatedResponse{
		Items:      pagedRecords,
		Pagination: pagination,
	}
}

func (ds *CSVDataSource) GetPaginated(table string, paging squealx.Paging) squealx.PaginatedResponse {
	if !strings.Contains(table, "read_file") {
		table = fmt.Sprintf("read_file('%s')", ds.fileName)
	}
	query := fmt.Sprintf("SELECT * FROM %s", table)
	return ds.GetRawPaginatedCollection(query, paging)
}

func (ds *CSVDataSource) GetSingle(table string) (map[string]any, error) {
	records, err := ds.GetCollection(table)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, errors.New("no records found")
	}
	return records[0], nil
}

func (ds *CSVDataSource) Migrate(table string, dst DataSource) error {
	records, err := ds.GetCollection(table)
	if err != nil {
		return err
	}
	for _, record := range records {
		if err := dst.Store(table, record); err != nil {
			return err
		}
	}
	return nil
}

func (ds *CSVDataSource) GetType() string {
	return "csv"
}

func (ds *CSVDataSource) Store(table string, val any) error {
	return errors.New("Store not supported for CSV datasource")
}

func (ds *CSVDataSource) StoreInBatches(table string, val any, size int) error {
	return errors.New("StoreInBatches not supported for CSV datasource")
}

func (ds *CSVDataSource) Close() error {
	return nil
}
