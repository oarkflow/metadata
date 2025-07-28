package metadata

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/oarkflow/errors"
	"github.com/oarkflow/squealx"

	"github.com/oarkflow/metadata/sql"
)

type JSONDataSource struct {
	fileName string
	config   Config
}

func NewJSONDataSource(fileName string) *JSONDataSource {
	return &JSONDataSource{
		fileName: fileName,
		config: Config{
			Database: fmt.Sprintf("read_file('%s')", fileName),
			Driver:   "json",
		},
	}
}

func (ds *JSONDataSource) Config() Config {
	return ds.config
}

func (ds *JSONDataSource) GetDBName(database ...string) string {
	if len(database) > 0 {
		return database[0]
	}
	return ds.fileName
}

func (ds *JSONDataSource) GetSources(database ...string) ([]Source, error) {
	return []Source{{Name: ds.fileName}}, nil
}

func (ds *JSONDataSource) GetDataTypeMap(dataType string) string {
	// For JSON, we assume all fields are strings.
	return "string"
}

func (ds *JSONDataSource) GetTables(database ...string) ([]Source, error) {
	return ds.GetSources(database...)
}

func (ds *JSONDataSource) GetViews(database ...string) ([]Source, error) {
	return []Source{}, nil
}

func (ds *JSONDataSource) GetForeignKeys(table string, database ...string) ([]ForeignKey, error) {
	return nil, errors.New("foreign keys not supported for JSON datasource")
}

func (ds *JSONDataSource) GetIndices(table string, database ...string) ([]Index, error) {
	return nil, errors.New("indices not supported for JSON datasource")
}

func (ds *JSONDataSource) Begin() (squealx.SQLTx, error) {
	return nil, errors.New("transactions not supported for JSON datasource")
}

func (ds *JSONDataSource) Exec(sql string, values ...any) error {
	return errors.New("Exec not supported for JSON datasource")
}

func (ds *JSONDataSource) GenerateSQL(table string, newFields []Field, indices ...Indices) (string, error) {
	return "", errors.New("GenerateSQL not supported for JSON datasource")
}

func (ds *JSONDataSource) LastInsertedID() (any, error) {
	return nil, errors.New("LastInsertedID not supported for JSON datasource")
}

func (ds *JSONDataSource) MaxID(table, field string) (any, error) {
	return nil, errors.New("MaxID not supported for JSON datasource")
}

func (ds *JSONDataSource) Client() any {
	return nil
}

func (ds *JSONDataSource) Connect() (DataSource, error) {
	if _, err := os.Stat(ds.fileName); os.IsNotExist(err) {
		return nil, fmt.Errorf("file %s does not exist", ds.fileName)
	}
	return ds, nil
}

func (ds *JSONDataSource) GetFields(table string, database ...string) ([]Field, error) {
	file, err := os.Open(ds.fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.UseNumber() // To preserve number formats
	var jsonObj interface{}
	if err := decoder.Decode(&jsonObj); err != nil {
		return nil, err
	}
	fieldMap, err := InferJSONFileType(jsonObj)
	if err != nil {
		return nil, err
	}
	var fields []Field
	for key, val := range fieldMap {
		fields = append(fields, Field{Name: key, DataType: val})
	}
	return fields, nil
}

func (ds *JSONDataSource) GetCollection(table string) ([]map[string]any, error) {
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

func (ds *JSONDataSource) GetRawCollection(query string, params ...map[string]any) ([]map[string]any, error) {
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

func (ds *JSONDataSource) GetRawPaginatedCollection(query string, paging squealx.Paging, params ...map[string]any) squealx.PaginatedResponse {
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

func (ds *JSONDataSource) GetPaginated(table string, paging squealx.Paging) squealx.PaginatedResponse {
	if !strings.Contains(table, "read_file") {
		table = fmt.Sprintf("read_file('%s')", ds.fileName)
	}
	query := fmt.Sprintf("SELECT * FROM %s", table)
	return ds.GetRawPaginatedCollection(query, paging)
}

func (ds *JSONDataSource) GetSingle(table string) (map[string]any, error) {
	records, err := ds.GetCollection(table)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, errors.New("no records found")
	}
	return records[0], nil
}

func (ds *JSONDataSource) Migrate(table string, dst DataSource) error {
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

func (ds *JSONDataSource) GetType() string {
	return "json"
}

func (ds *JSONDataSource) Store(table string, val any) error {
	return errors.New("Store not supported for JSON datasource")
}

func (ds *JSONDataSource) StoreInBatches(table string, val any, size int) error {
	return errors.New("StoreInBatches not supported for JSON datasource")
}

func (ds *JSONDataSource) Close() error {
	return nil
}

func (ds *JSONDataSource) FieldAsString(f Field, action string) string {
	// JSON datasource doesn't support SQL generation
	return ""
}

func (ds *JSONDataSource) GetTheIndices(table string, database ...string) ([]Indices, error) {
	// JSON datasource doesn't support indices
	return []Indices{}, nil
}
