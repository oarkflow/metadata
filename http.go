package metadata

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	stdHttp "net/http"
	"strings"

	"github.com/oarkflow/db"
	"github.com/oarkflow/errors"
	"github.com/oarkflow/protocol"
	"github.com/oarkflow/protocol/http"
)

type Http struct {
	Payload     protocol.Payload
	client      *protocol.HTTP
	AccessToken string
	ExpiresIn   int
}

func (h *Http) GetForeignKeys(table string) (fields []ForeignKey, err error) {
	return nil, nil
}

func (h *Http) GetIndices(table string) (fields []Index, err error) {
	return nil, nil
}

func (h *Http) Connect() (DataSource, error) {
	err := h.client.Setup()
	return h, err
}

func (h *Http) GetSources() ([]Source, error) {
	return nil, nil
}

func (h *Http) GetFields(table string) ([]Field, error) {
	return nil, nil
}

func (h *Http) Store(val any) error {
	panic("Implement me")
}

func (h *Http) StoreInBatches(val any, size int) error {
	panic("Implement me")
}

func (h *Http) GetCollection(table string) ([]map[string]any, error) {
	response, err := h.client.Handle(h.Payload)
	if err != nil {
		return nil, err
	}
	if h.client.Config.DataField == "" {
		h.client.Config.DataField = "data"
	}
	switch data := response.(type) {
	case []byte:
		resp, err := h.client.Config.ResponseCallback(data, h.client.Config.DataField)
		if err != nil {
			return nil, err
		}
		switch rows := resp.(type) {
		case []map[string]any:
			return rows, nil
		case map[string]any:
			return []map[string]any{
				rows,
			}, nil
		}
		return nil, err
	}
	return nil, nil
}

func (h *Http) Exec(sql string, values ...any) error {
	return nil
}

func (h *Http) DB() (*sql.DB, error) {
	return nil, nil
}

func (h *Http) GetRawCollection(query string, params ...map[string]any) ([]map[string]any, error) {
	// TODO implement me
	panic("implement me")
}

func (h *Http) GetRawPaginatedCollection(query string, params ...map[string]any) db.PaginatedResponse {
	// TODO implement me
	panic("implement me")
}

func (h *Http) GetPaginated(table string, paging db.Paging) db.PaginatedResponse {
	// TODO implement me
	panic("implement me")
}

func (h *Http) GetType() string {
	return "http"
}

func (h *Http) GetSingle(table string) (map[string]any, error) {
	response, err := h.client.Handle(h.Payload)
	if err != nil {
		return nil, err
	}
	switch data := response.(type) {
	case []byte:
		resp, err := h.client.Config.ResponseCallback(data, h.client.Config.DataField)
		if err != nil {
			return nil, err
		}
		switch rows := resp.(type) {
		case map[string]any:
			return rows, nil
		}
		return nil, err
	}
	return nil, nil
}

func (h *Http) GenerateSQL(table string, newFields []Field, indices ...Indices) (string, error) {
	return "", nil
}

func (h *Http) Migrate(table string, dst DataSource) error {
	return nil
}

func NewHttp(config *http.Options, payload protocol.Payload) *Http {
	if config.ResponseCallback == nil {
		config.ResponseCallback = defaultResponseCallback
	}
	httpClient, _ := protocol.NewHTTP(config)
	return &Http{
		client:  httpClient,
		Payload: payload,
	}
}

func NewHttpFromClient(httpClient *protocol.HTTP, payload protocol.Payload, dataField string) (*Http, error) {
	if httpClient.Config.ResponseCallback == nil {
		httpClient.Config.ResponseCallback = defaultResponseCallback
	}

	httpClient.Config.DataField = dataField
	connector := &Http{
		client:  httpClient,
		Payload: payload,
	}
	err := connector.SetupAuth()
	return connector, err
}

func defaultResponseCallback(response []byte, dataField ...string) (any, error) {
	field := ""
	if len(dataField) > 0 {
		field = dataField[0]
	}
	var rows []map[string]any
	var row map[string]any
	err := json.Unmarshal(response, &rows)
	if err == nil {
		return rows, nil
	}
	err = json.Unmarshal(response, &row)
	if err != nil {
		return nil, err
	}
	fieldParts := strings.Split(field, ".")
	var data any
	for i := 0; i < len(fieldParts); i++ {
		if val, ok := row[fieldParts[i]]; ok {
			if len(fieldParts) == 1 || i != len(fieldParts)-1 {
				data = val
			}
			if i == len(fieldParts)-1 {
				bt, err := json.Marshal(data)
				if err != nil {
					return nil, err
				}
				var rowCollection []map[string]any
				var rowSingle map[string]any
				err = json.Unmarshal(bt, &rowCollection)
				if err == nil {
					return rowCollection, nil
				}
				err = json.Unmarshal(bt, &rowSingle)
				if err != nil {
					return nil, err
				}
				return rowSingle, nil
			}
		}
	}
	return row, nil
}

func (h *Http) setupBasicAuth(auth *http.BasicAuth) error {
	var resp map[string]interface{}
	encoded := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", auth.Username, auth.Password)))
	data := make(map[string]interface{})
	if auth.Data != nil {
		for field, val := range auth.Data {
			data[field] = val
		}
	}
	headers := map[string]string{
		"Authorization": "Basic " + encoded,
	}
	if auth.Headers != nil {
		for key, val := range headers {
			headers[key] = val
		}
	}
	if auth.URL == "" {
		h.client.Config.MU.Lock()
		h.client.Config.Headers["Authorization"] = "Basic " + encoded
		h.client.Config.MU.Unlock()
		return nil
	}
	payload := protocol.Payload{
		URL:     auth.URL,
		Method:  auth.Method,
		Data:    data,
		Headers: headers,
	}
	bodyBytes, err := h.client.Handle(payload)
	if err != nil {
		return err
	}
	err = json.Unmarshal(bodyBytes.([]byte), &resp)
	if err != nil {
		return err
	}
	if auth.TokenField == "" {
		h.AccessToken = encoded
		h.client.Config.MU.Lock()
		h.client.Config.Headers["Authorization"] = "Bearer " + h.AccessToken
		h.client.Config.MU.Unlock()
		return nil
	}
	if val, ok := resp[auth.TokenField]; ok {
		if h.client.Config.Headers == nil {
			h.client.Config.Headers = make(map[string]string)
		}
		h.AccessToken = val.(string)
		if auth.ExpiryField != "" {
			if v, ok := resp[auth.ExpiryField]; ok {
				h.ExpiresIn = int(v.(float64))
			}
		}
		h.client.Config.MU.Lock()
		h.client.Config.Headers["Authorization"] = "Bearer " + h.AccessToken
		h.client.Config.MU.Unlock()
		return nil
	}
	return nil
}

func (h *Http) setupOAuth2(auth *http.OAuth2) error {
	var resp map[string]interface{}
	payload := protocol.Payload{
		URL:     auth.URL,
		Method:  "POST",
		Data:    auth.Data,
		Headers: auth.Headers,
	}
	response, err := h.client.Handle(payload)
	if err != nil {
		return err
	}
	bodyBytes, err := io.ReadAll(response.(*stdHttp.Response).Body)
	if err != nil {
		return err
	}
	err = json.Unmarshal(bodyBytes, &resp)
	if err != nil {
		return err
	}
	if auth.TokenField == "" {
		return errors.New("no token field defined")
	}
	if val, ok := resp[auth.TokenField]; ok {
		if h.client.Config.Headers == nil {
			h.client.Config.Headers = make(map[string]string)
		}
		h.AccessToken = val.(string)
		if auth.ExpiryField != "" {
			if v, ok := resp[auth.ExpiryField]; ok {
				h.ExpiresIn = int(v.(float64))
			}
		}
		h.client.Config.MU.Lock()
		h.client.Config.Headers["Authorization"] = "Bearer " + h.AccessToken
		h.client.Config.MU.Unlock()
		return nil
	}
	return fmt.Errorf("invalid Credential: %s", string(bodyBytes))
}

func (h *Http) setupBearerToken(auth *http.BearerToken) error {
	if h.client.Config.Headers == nil {
		h.client.Config.Headers = make(map[string]string)
	}
	h.AccessToken = auth.Token
	h.client.Config.MU.Lock()
	h.client.Config.Headers["Authorization"] = "Bearer " + h.AccessToken
	h.client.Config.MU.Unlock()
	return nil
}

func (h *Http) SetupAuth() error {
	switch auth := h.client.Config.Auth.(type) {
	case *http.BearerToken:
		return h.setupBearerToken(auth)
	case *http.BasicAuth:
		return h.setupBasicAuth(auth)
	case *http.OAuth2:
		return h.setupOAuth2(auth)
	default:
		return nil
	}
}
