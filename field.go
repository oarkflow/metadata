package metadata

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"

	"github.com/oarkflow/date"
)

// isTime returns true if the given string is a valid time according to the date.ParseFormat parser.
func isTime(s string) bool {
	_, err := date.ParseFormat(s)
	return err == nil
}

// cleanField trims whitespace and removes thousands separators if the field appears numeric.
func cleanField(s string) string {
	s = strings.TrimSpace(s)
	re := regexp.MustCompile(`^\d{1,3}(,\d{3})*(\.\d+)?$`)
	if re.MatchString(s) {
		return strings.ReplaceAll(s, ",", "")
	}
	return s
}

// inferTypeFromString attempts to deduce a type from a string.
func inferTypeFromString(s string) string {
	s = cleanField(s)
	if s == "" {
		return "empty"
	}
	if _, err := strconv.Atoi(s); err == nil {
		return "int"
	}
	if _, err := strconv.ParseInt(s, 10, 64); err == nil {
		return "int64"
	}
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		return "float64"
	}
	lower := strings.ToLower(s)
	if lower == "true" || lower == "false" {
		return "bool"
	}
	if isTime(s) {
		return "time.Time"
	}
	return "string"
}

// mergeTypes merges two inferred CSV types into one, promoting to a more general type if needed.
func mergeTypes(t1, t2 string) string {
	if t1 == t2 {
		return t1
	}
	if t1 == "empty" {
		return t2
	}
	if t2 == "empty" {
		return t1
	}
	if (t1 == "int" && t2 == "int64") || (t1 == "int64" && t2 == "int") {
		return "int64"
	}
	if (t1 == "int" || t1 == "int64") && t2 == "float64" ||
		(t2 == "int" || t2 == "int64") && t1 == "float64" {
		return "float64"
	}
	return "string"
}

// InferCSVColumnType infers the type for a CSV column given its values.
func InferCSVColumnType(values []string) string {
	inferredType := "empty"
	timeCount := 0
	nonEmptyCount := 0
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v != "" {
			nonEmptyCount++
			if isTime(v) {
				timeCount++
			}
			typ := inferTypeFromString(v)
			if inferredType == "empty" {
				inferredType = typ
			} else {
				inferredType = mergeTypes(inferredType, typ)
			}
		}
	}
	if nonEmptyCount > 0 && timeCount == nonEmptyCount {
		return "time.Time"
	}
	if inferredType == "empty" {
		return "string"
	}
	return inferredType
}

// fixRecord adjusts a CSV record to have the expected number of fields.
// It pads short records and merges fields if there are too many.
func fixRecord(record []string, expected int, affectedIndex int) ([]string, error) {
	if len(record) < expected {
		for len(record) < expected {
			record = append(record, "")
		}
		return record, nil
	}
	if len(record) > expected {
		extraTokens := len(record) - expected
		joinStart := affectedIndex
		joinEnd := affectedIndex + extraTokens + 1
		if joinEnd > len(record) {
			return nil, fmt.Errorf("cannot merge fields properly in record: %v", record)
		}
		joinedField := strings.Join(record[joinStart:joinEnd], "")
		var fixed []string
		fixed = append(fixed, record[:affectedIndex]...)
		fixed = append(fixed, joinedField)
		fixed = append(fixed, record[joinEnd:]...)
		if len(fixed) != expected {
			return nil, fmt.Errorf("after merging, expected %d fields but got %d: %v", expected, len(fixed), fixed)
		}
		return fixed, nil
	}
	return record, nil
}

// InferCSVFileTypes reads CSV data and returns a map from header names to the inferred type.
func InferCSVFileTypes(reader io.Reader) (map[string]string, error) {
	r := csv.NewReader(reader)
	r.FieldsPerRecord = -1
	headers, err := r.Read()
	if err != nil {
		return nil, err
	}
	expectedFields := len(headers)
	columns := make(map[int][]string)
	for i := range headers {
		columns[i] = []string{}
	}
	lineNum := 2
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading CSV on line %d: %v", lineNum, err)
		}
		for i := range record {
			record[i] = cleanField(record[i])
		}
		if len(record) != expectedFields {
			fixed, err := fixRecord(record, expectedFields, 2)
			if err != nil {
				return nil, fmt.Errorf("could not fix record on line %d: %v", lineNum, err)
			}
			record = fixed
		}
		if len(record) != expectedFields {
			return nil, fmt.Errorf("record on line %d: expected %d fields but got %d", lineNum, expectedFields, len(record))
		}
		for i, v := range record {
			columns[i] = append(columns[i], v)
		}
		lineNum++
	}
	result := make(map[string]string)
	for i, header := range headers {
		result[header] = InferCSVColumnType(columns[i])
	}
	return result, nil
}

// InferJSONFieldType infers the type for a single JSON value.
func InferJSONFieldType(value interface{}) string {
	switch v := value.(type) {
	case json.Number:
		if _, err := v.Int64(); err == nil {
			if strings.Contains(v.String(), ".") {
				return "float64"
			}
			return "int"
		}
		if f, err := v.Float64(); err == nil {
			if f == float64(int64(f)) {
				return "int"
			}
			return "float64"
		}
	case bool:
		return "bool"
	case string:
		if isTime(v) {
			return "time.Time"
		}
		return "string"
	case nil:
		return "null"
	case []interface{}:
		// For slices, infer the type of each element and merge.
		if len(v) == 0 {
			return "[]any"
		}
		var unifiedType string
		homogeneous := true
		for i, elem := range v {
			t := InferJSONFieldType(elem)
			if i == 0 {
				unifiedType = t
			} else {
				unifiedType = mergeJSONTypes(unifiedType, t)
				if unifiedType == "any" {
					homogeneous = false
				}
			}
		}
		if homogeneous {
			return "[]" + unifiedType
		}
		return "[]any"
	case map[string]interface{}:
		// For nested objects, you might recursively inspect fields.
		// Here we simply return a placeholder type.
		return "map[string]any"
	}
	return "unknown"
}

// mergeJSONTypes merges two inferred JSON types into one.
func mergeJSONTypes(t1, t2 string) string {
	if t1 == t2 {
		return t1
	}
	if t1 == "null" {
		return t2
	}
	if t2 == "null" {
		return t1
	}
	if strings.HasPrefix(t1, "[]") && strings.HasPrefix(t2, "[]") {
		inner1 := t1[2:]
		inner2 := t2[2:]
		mergedInner := mergeJSONTypes(inner1, inner2)
		return "[]" + mergedInner
	}
	if (t1 == "int" && t2 == "float64") || (t1 == "float64" && t2 == "int") {
		return "float64"
	}
	return "any"
}

// InferJSONFileType processes JSON data that is either a single object
// (map[string]interface{}) or an array of objects, merging field types across records.
// It returns a map[string]string that maps field names to their inferred types.
func InferJSONFileType(data interface{}) (map[string]string, error) {
	result := make(map[string]string)
	switch v := data.(type) {
	case map[string]interface{}:
		// Single JSON object.
		for key, val := range v {
			result[key] = InferJSONFieldType(val)
		}
		return result, nil
	case []interface{}:
		// Array of JSON objects.
		for _, item := range v {
			record, ok := item.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("array contains non-object items")
			}
			for key, val := range record {
				t := InferJSONFieldType(val)
				if existing, exists := result[key]; exists {
					result[key] = mergeJSONTypes(existing, t)
				} else {
					result[key] = t
				}
			}
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unexpected JSON data format")
	}
}
