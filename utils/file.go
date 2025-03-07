package utils

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/goccy/go-json"

	"github.com/oarkflow/convert"
)

type Record = map[string]any

func ProcessFile(filename string, callbacks ...func(Record)) ([]Record, error) {
	var callback func(Record)
	if len(callbacks) > 0 {
		callback = callbacks[0]
	}
	var records []Record
	processCallback := func(record Record) {
		if callback != nil {
			callback(record)
		} else {
			records = append(records, record)
		}
	}
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".json":
		if err := ProcessJSONStream(filename, processCallback); err != nil {
			return nil, err
		}
	case ".csv":
		if err := ProcessCSVFile(filename, processCallback); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported file extension: %s", ext)
	}
	return records, nil
}

func ProcessJSONStream(filename string, callback func(Record)) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
	}()
	decoder := json.NewDecoder(file)
	_, err = decoder.Token()
	if err != nil {
		return err
	}
	for decoder.More() {
		var obj Record
		if err := decoder.Decode(&obj); err != nil {
			return err
		}
		if callback != nil {
			callback(obj)
		}
	}
	return nil
}

func ProcessCSVFile(filename string, callback func(Record)) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	reader := csv.NewReader(file)
	reader.ReuseRecord = true
	headers, err := reader.Read()
	if err != nil {
		return err
	}
	headerCopy := make([]string, len(headers))
	copy(headerCopy, headers)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		row := make(Record)
		for i, header := range headerCopy {
			if i < len(record) {
				row[header] = record[i]
			} else {
				row[header] = nil
			}
		}
		if callback != nil {
			callback(row)
		}
	}
	return nil
}

func ApplyAliasToRecord(row Record, alias string) Record {
	newRow := make(Record)
	for k, v := range row {
		newRow[alias+"."+k] = v
	}
	return newRow
}

func CompareValues(a, b any) int {
	af, errA := convert.ToFloat64(a)
	bf, errB := convert.ToFloat64(b)
	if errA && errB {
		if af < bf {
			return -1
		} else if af > bf {
			return 1
		} else {
			return 0
		}
	}
	as := fmt.Sprintf("%v", a)
	bs := fmt.Sprintf("%v", b)
	return strings.Compare(as, bs)
}

func DeepEqual(a, b Record) bool {
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

func Union(left, right []Record) []Record {
	m := map[string]Record{}
	for _, row := range left {
		key := fmt.Sprintf("%v", row)
		m[key] = row
	}
	for _, row := range right {
		key := fmt.Sprintf("%v", row)
		m[key] = row
	}
	var result []Record
	for _, row := range m {
		result = append(result, row)
	}
	return result
}

func Intersect(left, right []Record) []Record {
	m := map[string]Record{}
	for _, row := range left {
		key := fmt.Sprintf("%v", row)
		m[key] = row
	}
	var result []Record
	for _, row := range right {
		key := fmt.Sprintf("%v", row)
		if _, ok := m[key]; ok {
			result = append(result, row)
		}
	}
	return result
}

func Except(left, right []Record) []Record {
	m := map[string]bool{}
	for _, row := range right {
		key := fmt.Sprintf("%v", row)
		m[key] = true
	}
	var result []Record
	for _, row := range left {
		key := fmt.Sprintf("%v", row)
		if !m[key] {
			result = append(result, row)
		}
	}
	return result
}

func MergeRows(left, right Record, rightAlias string) Record {
	merged := make(Record)
	if left != nil {
		for k, v := range left {
			merged[k] = v
		}
	}
	if right != nil {
		for k, v := range right {
			if rightAlias != "" {
				merged[rightAlias+"."+k] = v
			} else {
				merged[k] = v
			}
		}
	}
	return merged
}

func RecordKey(left, right Record) string {
	return fmt.Sprintf("%v|%v", left, right)
}
