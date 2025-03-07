package sql

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	"github.com/oarkflow/convert"

	"github.com/oarkflow/metadata/utils"
)

func (tr *TableReference) loadData() ([]utils.Record, error) {
	switch strings.ToLower(tr.Source) {
	case "read_file":
		return utils.ProcessFile(tr.Name)
	case "read_db":
		return utils.ProcessFile(tr.Name)
	case "read_api":
		resp, err := http.Get(tr.Name)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		var rows []utils.Record
		if err := json.Unmarshal(body, &rows); err != nil {
			return nil, err
		}
		return rows, nil
	default:
		return nil, fmt.Errorf("unsupported data source: %s", tr.Source)
	}
}

func (query *SQL) executeQuery(rows []utils.Record) ([]utils.Record, error) {
	ctx := NewEvalContext()
	var filteredRows []utils.Record

	// Flag to mark if we applied an indexed lookup.
	indexApplied := false

	// Attempt to apply O(1) filtering when WHERE is a simple equality (e.g. col = value).
	if query.Where != nil {
		if be, ok := query.Where.(*BinaryExpression); ok && be.Operator == "=" {
			if ident, ok := be.Left.(*Identifier); ok {
				if lit, ok := be.Right.(*Literal); ok {
					indexApplied = true
					keyName := ident.Value
					filterValue := fmt.Sprintf("%v", lit.Value)
					// Build an index for the keyName.
					index := make(map[string][]utils.Record)
					for _, row := range rows {
						if val, exists := row[keyName]; exists {
							keyStr := fmt.Sprintf("%v", val)
							index[keyStr] = append(index[keyStr], row)
						}
					}
					// O(1) lookup in the index.
					if recs, found := index[filterValue]; found {
						filteredRows = recs
					} else {
						// No matching records found.
						filteredRows = []utils.Record{}
					}
				}
			}
		}
	}

	// If no index was applied (or WHERE is not simple), fall back to iterative filtering.
	if !indexApplied {
		for _, row := range rows {
			if query.Where != nil {
				result := ctx.evalExpression(query.Where, row)
				if b, ok := result.(bool); !ok || !b {
					continue
				}
			}
			filteredRows = append(filteredRows, row)
		}
	}

	// Continue with processing aggregates, GROUP BY, HAVING, etc.
	ctx.CurrentResultSet = filteredRows

	hasAggregate := false
	for _, expr := range query.Select.Fields {
		_, underlying := unwrapAlias(expr)
		if fc, ok := underlying.(*FunctionCall); ok {
			switch strings.ToUpper(fc.FunctionName) {
			case "COUNT", "AVG", "SUM", "MIN", "MAX", "DIFF":
				hasAggregate = true
			}
		}
	}

	var resultRows []utils.Record
	if query.GroupBy != nil {
		groups := make(map[string][]utils.Record)
		for _, row := range filteredRows {
			keyVal := ctx.evalExpression(query.GroupBy.Fields[0], row)
			key := fmt.Sprintf("%v", keyVal)
			groups[key] = append(groups[key], row)
		}
		for _, groupRows := range groups {
			resultRow := make(utils.Record)
			for i, expr := range query.Select.Fields {
				colName := getFieldName(expr, i)
				_, underlying := unwrapAlias(expr)
				if fc, ok := underlying.(*FunctionCall); ok {
					switch strings.ToUpper(fc.FunctionName) {
					case "COUNT":
						resultRow[colName] = len(groupRows)
					case "AVG":
						sum := 0.0
						count := 0.0
						for _, r := range groupRows {
							val := ctx.evalExpression(fc.Args[0], r)
							num, err := convert.ToFloat64(val)
							if err {
								sum += num
								count++
							}
						}
						if count > 0 {
							resultRow[colName] = sum / count
						} else {
							resultRow[colName] = nil
						}
					case "SUM":
						sum := 0.0
						for _, r := range groupRows {
							val := ctx.evalExpression(fc.Args[0], r)
							num, err := convert.ToFloat64(val)
							if err {
								sum += num
							}
						}
						resultRow[colName] = sum
					case "MIN":
						var minVal float64
						first := true
						for _, r := range groupRows {
							val := ctx.evalExpression(fc.Args[0], r)
							num, err := convert.ToFloat64(val)
							if err {
								if first {
									minVal = num
									first = false
								} else if num < minVal {
									minVal = num
								}
							}
						}
						if first {
							resultRow[colName] = nil
						} else {
							resultRow[colName] = minVal
						}
					case "MAX":
						var maxVal float64
						first := true
						for _, r := range groupRows {
							val := ctx.evalExpression(fc.Args[0], r)
							num, err := convert.ToFloat64(val)
							if err {
								if first {
									maxVal = num
									first = false
								} else if num > maxVal {
									maxVal = num
								}
							}
						}
						if first {
							resultRow[colName] = nil
						} else {
							resultRow[colName] = maxVal
						}
					case "DIFF":
						var minVal, maxVal float64
						first := true
						for _, r := range groupRows {
							val := ctx.evalExpression(fc.Args[0], r)
							num, err := convert.ToFloat64(val)
							if err {
								if first {
									minVal = num
									maxVal = num
									first = false
								} else {
									if num < minVal {
										minVal = num
									}
									if num > maxVal {
										maxVal = num
									}
								}
							}
						}
						if first {
							resultRow[colName] = nil
						} else {
							resultRow[colName] = maxVal - minVal
						}
					default:
						resultRow[colName] = nil
					}
				} else {
					resultRow[colName] = ctx.evalExpression(underlying, groupRows[0])
				}
			}
			if query.Having != nil {
				hv := ctx.evalExpression(query.Having, resultRow)
				if b, ok := hv.(bool); !ok || !b {
					continue
				}
			}
			resultRows = append(resultRows, resultRow)
		}
	} else if hasAggregate {
		resultRow := make(utils.Record)
		for i, expr := range query.Select.Fields {
			colName := getFieldName(expr, i)
			_, underlying := unwrapAlias(expr)
			if fc, ok := underlying.(*FunctionCall); ok {
				switch strings.ToUpper(fc.FunctionName) {
				case "COUNT":
					resultRow[colName] = len(filteredRows)
				case "AVG":
					sum := 0.0
					count := 0.0
					for _, r := range filteredRows {
						val := ctx.evalExpression(fc.Args[0], r)
						num, err := convert.ToFloat64(val)
						if err {
							sum += num
							count++
						}
					}
					if count > 0 {
						resultRow[colName] = sum / count
					} else {
						resultRow[colName] = nil
					}
				case "SUM":
					sum := 0.0
					for _, r := range filteredRows {
						val := ctx.evalExpression(fc.Args[0], r)
						num, err := convert.ToFloat64(val)
						if err {
							sum += num
						}
					}
					resultRow[colName] = sum
				case "MIN":
					var minVal float64
					first := true
					for _, r := range filteredRows {
						val := ctx.evalExpression(fc.Args[0], r)
						num, err := convert.ToFloat64(val)
						if err {
							if first {
								minVal = num
								first = false
							} else if num < minVal {
								minVal = num
							}
						}
					}
					if first {
						resultRow[colName] = nil
					} else {
						resultRow[colName] = minVal
					}
				case "MAX":
					var maxVal float64
					first := true
					for _, r := range filteredRows {
						val := ctx.evalExpression(fc.Args[0], r)
						num, err := convert.ToFloat64(val)
						if err {
							if first {
								maxVal = num
								first = false
							} else if num > maxVal {
								maxVal = num
							}
						}
					}
					if first {
						resultRow[colName] = nil
					} else {
						resultRow[colName] = maxVal
					}
				case "DIFF":
					var minVal, maxVal float64
					first := true
					for _, r := range filteredRows {
						val := ctx.evalExpression(fc.Args[0], r)
						num, err := convert.ToFloat64(val)
						if err {
							if first {
								minVal = num
								maxVal = num
								first = false
							} else {
								if num < minVal {
									minVal = num
								}
								if num > maxVal {
									maxVal = num
								}
							}
						}
					}
					if first {
						resultRow[colName] = nil
					} else {
						resultRow[colName] = maxVal - minVal
					}
				default:
					resultRow[colName] = nil
				}
			} else {
				if len(filteredRows) > 0 {
					resultRow[colName] = ctx.evalExpression(underlying, filteredRows[0])
				} else {
					resultRow[colName] = nil
				}
			}
		}
		resultRows = append(resultRows, resultRow)
	} else {
		for _, row := range filteredRows {
			newRow := make(utils.Record)
			for i, expr := range query.Select.Fields {
				colName := getFieldName(expr, i)
				_, underlying := unwrapAlias(expr)
				if _, ok := underlying.(*Star); ok {
					for k, v := range row {
						newRow[k] = v
					}
				} else {
					newRow[colName] = ctx.evalExpression(underlying, row)
				}
			}
			resultRows = append(resultRows, newRow)
		}
	}
	if query.Distinct {
		var unique []utils.Record
		seen := make(map[string]bool)
		for _, row := range resultRows {
			key := fmt.Sprintf("%v", row)
			if !seen[key] {
				seen[key] = true
				unique = append(unique, row)
			}
		}
		resultRows = unique
	}
	if query.OrderBy != nil {
		sort.Slice(resultRows, func(i, j int) bool {
			for index, expr := range query.OrderBy.Fields {
				dir := "ASC"
				if index < len(query.OrderBy.Directions) {
					dir = query.OrderBy.Directions[index]
				}
				vi := ctx.evalExpression(expr, resultRows[i])
				vj := ctx.evalExpression(expr, resultRows[j])
				cmp := utils.CompareValues(vi, vj)
				if cmp == 0 {
					continue
				}
				if dir == "ASC" {
					return cmp < 0
				}
				return cmp > 0
			}
			return false
		})
	}
	if query.Limit != nil {
		start := query.Limit.Offset
		end := start + query.Limit.Limit
		if start > len(resultRows) {
			resultRows = []utils.Record{}
		} else {
			if end > len(resultRows) {
				end = len(resultRows)
			}
			resultRows = resultRows[start:end]
		}
	}
	return resultRows, nil
}

func (query *SQL) executeJoins(rows []utils.Record) ([]utils.Record, error) {
	currentRows := rows
	for _, join := range query.Joins {
		joinRows, err := join.Table.loadData()
		if err != nil {
			return nil, fmt.Errorf("failed to load join table %s: %s", join.Table.Name, err)
		}
		alias := join.Table.Alias
		if alias == "" {
			alias = join.Table.Name
		}
		joinType := strings.ToUpper(join.JoinType)
		var newResult []utils.Record
		if joinType == "CROSS" || joinType == "CROSS JOIN" {
			for _, leftRow := range currentRows {
				for _, rightRow := range joinRows {
					merged := utils.MergeRows(leftRow, rightRow, alias)
					newResult = append(newResult, merged)
				}
			}
		} else if joinType == "LEFT" || joinType == "LEFT JOIN" || joinType == "LEFT OUTER" || joinType == "LEFT OUTER JOIN" {
			for _, leftRow := range currentRows {
				matched := false
				for _, rightRow := range joinRows {
					merged := utils.MergeRows(leftRow, rightRow, alias)
					ctx := NewEvalContext()
					cond := ctx.evalExpression(join.On, merged)
					if b, ok := cond.(bool); ok && b {
						newResult = append(newResult, merged)
						matched = true
					}
				}
				if !matched {
					merged := utils.MergeRows(leftRow, nil, alias)
					newResult = append(newResult, merged)
				}
			}
		} else if joinType == "RIGHT" || joinType == "RIGHT JOIN" || joinType == "RIGHT OUTER" || joinType == "RIGHT OUTER JOIN" {
			for _, rightRow := range joinRows {
				matched := false
				for _, leftRow := range currentRows {
					merged := utils.MergeRows(leftRow, rightRow, alias)
					ctx := NewEvalContext()
					cond := ctx.evalExpression(join.On, merged)
					if b, ok := cond.(bool); ok && b {
						newResult = append(newResult, merged)
						matched = true
					}
				}
				if !matched {
					merged := utils.MergeRows(nil, rightRow, alias)
					newResult = append(newResult, merged)
				}
			}
		} else if joinType == "FULL" || joinType == "FULL JOIN" || joinType == "FULL OUTER" || joinType == "FULL OUTER JOIN" || joinType == "OUTER" || joinType == "OUTER JOIN" {
			leftMatched := make(map[string]bool)
			for _, leftRow := range currentRows {
				matched := false
				for _, rightRow := range joinRows {
					merged := utils.MergeRows(leftRow, rightRow, alias)
					ctx := NewEvalContext()
					cond := ctx.evalExpression(join.On, merged)
					if b, ok := cond.(bool); ok && b {
						newResult = append(newResult, merged)
						matched = true
						key := utils.RecordKey(leftRow, rightRow)
						leftMatched[key] = true
					}
				}
				if !matched {
					merged := utils.MergeRows(leftRow, nil, alias)
					newResult = append(newResult, merged)
				}
			}
			for _, rightRow := range joinRows {
				matched := false
				for _, leftRow := range currentRows {
					merged := utils.MergeRows(leftRow, rightRow, alias)
					ctx := NewEvalContext()
					cond := ctx.evalExpression(join.On, merged)
					if b, ok := cond.(bool); ok && b {
						key := utils.RecordKey(leftRow, rightRow)
						if leftMatched[key] {
							matched = true
							break
						}
					}
				}
				if !matched {
					merged := utils.MergeRows(nil, rightRow, alias)
					newResult = append(newResult, merged)
				}
			}
		} else {
			for _, leftRow := range currentRows {
				for _, rightRow := range joinRows {
					merged := utils.MergeRows(leftRow, rightRow, alias)
					ctx := NewEvalContext()
					cond := ctx.evalExpression(join.On, merged)
					if b, ok := cond.(bool); ok && b {
						newResult = append(newResult, merged)
					}
				}
			}
		}
		currentRows = newResult
	}
	return currentRows, nil
}
