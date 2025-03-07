package sql

import (
	"fmt"
	"sort"
	"strings"

	"github.com/oarkflow/convert"

	"github.com/oarkflow/metadata/utils"
)

func loadDataForSubquery() []utils.Record {
	return []utils.Record{}
}

type EvalContext struct {
	OuterRow         utils.Record
	CurrentResultSet []utils.Record
}

func NewEvalContext() *EvalContext {
	return &EvalContext{OuterRow: make(map[string]any)}
}

func (ctx *EvalContext) evalExpression(expr Expression, row utils.Record) any {
	switch e := expr.(type) {
	case *Identifier:
		if v, ok := row[e.Value]; ok {
			return v
		}
		if ctx.OuterRow != nil {
			if v, ok := ctx.OuterRow[e.Value]; ok {
				return v
			}
		}
		return nil
	case *Literal:
		return e.Value
	case *AliasExpression:
		return ctx.evalExpression(e.Expr, row)
	case *BinaryExpression:
		return ctx.evalBinaryExpression(e, row)
	case *InExpression:
		leftVal := ctx.evalExpression(e.Left, row)
		found := false
		for _, ex := range e.List {
			if utils.CompareValues(ctx.evalExpression(ex, row), leftVal) == 0 {
				found = true
				break
			}
		}
		if e.Not {
			return !found
		}
		return found
	case *LikeExpression:
		leftVal := ctx.evalExpression(e.Left, row)
		pattern := ctx.evalExpression(e.Pattern, row)
		s, ok1 := leftVal.(string)
		pat, ok2 := pattern.(string)
		if ok1 && ok2 {
			match := strings.Contains(s, pat)
			if e.Not {
				return !match
			}
			return match
		}
		return false
	case *FunctionCall:
		name := strings.ToUpper(e.FunctionName)
		switch name {
		case "COALESCE", "CONCAT", "IF":
			return ctx.evalScalarFunction(e, row)
		case "SUBSTR":
			if len(e.Args) < 2 {
				return nil
			}
			s := fmt.Sprintf("%v", ctx.evalExpression(e.Args[0], row))
			startVal, err := convert.ToFloat64(ctx.evalExpression(e.Args[1], row))
			if !err {
				return nil
			}
			start := int(startVal) - 1
			if len(e.Args) == 3 {
				lenVal, err := convert.ToFloat64(ctx.evalExpression(e.Args[2], row))
				if !err {
					return nil
				}
				length := int(lenVal)
				if start < 0 || start >= len(s) {
					return ""
				}
				end := start + length
				if end > len(s) {
					end = len(s)
				}
				return s[start:end]
			}
			if start < 0 || start >= len(s) {
				return ""
			}
			return s[start:]
		case "LENGTH":
			s := fmt.Sprintf("%v", ctx.evalExpression(e.Args[0], row))
			return len(s)
		case "UPPER":
			s := fmt.Sprintf("%v", ctx.evalExpression(e.Args[0], row))
			return strings.ToUpper(s)
		case "LOWER":
			s := fmt.Sprintf("%v", ctx.evalExpression(e.Args[0], row))
			return strings.ToLower(s)
		default:
			return nil
		}
	case *WindowFunction:
		fnName := strings.ToUpper(e.Func.TokenLiteral())
		switch fnName {
		case "COUNT":
			key := ctx.getPartitionKey(row, e.PartitionBy)
			count := 0
			for _, r := range ctx.CurrentResultSet {
				if ctx.getPartitionKey(r, e.PartitionBy) == key {
					count++
				}
			}
			return count
		case "ROW_NUMBER":
			key := ctx.getPartitionKey(row, e.PartitionBy)
			var partitionRows []utils.Record
			for _, r := range ctx.CurrentResultSet {
				if ctx.getPartitionKey(r, e.PartitionBy) == key {
					partitionRows = append(partitionRows, r)
				}
			}
			if e.OrderBy != nil && len(e.OrderBy.Fields) > 0 {
				sort.SliceStable(partitionRows, func(i, j int) bool {
					vi := ctx.evalExpression(e.OrderBy.Fields[0], partitionRows[i])
					vj := ctx.evalExpression(e.OrderBy.Fields[0], partitionRows[j])
					return utils.CompareValues(vi, vj) < 0
				})
			}
			for i, r := range partitionRows {
				if utils.DeepEqual(r, row) {
					return i + 1
				}
			}
			return nil
		default:
			return ctx.evalExpression(e.Func, row)
		}
	case *CaseExpression:
		for _, wc := range e.WhenClauses {
			cond := ctx.evalExpression(wc.Condition, row)
			if b, ok := cond.(bool); ok && b {
				return ctx.evalExpression(wc.Result, row)
			}
		}
		if e.Else != nil {
			return ctx.evalExpression(e.Else, row)
		}
		return nil
	case *Star:
		return nil
	case *Subquery:
		oldOuter := ctx.OuterRow
		ctx.OuterRow = row
		subRows, err := e.Query.executeQuery(loadDataForSubquery())
		ctx.OuterRow = oldOuter
		if err != nil || len(subRows) == 0 {
			return nil
		}
		for _, v := range subRows[0] {
			return v
		}
		return nil
	default:
		return nil
	}
}

func (ctx *EvalContext) evalBinaryExpression(e *BinaryExpression, row utils.Record) any {
	left := ctx.evalExpression(e.Left, row)
	right := ctx.evalExpression(e.Right, row)
	if e.Operator == PLUS || e.Operator == MINUS || e.Operator == ASTERISK || e.Operator == SLASH {
		lnum, errA := convert.ToFloat64(left)
		rnum, errB := convert.ToFloat64(right)
		if errA && errB {
			switch e.Operator {
			case PLUS:
				return lnum + rnum
			case MINUS:
				return lnum - rnum
			case ASTERISK:
				return lnum * rnum
			case SLASH:
				if rnum == 0 {
					return nil
				}
				return lnum / rnum
			}
		}
	}
	switch e.Operator {
	case "=":
		return utils.CompareValues(left, right) == 0
	case "!=":
		return utils.CompareValues(left, right) != 0
	case "<":
		return utils.CompareValues(left, right) < 0
	case ">":
		return utils.CompareValues(left, right) > 0
	case "<=":
		return utils.CompareValues(left, right) <= 0
	case ">=":
		return utils.CompareValues(left, right) >= 0
	case "IS NULL":
		return left == nil
	case "IS NOT NULL":
		return left != nil
	case "AND":
		leftVal, _ := convert.ToBool(left)
		rightVal, _ := convert.ToBool(right)
		return leftVal && rightVal
	}
	return nil
}

func (ctx *EvalContext) evalScalarFunction(fc *FunctionCall, row utils.Record) any {
	name := strings.ToUpper(fc.FunctionName)
	switch name {
	case "COALESCE":
		for _, arg := range fc.Args {
			val := ctx.evalExpression(arg, row)
			if val != nil && val != "" {
				return val
			}
		}
		return nil
	case "CONCAT":
		var parts []string
		for _, arg := range fc.Args {
			val := ctx.evalExpression(arg, row)
			parts = append(parts, fmt.Sprintf("%v", val))
		}
		return strings.Join(parts, "")
	case "IF":
		if len(fc.Args) < 3 {
			return nil
		}
		cond := ctx.evalExpression(fc.Args[0], row)
		if b, ok := cond.(bool); ok && b {
			return ctx.evalExpression(fc.Args[1], row)
		} else {
			return ctx.evalExpression(fc.Args[2], row)
		}
	default:
		return nil
	}
}

func (ctx *EvalContext) getPartitionKey(row utils.Record, exprs []Expression) string {
	if len(exprs) == 0 {
		return "all"
	}
	var parts []string
	for _, expr := range exprs {
		val := ctx.evalExpression(expr, row)
		parts = append(parts, fmt.Sprintf("%v", val))
	}
	return strings.Join(parts, "|")
}
