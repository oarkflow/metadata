package sql

import (
	"errors"
	"fmt"
	"strings"

	"github.com/oarkflow/metadata/utils"
)

func Query(query string) ([]utils.Record, error) {
	lexer := NewLexer(query)
	parser := NewParser(lexer)
	program := parser.ParseQueryStatement()
	if len(parser.errors) != 0 {
		return nil, errors.New(strings.Join(parser.errors, "\t"))
	}
	return program.parseAndExecute()
}

type QueryStatement struct {
	With     *WithClause
	Query    *SQL
	Compound *CompoundQuery
}

func (qs *QueryStatement) parseAndExecute() ([]utils.Record, error) {
	mainRows, err := qs.Query.From.loadData()
	if err != nil {
		return nil, err
	}

	if qs.Query.From.Alias != "" {
		alias := qs.Query.From.Alias
		for i, row := range mainRows {
			mainRows[i] = utils.ApplyAliasToRecord(row, alias)
		}
	}
	if len(qs.Query.Joins) > 0 {
		mainRows, err = qs.Query.executeJoins(mainRows)
		if err != nil {
			return nil, err
		}
	}
	result, err := qs.Query.executeQuery(mainRows)
	if err != nil {
		return nil, err
	}
	if qs.Compound != nil {
		leftRes, _ := qs.Compound.Left.executeQuery(mainRows)
		rightRes, _ := qs.Compound.Right.executeQuery(mainRows)
		switch qs.Compound.Operator {
		case UNION:
			result = utils.Union(leftRes, rightRes)
		case INTERSECT:
			result = utils.Intersect(leftRes, rightRes)
		case EXCEPT:
			result = utils.Except(leftRes, rightRes)
		}
	}
	return result, nil
}

func (qs *QueryStatement) statementNode()       {}
func (qs *QueryStatement) TokenLiteral() string { return qs.Query.TokenLiteral() }
func (qs *QueryStatement) String() string {
	if qs.Compound != nil {
		return qs.Compound.String()
	}
	if qs.With != nil {
		var ctes []string
		for _, cte := range qs.With.CTEs {
			ctes = append(ctes, fmt.Sprintf("%s AS (%s)", cte.Name, cte.Query.String()))
		}
		return fmt.Sprintf("WITH %s %s", strings.Join(ctes, ", "), qs.Query.String())
	}
	return qs.Query.String()
}

type SQL struct {
	With     *WithClause
	Select   *SelectClause
	From     *TableReference
	Joins    []*JoinClause
	Where    Expression
	GroupBy  *GroupByClause
	Having   Expression
	OrderBy  *OrderByClause
	Limit    *LimitClause
	Distinct bool
}

func (query *SQL) TokenLiteral() string { return query.Select.TokenLiteral() }
func (query *SQL) String() string {
	var parts []string
	if query.With != nil {
		var ctes []string
		for _, cte := range query.With.CTEs {
			ctes = append(ctes, fmt.Sprintf("%s AS (%s)", cte.Name, cte.Query.String()))
		}
		parts = append(parts, "WITH "+strings.Join(ctes, ", "))
	}
	sel := "SELECT "
	if query.Distinct {
		sel += "DISTINCT "
	}
	sel += query.Select.String()
	parts = append(parts, sel)
	parts = append(parts, "FROM "+query.From.String())
	for _, join := range query.Joins {
		parts = append(parts, join.String())
	}
	if query.Where != nil {
		parts = append(parts, "WHERE "+query.Where.String())
	}
	if query.GroupBy != nil {
		parts = append(parts, "GROUP BY "+query.GroupBy.String())
	}
	if query.Having != nil {
		parts = append(parts, "HAVING "+query.Having.String())
	}
	if query.OrderBy != nil {
		parts = append(parts, query.OrderBy.String())
	}
	if query.Limit != nil {
		parts = append(parts, query.Limit.String())
	}
	return strings.Join(parts, " ")
}
