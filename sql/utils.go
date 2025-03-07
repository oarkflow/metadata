package sql

var (
	reserved    = []string{"JOIN", "FROM", "WHERE", "GROUP", "HAVING", "ON", "CASE", "WHEN", "THEN", "ELSE", "END", "ORDER", "LIMIT", "OFFSET", "UNION", "INTERSECT", "EXCEPT", "WITH"}
	precedences = map[TokenType]int{
		PLUS:     10,
		MINUS:    10,
		ASTERISK: 20,
		SLASH:    20,
		ASSIGN:   5,
		NOT_EQ:   5,
		LT:       5,
		GT:       5,
		LTE:      5,
		GTE:      5,
		BETWEEN:  5,
		LPAREN:   30,
	}
)

const (
	ILLEGAL = "ILLEGAL"
	EOF     = "EOF"
	IDENT   = "IDENT"
	INT     = "INT"
	STRING  = "STRING"

	ASSIGN   = "="
	NOT_EQ   = "!="
	LT       = "<"
	GT       = ">"
	LTE      = "<="
	GTE      = ">="
	PLUS     = "+"
	MINUS    = "-"
	ASTERISK = "*"
	SLASH    = "/"

	COMMA     = ","
	SEMICOLON = ";"
	LPAREN    = "("
	RPAREN    = ")"

	SELECT   = "SELECT"
	FROM     = "FROM"
	WHERE    = "WHERE"
	GROUP    = "GROUP"
	BY       = "BY"
	HAVING   = "HAVING"
	IN       = "IN"
	NOT      = "NOT"
	LIKE     = "LIKE"
	COUNT    = "COUNT"
	AVG      = "AVG"
	SUM      = "SUM"
	MIN      = "MIN"
	MAX      = "MAX"
	DIFF     = "DIFF"
	AS       = "AS"
	JOIN     = "JOIN"
	INNER    = "INNER"
	LEFT     = "LEFT"
	RIGHT    = "RIGHT"
	FULL     = "FULL"
	OUTER    = "OUTER"
	CROSS    = "CROSS"
	ON       = "ON"
	COALESCE = "COALESCE"
	CONCAT   = "CONCAT"
	IF       = "IF"
	WHEN     = "WHEN"
	THEN     = "THEN"
	ELSE     = "ELSE"
	END      = "END"

	DISTINCT  = "DISTINCT"
	ORDER     = "ORDER"
	ASC       = "ASC"
	DESC      = "DESC"
	LIMIT     = "LIMIT"
	OFFSET    = "OFFSET"
	UNION     = "UNION"
	INTERSECT = "INTERSECT"
	EXCEPT    = "EXCEPT"

	WITH      = "WITH"
	OVER      = "OVER"
	PARTITION = "PARTITION"
	BETWEEN   = "BETWEEN"
	AND       = "AND"
	IS        = "IS"
	NULL      = "NULL"
	EXISTS    = "EXISTS"
	ANY       = "ANY"
	ALL       = "ALL"

	ROW_NUMBER = "ROW_NUMBER"
	RANK       = "RANK"
	DENSE_RANK = "DENSE_RANK"
)
