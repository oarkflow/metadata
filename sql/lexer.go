package sql

import (
	"strings"
	"unicode"
)

type TokenType string

func lookupKeyword(ident string) TokenType {
	switch strings.ToUpper(ident) {
	case "SELECT":
		return SELECT
	case "FROM":
		return FROM
	case "WHERE":
		return WHERE
	case "GROUP":
		return GROUP
	case "BY":
		return BY
	case "HAVING":
		return HAVING
	case "IN":
		return IN
	case "NOT":
		return NOT
	case "LIKE":
		return LIKE
	case "COUNT":
		return COUNT
	case "AVG":
		return AVG
	case "SUM":
		return SUM
	case "MIN":
		return MIN
	case "MAX":
		return MAX
	case "DIFF":
		return DIFF
	case "AS":
		return AS
	case "JOIN":
		return JOIN
	case "INNER":
		return INNER
	case "LEFT":
		return LEFT
	case "RIGHT":
		return RIGHT
	case "FULL":
		return FULL
	case "OUTER":
		return OUTER
	case "CROSS":
		return CROSS
	case "ON":
		return ON
	case "COALESCE":
		return COALESCE
	case "CONCAT":
		return CONCAT
	case "IF":
		return IF
	case "WHEN":
		return WHEN
	case "THEN":
		return THEN
	case "ELSE":
		return ELSE
	case "END":
		return END
	case "DISTINCT":
		return DISTINCT
	case "ORDER":
		return ORDER
	case "ASC":
		return ASC
	case "DESC":
		return DESC
	case "LIMIT":
		return LIMIT
	case "OFFSET":
		return OFFSET
	case "UNION":
		return UNION
	case "INTERSECT":
		return INTERSECT
	case "EXCEPT":
		return EXCEPT
	case "WITH":
		return WITH
	case "OVER":
		return OVER
	case "PARTITION":
		return PARTITION
	case "BETWEEN":
		return BETWEEN
	case "AND":
		return AND
	case "IS":
		return IS
	case "NULL":
		return NULL
	case "EXISTS":
		return EXISTS
	case "ANY":
		return ANY
	case "ALL":
		return ALL
	default:
		return IDENT
	}
}

type Token struct {
	Type    TokenType
	Literal string
}

type Lexer struct {
	input        string
	position     int
	readPosition int
	ch           byte
}

func NewLexer(input string) *Lexer {
	l := &Lexer{input: input}
	l.readChar()
	return l
}

func (l *Lexer) readChar() {
	if l.readPosition >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.readPosition]
	}
	l.position = l.readPosition
	l.readPosition++
}

func (l *Lexer) peekChar() byte {
	if l.readPosition >= len(l.input) {
		return 0
	}
	return l.input[l.readPosition]
}

func isIdentifierChar(ch byte) bool {
	return unicode.IsLetter(rune(ch)) || unicode.IsDigit(rune(ch)) || ch == '_' || ch == '.'
}

func (l *Lexer) NextToken() Token {
	var tok Token
	l.skipWhitespace()
	switch l.ch {
	case '=':
		tok = newToken(ASSIGN, l.ch)
	case '!':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = Token{Type: NOT_EQ, Literal: string(ch) + string(l.ch)}
		} else {
			tok = newToken(ILLEGAL, l.ch)
		}
	case '<':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = Token{Type: LTE, Literal: string(ch) + string(l.ch)}
		} else {
			tok = newToken(LT, l.ch)
		}
	case '>':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = Token{Type: GTE, Literal: string(ch) + string(l.ch)}
		} else {
			tok = newToken(GT, l.ch)
		}
	case '+':
		tok = newToken(PLUS, l.ch)
	case '-':
		tok = newToken(MINUS, l.ch)
	case '*':
		tok = newToken(ASTERISK, l.ch)
	case '/':
		tok = newToken(SLASH, l.ch)
	case ',':
		tok = newToken(COMMA, l.ch)
	case ';':
		tok = newToken(SEMICOLON, l.ch)
	case '(':
		tok = newToken(LPAREN, l.ch)
	case ')':
		tok = newToken(RPAREN, l.ch)
	case '\'':
		tok.Type = STRING
		tok.Literal = l.readString()
	case 0:
		tok.Literal = ""
		tok.Type = EOF
	default:
		if unicode.IsLetter(rune(l.ch)) || l.ch == '_' {
			literal := l.readIdentifier()
			tok.Type = lookupKeyword(literal)
			tok.Literal = literal
			return tok
		} else if isDigit(l.ch) {
			tok.Type = INT
			tok.Literal = l.readNumber()
			return tok
		} else {
			tok = newToken(ILLEGAL, l.ch)
		}
	}
	l.readChar()
	return tok
}

func newToken(tokenType TokenType, ch byte) Token {
	return Token{Type: tokenType, Literal: string(ch)}
}

func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

func (l *Lexer) readIdentifier() string {
	start := l.position
	for isIdentifierChar(l.ch) {
		l.readChar()
	}
	return l.input[start:l.position]
}

func (l *Lexer) readNumber() string {
	start := l.position
	for isDigit(l.ch) {
		l.readChar()
	}
	return l.input[start:l.position]
}

func (l *Lexer) readString() string {
	l.readChar()
	start := l.position
	for l.ch != '\'' && l.ch != 0 {
		l.readChar()
	}
	return l.input[start:l.position]
}
