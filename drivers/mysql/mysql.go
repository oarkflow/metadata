// Package mysql defines and registers usql's MySQL driver.
//
// Alias: memsql, SingleStore MemSQL
// Alias: vitess, Vitess Database
// Alias: tidb, TiDB
//
// See: https://github.com/go-sql-driver/mysql
// Group: base
package mysql

import (
	"io"
	"strconv"

	"github.com/go-sql-driver/mysql" // DRIVER

	"github.com/oarkflow/metadata"
	"github.com/oarkflow/metadata/drivers"
	mymeta "github.com/oarkflow/metadata/mysql"
)

func init() {
	drivers.Register("mysql", drivers.Driver{
		AllowMultilineComments: true,
		AllowHashComments:      true,
		LexerName:              "mysql",
		UseColumnTypes:         true,
		ForceParams: drivers.ForceQueryParameters([]string{
			"parseTime", "true",
			"loc", "Local",
			"sql_mode", "ansi",
		}),
		Err: func(err error) (string, string) {
			if e, ok := err.(*mysql.MySQLError); ok {
				return strconv.Itoa(int(e.Number)), e.Message
			}
			return "", err.Error()
		},
		IsPasswordErr: func(err error) bool {
			if e, ok := err.(*mysql.MySQLError); ok {
				return e.Number == 1045
			}
			return false
		},
		NewMetadataReader: mymeta.NewReader,
		NewMetadataWriter: func(db drivers.DB, w io.Writer, opts ...metadata.ReaderOption) metadata.Writer {
			return metadata.NewDefaultWriter(mymeta.NewReader(db, opts...))(db, w)
		},
		Copy:         drivers.CopyWithInsert(func(int) string { return "?" }),
		NewCompleter: mymeta.NewCompleter,
	}, "memsql", "vitess", "tidb")
}
