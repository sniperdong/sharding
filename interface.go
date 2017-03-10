package sharding

import (
	"database/sql"
)

type Eorm interface {
	Read(md interface{}, cols ...string) error
	Insert(md interface{}) (int64, error)
	Update(md interface{}, cols ...string) (int64, error)
	Delete(md interface{}) (int64, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
    Query2Obj(res interface{},query string, args ...interface{}) error
	Using(name string) error
	Begin() error
	Commit() error
	Rollback() error
}

//db interface
type dbQuerier interface {
	Begin() (*sql.Tx, error)
}

//row interface
type rowQuerier interface {
	Scan(dest ...interface{}) error
}

//rows interface
type rowsQuerier interface {
	Close() error
	Columns() ([]string, error)
	Err() error
	Next() bool
	Scan(dest ...interface{}) error
}

// stmt interface
type stmtQuerier interface {
	Close() error
	Exec(args ...interface{}) (sql.Result, error)
	Query(args ...interface{}) (*sql.Rows, error)
	QueryRow(args ...interface{}) *sql.Row

}

// transaction interface
type tranQuerier interface {
	Prepare(query string) (*sql.Stmt, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	Stmt(stmt *sql.Stmt) *sql.Stmt
	Commit() error
	Rollback() error
}
