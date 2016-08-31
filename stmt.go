package nap

import (
	"database/sql"
    "time"
)

// Stmt is an aggregate prepared statement.
// It holds a prepared statement for each underlying physical db.
type Stmt interface {
	Close() error
	Exec(...interface{}) (sql.Result, error)
	Query(...interface{}) (*sql.Rows, error)
	QueryRow(...interface{}) *sql.Row
}



type stmt struct {
	db    *DB
	stmts []*sql.Stmt
    timeout time.Duration
}

// Close closes the statement by concurrently closing all underlying
// statements concurrently, returning the first non nil error.
func (s *stmt) Close() error {
	return scatter(len(s.stmts), func(i int) error {
		return s.stmts[i].Close()
	})
}

// Exec executes a prepared statement with the given arguments
// and returns a Result summarizing the effect of the statement.
// Exec uses the master as the underlying physical db.
func (s *stmt) Exec(args ...interface{}) (sql.Result, error) {
	return s.stmts[0].Exec(args...)
}

// Query executes a prepared query statement with the given
// arguments and returns the query results as a *sql.Rows.
// Query uses a slave as the underlying physical db.
func (s *stmt) Query(args ...interface{}) (*sql.Rows, error) {
    if len(args) == 0 {
        return s.stmts[s.db.slave(len(s.db.pdbs))].Query(args...)
    }
    m, ok :=  args[len(args) - 1].(OnlyMaster)
    if ok && m == true {
        args = args[0:len(args)-1]
        return s.stmts[0].Query(args...)   
    } else {
	    return s.stmts[s.db.slave(len(s.db.pdbs))].Query(args...)
    }
}

// QueryRow executes a prepared query statement with the given arguments.
// If an error occurs during the execution of the statement, that error
// will be returned by a call to Scan on the returned *Row, which is always non-nil.
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *sql.Row's Scan scans the first selected row and discards the rest.
// QueryRow uses a slave as the underlying physical db.
func (s *stmt) QueryRow(args ...interface{}) *sql.Row {
    if len(args) == 0 {
        return s.stmts[s.db.slave(len(s.db.pdbs))].QueryRow(args...)
    }
    m, ok :=  args[len(args) - 1].(OnlyMaster)
    if ok && m == true {
        args = args[0:len(args)-1]
        return s.stmts[0].QueryRow(args...)
    } else {
        return s.stmts[s.db.slave(len(s.db.pdbs))].QueryRow(args...)
    }
}
