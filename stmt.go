package pan

import (
	"database/sql"
)

// Stmt is an aggregate prepared statement.
// It holds a prepared statement for each underlying physical db.
type Stmt struct {
	*DB
	stmts []*sql.Stmt
}

// Close closes the statement by concurrently closing all underlying
// statements concurrently, returning the first non nil error.
func (s *Stmt) Close() error {
	errors := make(chan error, len(s.stmts))

	for i := range s.stmts {
		go func(i int) { errors <- s.stmts[i].Close() }(i)
	}

	for i := 0; i < cap(errors); i++ {
		if err := <-errors; err != nil {
			return err
		}
	}

	return nil
}

// Exec executes a prepared statement with the given arguments
// and returns a Result summarizing the effect of the statement.
// Exec uses the master as the underlying physical db.
func (s *Stmt) Exec(args ...interface{}) (sql.Result, error) {
	return s.stmts[0].Exec(args...)
}

// Query executes a prepared query statement with the given
// arguments and returns the query results as a *sql.Rows.
// Query uses a slave as the underlying physical db.
func (s *Stmt) Query(args ...interface{}) (*sql.Rows, error) {
	return s.stmts[s.slave(len(s.pdbs))].Query(args...)
}

// QueryRow executes a prepared query statement with the given arguments.
// If an error occurs during the execution of the statement, that error
// will be returned by a call to Scan on the returned *Row, which is always non-nil.
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *sql.Row's Scan scans the first selected row and discards the rest.
// QueryRow uses a slave as the underlying physical db.
func (s *Stmt) QueryRow(args ...interface{}) *sql.Row {
	return s.stmts[s.slave(len(s.pdbs))].QueryRow(args...)
}
