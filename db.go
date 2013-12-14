package pan

import (
	"database/sql"
	"database/sql/driver"
	"strings"
	"sync/atomic"
)

// DB is a logical database with multiple underlying physical databases
// forming a single master multiple slaves topology.
// Reads and writes are automatically directed to the correct physical db.
type DB struct {
	pdbs  []*sql.DB // Physical databases
	count uint64    // Monotonically incrementing counter on each query
}

// Open constructs a DB struct by splitting dataSourceNames into multiple
// connection strings (separated by ';').
// For each connection string it creates a *sql.DB or returns an error if it fails.
// The first connection string is assumed to be the master
// while the rest are used for the slaves.
func Open(driverName, dataSourceNames string) (*DB, error) {
	conns := strings.Split(dataSourceNames, ";")
	db := &DB{pdbs: make([]*sql.DB, len(conns))}

	var err error
	for i, conn := range conns {
		if db.pdbs[i], err = sql.Open(driverName, conn); err != nil {
			return nil, err
		}
	}

	return db, nil
}

// Close closes all physical databases concurrently, releasing any open resources.
func (db *DB) Close() error {
	errors := make(chan error, len(db.pdbs))

	for i := range db.pdbs {
		go func(i int) { errors <- db.pdbs[i].Close() }(i)
	}

	for i := 0; i < cap(errors); i++ {
		if err := <-errors; err != nil {
			return err
		}
	}

	return nil
}

// Driver returns the physical database's underlying driver.
func (db *DB) Driver() driver.Driver {
	return db.pdbs[0].Driver()
}

// Begin starts a transaction on the master. The isolation level is dependent on the driver.
func (db *DB) Begin() (*sql.Tx, error) {
	return db.pdbs[0].Begin()
}

// Exec executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
// Exec uses the master as the underlying physical db.
func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.pdbs[0].Exec(query, args...)
}

// Ping verifies if a connection to each physical database is still alive,
// establishing a connection if necessary.
func (db *DB) Ping() error {
	errors := make(chan error, len(db.pdbs))

	for i := range db.pdbs {
		go func(i int) { errors <- db.pdbs[i].Ping() }(i)
	}

	for i := 0; i < cap(errors); i++ {
		if err := <-errors; err != nil {
			return err
		}
	}

	return nil
}

// Prepare creates a prepared statement for later queries or executions
// on each physical database, concurrently.
func (db *DB) Prepare(query string) (*Stmt, error) {
	stmts := make([]*sql.Stmt, len(db.pdbs))
	errors := make(chan error, len(db.pdbs))

	for i := range db.pdbs {
		go func(i int) {
			var err error
			stmts[i], err = db.pdbs[i].Prepare(query)
			errors <- err
		}(i)
	}

	for i := 0; i < cap(errors); i++ {
		if err := <-errors; err != nil {
			return nil, err
		}
	}

	return &Stmt{DB: db, stmts: stmts}, nil
}

// Query executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
// Query uses a slave as the physical db.
func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return db.pdbs[db.slave(len(db.pdbs))].Query(query, args...)
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always return a non-nil value.
// Errors are deferred until Row's Scan method is called.
// QueryRow uses a slave as the physical db.
func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	return db.pdbs[db.slave(len(db.pdbs))].QueryRow(query, args...)
}

// SetMaxIdleConns sets the maximum number of connections in the idle
// connection pool for each underlying physical db.
// If MaxOpenConns is greater than 0 but less than the new MaxIdleConns then the
// new MaxIdleConns will be reduced to match the MaxOpenConns limit
// If n <= 0, no idle connections are retained.
func (db *DB) SetMaxIdleConns(n int) {
	for i := range db.pdbs {
		db.pdbs[i].SetMaxIdleConns(n)
	}
}

// SetMaxOpenConns sets the maximum number of open connections
// to each physical database.
// If MaxIdleConns is greater than 0 and the new MaxOpenConns
// is less than MaxIdleConns, then MaxIdleConns will be reduced to match
// the new MaxOpenConns limit. If n <= 0, then there is no limit on the number
// of open connections. The default is 0 (unlimited).
func (db *DB) SetMaxOpenConns(n int) {
  for i := range db.pdbs {
    db.pdbs[i].SetMaxOpenConns(n)
  }
}

// Slave returns one of the physical databases which is a slave
func (db *DB) Slave() *sql.DB {
	return db.pdbs[db.slave(len(db.pdbs))]
}

// Master returns the master physical database
func (db *DB) Master() *sql.DB {
	return db.pdbs[0]
}

func (db *DB) slave(n int) int {
	if n <= 1 {
		return 0
	}
	return int(1 + (atomic.AddUint64(&db.count, 1) % uint64(n-1)))
}
